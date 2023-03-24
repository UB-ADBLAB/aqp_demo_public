#include "aqp.h"

#include <catalog/pg_am.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_opfamily.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/primnodes.h>
#include <nodes/supportnodes.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/paths.h>
#include <utils/lsyscache.h>
#include <utils/selfuncs.h>

#include "pg_indxpath.h"

/*
 * Former static routines defined in optimizer/path/indxpath.c.
 */

/* XXX see PartCollMatchesExprColl */
#define IndexCollMatchesExprColl(idxcollation, exprcollation) \
	((idxcollation) == InvalidOid || (idxcollation) == (exprcollation))

/* Callback argument for ec_member_matches_indexcol */
typedef struct
{
	IndexOptInfo *index;		/* index we're considering */
	int			indexcol;		/* index column we want to match to */
} ec_member_matches_arg;

/****************************************************************************
 *                ----  ROUTINES TO CHECK QUERY CLAUSES  ----
 ****************************************************************************/

/*
 * aqp_pgport_match_restriction_clauses_to_index
 *      Identify restriction clauses for the rel that match the index.
 *      Matching clauses are added to *clauseset.
 */
void
aqp_pgport_match_restriction_clauses_to_index(PlannerInfo *root,
                                              IndexOptInfo *index,
                                              IndexClauseSet *clauseset)
{
    /* We can ignore clauses that are implied by the index predicate */
    aqp_pgport_match_clauses_to_index(root, index->indrestrictinfo, index,
                                      clauseset);
}

/*
 * aqp_pgport_match_join_clauses_to_index
 *      Identify join clauses for the rel that match the index.
 *      Matching clauses are added to *clauseset.
 *      Also, add any potentially usable join OR clauses to *joinorclauses.
 */
void
aqp_pgport_match_join_clauses_to_index(PlannerInfo *root,
                                       RelOptInfo *rel, IndexOptInfo *index,
                                       IndexClauseSet *clauseset,
                                       List **joinorclauses)
{
    ListCell   *lc;

    /* Scan the rel's join clauses */
    foreach(lc, rel->joininfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        /* Check if clause can be moved to this rel */
        if (!join_clause_is_movable_to(rinfo, rel))
            continue;

        /* Potentially usable, so see if it matches the index or is an OR */
        if (restriction_is_or_clause(rinfo))
            *joinorclauses = lappend(*joinorclauses, rinfo);
        else
            aqp_pgport_match_clause_to_index(root, rinfo, index, clauseset);
    }
}

/*
 * aqp_pgport_match_eclass_clauses_to_index
 *      Identify EquivalenceClass join clauses for the rel that match the index.
 *      Matching clauses are added to *clauseset.
 */
void
aqp_pgport_match_eclass_clauses_to_index(PlannerInfo *root, IndexOptInfo *index,
                              IndexClauseSet *clauseset)
{
    int            indexcol;

    /* No work if rel is not in any such ECs */
    if (!index->rel->has_eclass_joins)
        return;

    for (indexcol = 0; indexcol < index->nkeycolumns; indexcol++)
    {
        ec_member_matches_arg arg;
        List       *clauses;

        /* Generate clauses, skipping any that join to lateral_referencers */
        arg.index = index;
        arg.indexcol = indexcol;
        clauses = generate_implied_equalities_for_column(root,
                                                         index->rel,
                                                         aqp_pgport_ec_member_matches_indexcol,
                                                         (void *) &arg,
                                                         index->rel->lateral_referencers);

        /*
         * We have to check whether the results actually do match the index,
         * since for non-btree indexes the EC's equality operators might not
         * be in the index opclass (cf ec_member_matches_indexcol).
         */
        aqp_pgport_match_clauses_to_index(root, clauses, index, clauseset);
    }
}

/*
 * aqp_pgport match_clauses_to_index
 *      Perform match_clause_to_index() for each clause in a list.
 *      Matching clauses are added to *clauseset.
 */
void
aqp_pgport_match_clauses_to_index(PlannerInfo *root,
                       List *clauses,
                       IndexOptInfo *index,
                       IndexClauseSet *clauseset)
{
    ListCell   *lc;

    foreach(lc, clauses)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

        aqp_pgport_match_clause_to_index(root, rinfo, index, clauseset);
    }
}

/*
 * aqp_pgport_match_clause_to_index
 *      Test whether a qual clause can be used with an index.
 *
 * If the clause is usable, add an IndexClause entry for it to the appropriate
 * list in *clauseset.  (*clauseset must be initialized to zeroes before first
 * call.)
 *
 * Note: in some circumstances we may find the same RestrictInfos coming from
 * multiple places.  Defend against redundant outputs by refusing to add a
 * clause twice (pointer equality should be a good enough check for this).
 *
 * Note: it's possible that a badly-defined index could have multiple matching
 * columns.  We always select the first match if so; this avoids scenarios
 * wherein we get an inflated idea of the index's selectivity by using the
 * same clause multiple times with different index columns.
 */
void
aqp_pgport_match_clause_to_index(PlannerInfo *root,
                      RestrictInfo *rinfo,
                      IndexOptInfo *index,
                      IndexClauseSet *clauseset)
{
    int            indexcol;

    /*
     * Never match pseudoconstants to indexes.  (Normally a match could not
     * happen anyway, since a pseudoconstant clause couldn't contain a Var,
     * but what if someone builds an expression index on a constant? It's not
     * totally unreasonable to do so with a partial index, either.)
     */
    if (rinfo->pseudoconstant)
        return;

    /*
     * If clause can't be used as an indexqual because it must wait till after
     * some lower-security-level restriction clause, reject it.
     */
    if (!restriction_is_securely_promotable(rinfo, index->rel))
        return;

    /* OK, check each index key column for a match */
    for (indexcol = 0; indexcol < index->nkeycolumns; indexcol++)
    {
        IndexClause *iclause;
        ListCell   *lc;

        /* Ignore duplicates */
        foreach(lc, clauseset->indexclauses[indexcol])
        {
            IndexClause *iclause = (IndexClause *) lfirst(lc);

            if (iclause->rinfo == rinfo)
                return;
        }

        /* OK, try to match the clause to the index column */
        iclause = aqp_pgport_match_clause_to_indexcol(root,
                                                      rinfo,
                                                      indexcol,
                                                      index);
        if (iclause)
        {
            /* Success, so record it */
            clauseset->indexclauses[indexcol] =
                lappend(clauseset->indexclauses[indexcol], iclause);
            clauseset->nonempty = true;
            return;
        }
    }
}

/*
 * match_clause_to_indexcol()
 *      Determine whether a restriction clause matches a column of an index,
 *      and if so, build an IndexClause node describing the details.
 *
 *      To match an index normally, an operator clause:
 *
 *      (1)  must be in the form (indexkey op const) or (const op indexkey);
 *           and
 *      (2)  must contain an operator which is in the index's operator family
 *           for this column; and
 *      (3)  must match the collation of the index, if collation is relevant.
 *
 *      Our definition of "const" is exceedingly liberal: we allow anything that
 *      doesn't involve a volatile function or a Var of the index's relation.
 *      In particular, Vars belonging to other relations of the query are
 *      accepted here, since a clause of that form can be used in a
 *      parameterized indexscan.  It's the responsibility of higher code levels
 *      to manage restriction and join clauses appropriately.
 *
 *      Note: we do need to check for Vars of the index's relation on the
 *      "const" side of the clause, since clauses like (a.f1 OP (b.f2 OP a.f3))
 *      are not processable by a parameterized indexscan on a.f1, whereas
 *      something like (a.f1 OP (b.f2 OP c.f3)) is.
 *
 *      Presently, the executor can only deal with indexquals that have the
 *      indexkey on the left, so we can only use clauses that have the indexkey
 *      on the right if we can commute the clause to put the key on the left.
 *      We handle that by generating an IndexClause with the correctly-commuted
 *      opclause as a derived indexqual.
 *
 *      If the index has a collation, the clause must have the same collation.
 *      For collation-less indexes, we assume it doesn't matter; this is
 *      necessary for cases like "hstore ? text", wherein hstore's operators
 *      don't care about collation but the clause will get marked with a
 *      collation anyway because of the text argument.  (This logic is
 *      embodied in the macro IndexCollMatchesExprColl.)
 *
 *      It is also possible to match RowCompareExpr clauses to indexes (but
 *      currently, only btree indexes handle this).
 *
 *      It is also possible to match ScalarArrayOpExpr clauses to indexes, when
 *      the clause is of the form "indexkey op ANY (arrayconst)".
 *
 *      For boolean indexes, it is also possible to match the clause directly
 *      to the indexkey; or perhaps the clause is (NOT indexkey).
 *
 *      And, last but not least, some operators and functions can be processed
 *      to derive (typically lossy) indexquals from a clause that isn't in
 *      itself indexable.  If we see that any operand of an OpExpr or FuncExpr
 *      matches the index key, and the function has a planner support function
 *      attached to it, we'll invoke the support function to see if such an
 *      indexqual can be built.
 *
 * 'rinfo' is the clause to be tested (as a RestrictInfo node).
 * 'indexcol' is a column number of 'index' (counting from 0).
 * 'index' is the index of interest.
 *
 * Returns an IndexClause if the clause can be used with this index key,
 * or NULL if not.
 *
 * NOTE:  returns NULL if clause is an OR or AND clause; it is the
 * responsibility of higher-level routines to cope with those.
 */
IndexClause *
aqp_pgport_match_clause_to_indexcol(PlannerInfo *root,
                                    RestrictInfo *rinfo,
                                    int indexcol,
                                    IndexOptInfo *index)
{
    IndexClause *iclause;
    Expr       *clause = rinfo->clause;
    Oid            opfamily;

    Assert(indexcol < index->nkeycolumns);

    /*
     * Historically this code has coped with NULL clauses.  That's probably
     * not possible anymore, but we might as well continue to cope.
     */
    if (clause == NULL)
        return NULL;

    /* First check for boolean-index cases. */
    opfamily = index->opfamily[indexcol];
    if (IsBooleanOpfamily(opfamily))
    {
        iclause = aqp_pgport_match_boolean_index_clause(rinfo, indexcol, index);
        if (iclause)
            return iclause;
    }

    /*
     * Clause must be an opclause, funcclause, ScalarArrayOpExpr, or
     * RowCompareExpr.  Or, if the index supports it, we can handle IS
     * NULL/NOT NULL clauses.
     */
    if (IsA(clause, OpExpr))
    {
        return aqp_pgport_match_opclause_to_indexcol(root, rinfo, indexcol, index);
    }
    else if (IsA(clause, FuncExpr))
    {
        return aqp_pgport_match_funcclause_to_indexcol(root, rinfo, indexcol, index);
    }
    else if (IsA(clause, ScalarArrayOpExpr))
    {
        return aqp_pgport_match_saopclause_to_indexcol(rinfo, indexcol, index);
    }
    else if (IsA(clause, RowCompareExpr))
    {
        return aqp_pgport_match_rowcompare_to_indexcol(rinfo, indexcol, index);
    }
    else if (index->amsearchnulls && IsA(clause, NullTest))
    {
        NullTest   *nt = (NullTest *) clause;

        if (!nt->argisrow &&
            match_index_to_operand((Node *) nt->arg, indexcol, index))
        {
            iclause = makeNode(IndexClause);
            iclause->rinfo = rinfo;
            iclause->indexquals = list_make1(rinfo);
            iclause->lossy = false;
            iclause->indexcol = indexcol;
            iclause->indexcols = NIL;
            return iclause;
        }
    }

    return NULL;
}

/*
 * aqp_pgport_match_boolean_index_clause
 *      Recognize restriction clauses that can be matched to a boolean index.
 *
 * The idea here is that, for an index on a boolean column that supports the
 * BooleanEqualOperator, we can transform a plain reference to the indexkey
 * into "indexkey = true", or "NOT indexkey" into "indexkey = false", etc,
 * so as to make the expression indexable using the index's "=" operator.
 * Since Postgres 8.1, we must do this because constant simplification does
 * the reverse transformation; without this code there'd be no way to use
 * such an index at all.
 *
 * This should be called only when IsBooleanOpfamily() recognizes the
 * index's operator family.  We check to see if the clause matches the
 * index's key, and if so, build a suitable IndexClause.
 */
IndexClause *
aqp_pgport_match_boolean_index_clause(RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index)
{
    Node       *clause = (Node *) rinfo->clause;
    Expr       *op = NULL;

    /* Direct match? */
    if (match_index_to_operand(clause, indexcol, index))
    {
        /* convert to indexkey = TRUE */
        op = make_opclause(BooleanEqualOperator, BOOLOID, false,
                           (Expr *) clause,
                           (Expr *) makeBoolConst(true, false),
                           InvalidOid, InvalidOid);
    }
    /* NOT clause? */
    else if (is_notclause(clause))
    {
        Node       *arg = (Node *) get_notclausearg((Expr *) clause);

        if (match_index_to_operand(arg, indexcol, index))
        {
            /* convert to indexkey = FALSE */
            op = make_opclause(BooleanEqualOperator, BOOLOID, false,
                               (Expr *) arg,
                               (Expr *) makeBoolConst(false, false),
                               InvalidOid, InvalidOid);
        }
    }

    /*
     * Since we only consider clauses at top level of WHERE, we can convert
     * indexkey IS TRUE and indexkey IS FALSE to index searches as well.  The
     * different meaning for NULL isn't important.
     */
    else if (clause && IsA(clause, BooleanTest))
    {
        BooleanTest *btest = (BooleanTest *) clause;
        Node       *arg = (Node *) btest->arg;

        if (btest->booltesttype == IS_TRUE &&
            match_index_to_operand(arg, indexcol, index))
        {
            /* convert to indexkey = TRUE */
            op = make_opclause(BooleanEqualOperator, BOOLOID, false,
                               (Expr *) arg,
                               (Expr *) makeBoolConst(true, false),
                               InvalidOid, InvalidOid);
        }
        else if (btest->booltesttype == IS_FALSE &&
                 match_index_to_operand(arg, indexcol, index))
        {
            /* convert to indexkey = FALSE */
            op = make_opclause(BooleanEqualOperator, BOOLOID, false,
                               (Expr *) arg,
                               (Expr *) makeBoolConst(false, false),
                               InvalidOid, InvalidOid);
        }
    }

    /*
     * If we successfully made an operator clause from the given qual, we must
     * wrap it in an IndexClause.  It's not lossy.
     */
    if (op)
    {
        IndexClause *iclause = makeNode(IndexClause);

        iclause->rinfo = rinfo;
        iclause->indexquals = list_make1(make_simple_restrictinfo(op));
        iclause->lossy = false;
        iclause->indexcol = indexcol;
        iclause->indexcols = NIL;
        return iclause;
    }

    return NULL;
}

/*
 * aqp_pgport_match_opclause_to_indexcol()
 *      Handles the OpExpr case for match_clause_to_indexcol(),
 *      which see for comments.
 */
IndexClause *
aqp_pgport_match_opclause_to_indexcol(PlannerInfo *root,
                           RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index)
{
    IndexClause *iclause;
    OpExpr       *clause = (OpExpr *) rinfo->clause;
    Node       *leftop,
               *rightop;
    Oid            expr_op;
    Oid            expr_coll;
    Index        index_relid;
    Oid            opfamily;
    Oid            idxcollation;

    /*
     * Only binary operators need apply.  (In theory, a planner support
     * function could do something with a unary operator, but it seems
     * unlikely to be worth the cycles to check.)
     */
    if (list_length(clause->args) != 2)
        return NULL;

    leftop = (Node *) linitial(clause->args);
    rightop = (Node *) lsecond(clause->args);
    expr_op = clause->opno;
    expr_coll = clause->inputcollid;

    index_relid = index->rel->relid;
    opfamily = index->opfamily[indexcol];
    idxcollation = index->indexcollations[indexcol];

    /*
     * Check for clauses of the form: (indexkey operator constant) or
     * (constant operator indexkey).  See match_clause_to_indexcol's notes
     * about const-ness.
     *
     * Note that we don't ask the support function about clauses that don't
     * have one of these forms.  Again, in principle it might be possible to
     * do something, but it seems unlikely to be worth the cycles to check.
     */
    if (match_index_to_operand(leftop, indexcol, index) &&
        !bms_is_member(index_relid, rinfo->right_relids) &&
        !contain_volatile_functions(rightop))
    {
        if (IndexCollMatchesExprColl(idxcollation, expr_coll) &&
            op_in_opfamily(expr_op, opfamily))
        {
            iclause = makeNode(IndexClause);
            iclause->rinfo = rinfo;
            iclause->indexquals = list_make1(rinfo);
            iclause->lossy = false;
            iclause->indexcol = indexcol;
            iclause->indexcols = NIL;
            return iclause;
        }

        /*
         * If we didn't find a member of the index's opfamily, try the support
         * function for the operator's underlying function.
         */
        set_opfuncid(clause);    /* make sure we have opfuncid */
        return aqp_pgport_get_index_clause_from_support(root,
                                             rinfo,
                                             clause->opfuncid,
                                             0, /* indexarg on left */
                                             indexcol,
                                             index);
    }

    if (match_index_to_operand(rightop, indexcol, index) &&
        !bms_is_member(index_relid, rinfo->left_relids) &&
        !contain_volatile_functions(leftop))
    {
        if (IndexCollMatchesExprColl(idxcollation, expr_coll))
        {
            Oid            comm_op = get_commutator(expr_op);

            if (OidIsValid(comm_op) &&
                op_in_opfamily(comm_op, opfamily))
            {
                RestrictInfo *commrinfo;

                /* Build a commuted OpExpr and RestrictInfo */
                commrinfo = commute_restrictinfo(rinfo, comm_op);

                /* Make an IndexClause showing that as a derived qual */
                iclause = makeNode(IndexClause);
                iclause->rinfo = rinfo;
                iclause->indexquals = list_make1(commrinfo);
                iclause->lossy = false;
                iclause->indexcol = indexcol;
                iclause->indexcols = NIL;
                return iclause;
            }
        }

        /*
         * If we didn't find a member of the index's opfamily, try the support
         * function for the operator's underlying function.
         */
        set_opfuncid(clause);    /* make sure we have opfuncid */
        return aqp_pgport_get_index_clause_from_support(root,
                                             rinfo,
                                             clause->opfuncid,
                                             1, /* indexarg on right */
                                             indexcol,
                                             index);
    }

    return NULL;
}

/*
 * aqp_pgport_match_funcclause_to_indexcol()
 *      Handles the FuncExpr case for match_clause_to_indexcol(),
 *      which see for comments.
 */
IndexClause *
aqp_pgport_match_funcclause_to_indexcol(PlannerInfo *root,
                             RestrictInfo *rinfo,
                             int indexcol,
                             IndexOptInfo *index)
{
    FuncExpr   *clause = (FuncExpr *) rinfo->clause;
    int            indexarg;
    ListCell   *lc;

    /*
     * We have no built-in intelligence about function clauses, but if there's
     * a planner support function, it might be able to do something.  But, to
     * cut down on wasted planning cycles, only call the support function if
     * at least one argument matches the target index column.
     *
     * Note that we don't insist on the other arguments being pseudoconstants;
     * the support function has to check that.  This is to allow cases where
     * only some of the other arguments need to be included in the indexqual.
     */
    indexarg = 0;
    foreach(lc, clause->args)
    {
        Node       *op = (Node *) lfirst(lc);

        if (match_index_to_operand(op, indexcol, index))
        {
            return aqp_pgport_get_index_clause_from_support(root,
                                                 rinfo,
                                                 clause->funcid,
                                                 indexarg,
                                                 indexcol,
                                                 index);
        }

        indexarg++;
    }

    return NULL;
}

/*
 * aqp_pgport_get_index_clause_from_support()
 *        If the function has a planner support function, try to construct
 *        an IndexClause using indexquals created by the support function.
 */
IndexClause *
aqp_pgport_get_index_clause_from_support(PlannerInfo *root,
                              RestrictInfo *rinfo,
                              Oid funcid,
                              int indexarg,
                              int indexcol,
                              IndexOptInfo *index)
{
    Oid            prosupport = get_func_support(funcid);
    SupportRequestIndexCondition req;
    List       *sresult;

    if (!OidIsValid(prosupport))
        return NULL;

    req.type = T_SupportRequestIndexCondition;
    req.root = root;
    req.funcid = funcid;
    req.node = (Node *) rinfo->clause;
    req.indexarg = indexarg;
    req.index = index;
    req.indexcol = indexcol;
    req.opfamily = index->opfamily[indexcol];
    req.indexcollation = index->indexcollations[indexcol];

    req.lossy = true;            /* default assumption */

    sresult = (List *)
        DatumGetPointer(OidFunctionCall1(prosupport,
                                         PointerGetDatum(&req)));

    if (sresult != NIL)
    {
        IndexClause *iclause = makeNode(IndexClause);
        List       *indexquals = NIL;
        ListCell   *lc;

        /*
         * The support function API says it should just give back bare
         * clauses, so here we must wrap each one in a RestrictInfo.
         */
        foreach(lc, sresult)
        {
            Expr       *clause = (Expr *) lfirst(lc);

            indexquals = lappend(indexquals, make_simple_restrictinfo(clause));
        }

        iclause->rinfo = rinfo;
        iclause->indexquals = indexquals;
        iclause->lossy = req.lossy;
        iclause->indexcol = indexcol;
        iclause->indexcols = NIL;

        return iclause;
    }

    return NULL;
}

/*
 * match_saopclause_to_indexcol()
 *      Handles the ScalarArrayOpExpr case for match_clause_to_indexcol(),
 *      which see for comments.
 */
IndexClause *
aqp_pgport_match_saopclause_to_indexcol(RestrictInfo *rinfo,
                             int indexcol,
                             IndexOptInfo *index)
{
    ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) rinfo->clause;
    Node       *leftop,
               *rightop;
    Relids        right_relids;
    Oid            expr_op;
    Oid            expr_coll;
    Index        index_relid;
    Oid            opfamily;
    Oid            idxcollation;

    /* We only accept ANY clauses, not ALL */
    if (!saop->useOr)
        return NULL;
    leftop = (Node *) linitial(saop->args);
    rightop = (Node *) lsecond(saop->args);
    right_relids = pull_varnos(rightop);
    expr_op = saop->opno;
    expr_coll = saop->inputcollid;

    index_relid = index->rel->relid;
    opfamily = index->opfamily[indexcol];
    idxcollation = index->indexcollations[indexcol];

    /*
     * We must have indexkey on the left and a pseudo-constant array argument.
     */
    if (match_index_to_operand(leftop, indexcol, index) &&
        !bms_is_member(index_relid, right_relids) &&
        !contain_volatile_functions(rightop))
    {
        if (IndexCollMatchesExprColl(idxcollation, expr_coll) &&
            op_in_opfamily(expr_op, opfamily))
        {
            IndexClause *iclause = makeNode(IndexClause);

            iclause->rinfo = rinfo;
            iclause->indexquals = list_make1(rinfo);
            iclause->lossy = false;
            iclause->indexcol = indexcol;
            iclause->indexcols = NIL;
            return iclause;
        }

        /*
         * We do not currently ask support functions about ScalarArrayOpExprs,
         * though in principle we could.
         */
    }

    return NULL;
}

/*
 * aqp_pgport_match_rowcompare_to_indexcol()
 *      Handles the RowCompareExpr case for match_clause_to_indexcol(),
 *      which see for comments.
 *
 * In this routine we check whether the first column of the row comparison
 * matches the target index column.  This is sufficient to guarantee that some
 * index condition can be constructed from the RowCompareExpr --- the rest
 * is handled by expand_indexqual_rowcompare().
 */
IndexClause *
aqp_pgport_match_rowcompare_to_indexcol(RestrictInfo *rinfo,
                             int indexcol,
                             IndexOptInfo *index)
{
    RowCompareExpr *clause = (RowCompareExpr *) rinfo->clause;
    Index        index_relid;
    Oid            opfamily;
    Oid            idxcollation;
    Node       *leftop,
               *rightop;
    bool        var_on_left;
    Oid            expr_op;
    Oid            expr_coll;

    /* Forget it if we're not dealing with a btree index
     * XXX(zy) allow abtree?
     * */
    if (index->relam != BTREE_AM_OID)
        return NULL;

    index_relid = index->rel->relid;
    opfamily = index->opfamily[indexcol];
    idxcollation = index->indexcollations[indexcol];

    /*
     * We could do the matching on the basis of insisting that the opfamily
     * shown in the RowCompareExpr be the same as the index column's opfamily,
     * but that could fail in the presence of reverse-sort opfamilies: it'd be
     * a matter of chance whether RowCompareExpr had picked the forward or
     * reverse-sort family.  So look only at the operator, and match if it is
     * a member of the index's opfamily (after commutation, if the indexkey is
     * on the right).  We'll worry later about whether any additional
     * operators are matchable to the index.
     */
    leftop = (Node *) linitial(clause->largs);
    rightop = (Node *) linitial(clause->rargs);
    expr_op = linitial_oid(clause->opnos);
    expr_coll = linitial_oid(clause->inputcollids);

    /* Collations must match, if relevant */
    if (!IndexCollMatchesExprColl(idxcollation, expr_coll))
        return NULL;

    /*
     * These syntactic tests are the same as in match_opclause_to_indexcol()
     */
    if (match_index_to_operand(leftop, indexcol, index) &&
        !bms_is_member(index_relid, pull_varnos(rightop)) &&
        !contain_volatile_functions(rightop))
    {
        /* OK, indexkey is on left */
        var_on_left = true;
    }
    else if (match_index_to_operand(rightop, indexcol, index) &&
             !bms_is_member(index_relid, pull_varnos(leftop)) &&
             !contain_volatile_functions(leftop))
    {
        /* indexkey is on right, so commute the operator */
        expr_op = get_commutator(expr_op);
        if (expr_op == InvalidOid)
            return NULL;
        var_on_left = false;
    }
    else
        return NULL;

    /* We're good if the operator is the right type of opfamily member */
    switch (get_op_opfamily_strategy(expr_op, opfamily))
    {
        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber:
        case BTGreaterEqualStrategyNumber:
        case BTGreaterStrategyNumber:
            return aqp_pgport_expand_indexqual_rowcompare(rinfo,
                                               indexcol,
                                               index,
                                               expr_op,
                                               var_on_left);
    }

    return NULL;
}

/*
 * aqp_pgport_expand_indexqual_rowcompare --- expand a single indexqual
 * condition that is a RowCompareExpr
 *
 * It's already known that the first column of the row comparison matches
 * the specified column of the index.  We can use additional columns of the
 * row comparison as index qualifications, so long as they match the index
 * in the "same direction", ie, the indexkeys are all on the same side of the
 * clause and the operators are all the same-type members of the opfamilies.
 *
 * If all the columns of the RowCompareExpr match in this way, we just use it
 * as-is, except for possibly commuting it to put the indexkeys on the left.
 *
 * Otherwise, we build a shortened RowCompareExpr (if more than one
 * column matches) or a simple OpExpr (if the first-column match is all
 * there is).  In these cases the modified clause is always "<=" or ">="
 * even when the original was "<" or ">" --- this is necessary to match all
 * the rows that could match the original.  (We are building a lossy version
 * of the row comparison when we do this, so we set lossy = true.)
 *
 * Note: this is really just the last half of match_rowcompare_to_indexcol,
 * but we split it out for comprehensibility.
 */
IndexClause *
aqp_pgport_expand_indexqual_rowcompare(RestrictInfo *rinfo,
                            int indexcol,
                            IndexOptInfo *index,
                            Oid expr_op,
                            bool var_on_left)
{
    IndexClause *iclause = makeNode(IndexClause);
    RowCompareExpr *clause = (RowCompareExpr *) rinfo->clause;
    int            op_strategy;
    Oid            op_lefttype;
    Oid            op_righttype;
    int            matching_cols;
    List       *expr_ops;
    List       *opfamilies;
    List       *lefttypes;
    List       *righttypes;
    List       *new_ops;
    List       *var_args;
    List       *non_var_args;

    iclause->rinfo = rinfo;
    iclause->indexcol = indexcol;

    if (var_on_left)
    {
        var_args = clause->largs;
        non_var_args = clause->rargs;
    }
    else
    {
        var_args = clause->rargs;
        non_var_args = clause->largs;
    }

    get_op_opfamily_properties(expr_op, index->opfamily[indexcol], false,
                               &op_strategy,
                               &op_lefttype,
                               &op_righttype);

    /* Initialize returned list of which index columns are used */
    iclause->indexcols = list_make1_int(indexcol);

    /* Build lists of ops, opfamilies and operator datatypes in case needed */
    expr_ops = list_make1_oid(expr_op);
    opfamilies = list_make1_oid(index->opfamily[indexcol]);
    lefttypes = list_make1_oid(op_lefttype);
    righttypes = list_make1_oid(op_righttype);

    /*
     * See how many of the remaining columns match some index column in the
     * same way.  As in match_clause_to_indexcol(), the "other" side of any
     * potential index condition is OK as long as it doesn't use Vars from the
     * indexed relation.
     */
    matching_cols = 1;

    while (matching_cols < list_length(var_args))
    {
        Node       *varop = (Node *) list_nth(var_args, matching_cols);
        Node       *constop = (Node *) list_nth(non_var_args, matching_cols);
        int            i;

        expr_op = list_nth_oid(clause->opnos, matching_cols);
        if (!var_on_left)
        {
            /* indexkey is on right, so commute the operator */
            expr_op = get_commutator(expr_op);
            if (expr_op == InvalidOid)
                break;            /* operator is not usable */
        }
        if (bms_is_member(index->rel->relid, pull_varnos(constop)))
            break;                /* no good, Var on wrong side */
        if (contain_volatile_functions(constop))
            break;                /* no good, volatile comparison value */

        /*
         * The Var side can match any key column of the index.
         */
        for (i = 0; i < index->nkeycolumns; i++)
        {
            if (match_index_to_operand(varop, i, index) &&
                get_op_opfamily_strategy(expr_op,
                                         index->opfamily[i]) == op_strategy &&
                IndexCollMatchesExprColl(index->indexcollations[i],
                                         list_nth_oid(clause->inputcollids,
                                                      matching_cols)))
                break;
        }
        if (i >= index->nkeycolumns)
            break;                /* no match found */

        /* Add column number to returned list */
        iclause->indexcols = lappend_int(iclause->indexcols, i);

        /* Add operator info to lists */
        get_op_opfamily_properties(expr_op, index->opfamily[i], false,
                                   &op_strategy,
                                   &op_lefttype,
                                   &op_righttype);
        expr_ops = lappend_oid(expr_ops, expr_op);
        opfamilies = lappend_oid(opfamilies, index->opfamily[i]);
        lefttypes = lappend_oid(lefttypes, op_lefttype);
        righttypes = lappend_oid(righttypes, op_righttype);

        /* This column matches, keep scanning */
        matching_cols++;
    }

    /* Result is non-lossy if all columns are usable as index quals */
    iclause->lossy = (matching_cols != list_length(clause->opnos));

    /*
     * We can use rinfo->clause as-is if we have var on left and it's all
     * usable as index quals.
     */
    if (var_on_left && !iclause->lossy)
        iclause->indexquals = list_make1(rinfo);
    else
    {
        /*
         * We have to generate a modified rowcompare (possibly just one
         * OpExpr).  The painful part of this is changing < to <= or > to >=,
         * so deal with that first.
         */
        if (!iclause->lossy)
        {
            /* very easy, just use the commuted operators */
            new_ops = expr_ops;
        }
        else if (op_strategy == BTLessEqualStrategyNumber ||
                 op_strategy == BTGreaterEqualStrategyNumber)
        {
            /* easy, just use the same (possibly commuted) operators */
            new_ops = list_truncate(expr_ops, matching_cols);
        }
        else
        {
            ListCell   *opfamilies_cell;
            ListCell   *lefttypes_cell;
            ListCell   *righttypes_cell;

            if (op_strategy == BTLessStrategyNumber)
                op_strategy = BTLessEqualStrategyNumber;
            else if (op_strategy == BTGreaterStrategyNumber)
                op_strategy = BTGreaterEqualStrategyNumber;
            else
                elog(ERROR, "unexpected strategy number %d", op_strategy);
            new_ops = NIL;
            forthree(opfamilies_cell, opfamilies,
                     lefttypes_cell, lefttypes,
                     righttypes_cell, righttypes)
            {
                Oid            opfam = lfirst_oid(opfamilies_cell);
                Oid            lefttype = lfirst_oid(lefttypes_cell);
                Oid            righttype = lfirst_oid(righttypes_cell);

                expr_op = get_opfamily_member(opfam, lefttype, righttype,
                                              op_strategy);
                if (!OidIsValid(expr_op))    /* should not happen */
                    elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
                         op_strategy, lefttype, righttype, opfam);
                new_ops = lappend_oid(new_ops, expr_op);
            }
        }

        /* If we have more than one matching col, create a subset rowcompare */
        if (matching_cols > 1)
        {
            RowCompareExpr *rc = makeNode(RowCompareExpr);

            rc->rctype = (RowCompareType) op_strategy;
            rc->opnos = new_ops;
            rc->opfamilies = list_truncate(list_copy(clause->opfamilies),
                                           matching_cols);
            rc->inputcollids = list_truncate(list_copy(clause->inputcollids),
                                             matching_cols);
            rc->largs = list_truncate(copyObject(var_args),
                                      matching_cols);
            rc->rargs = list_truncate(copyObject(non_var_args),
                                      matching_cols);
            iclause->indexquals = list_make1(make_simple_restrictinfo((Expr *) rc));
        }
        else
        {
            Expr       *op;

            /* We don't report an index column list in this case */
            iclause->indexcols = NIL;

            op = make_opclause(linitial_oid(new_ops), BOOLOID, false,
                               copyObject(linitial(var_args)),
                               copyObject(linitial(non_var_args)),
                               InvalidOid,
                               linitial_oid(clause->inputcollids));
            iclause->indexquals = list_make1(make_simple_restrictinfo(op));
        }
    }

    return iclause;
}

/*
 * aqp_pgport_ec_member_matches_indexcol
 *	  Test whether an EquivalenceClass member matches an index column.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
bool
aqp_pgport_ec_member_matches_indexcol(PlannerInfo *root, RelOptInfo *rel,
						   EquivalenceClass *ec, EquivalenceMember *em,
						   void *arg)
{
	IndexOptInfo *index = ((ec_member_matches_arg *) arg)->index;
	int			indexcol = ((ec_member_matches_arg *) arg)->indexcol;
	Oid			curFamily;
	Oid			curCollation;

	Assert(indexcol < index->nkeycolumns);

	curFamily = index->opfamily[indexcol];
	curCollation = index->indexcollations[indexcol];

	/*
	 * If it's a btree index, we can reject it if its opfamily isn't
	 * compatible with the EC, since no clause generated from the EC could be
	 * used with the index.  For non-btree indexes, we can't easily tell
	 * whether clauses generated from the EC could be used with the index, so
	 * don't check the opfamily.  This might mean we return "true" for a
	 * useless EC, so we have to recheck the results of
	 * generate_implied_equalities_for_column; see
	 * match_eclass_clauses_to_index.
	 */
	if (index->relam == BTREE_AM_OID &&
		!list_member_oid(ec->ec_opfamilies, curFamily))
		return false;

	/* We insist on collation match for all index types, though */
	if (!IndexCollMatchesExprColl(curCollation, ec->ec_collation))
		return false;

	return match_index_to_operand((Node *) em->em_expr, indexcol, index);
}


/****************************************************************************
 *                ---- Cardinality estimation ----
 ****************************************************************************/

/*
 * get_loop_count
 *		Choose the loop count estimate to use for costing a parameterized path
 *		with the given set of outer relids.
 *
 * Since we produce parameterized paths before we've begun to generate join
 * relations, it's impossible to predict exactly how many times a parameterized
 * path will be iterated; we don't know the size of the relation that will be
 * on the outside of the nestloop.  However, we should try to account for
 * multiple iterations somehow in costing the path.  The heuristic embodied
 * here is to use the rowcount of the smallest other base relation needed in
 * the join clauses used by the path.  (We could alternatively consider the
 * largest one, but that seems too optimistic.)  This is of course the right
 * answer for single-other-relation cases, and it seems like a reasonable
 * zero-order approximation for multiway-join cases.
 *
 * In addition, we check to see if the other side of each join clause is on
 * the inside of some semijoin that the current relation is on the outside of.
 * If so, the only way that a parameterized path could be used is if the
 * semijoin RHS has been unique-ified, so we should use the number of unique
 * RHS rows rather than using the relation's raw rowcount.
 *
 * Note: for this to work, allpaths.c must establish all baserel size
 * estimates before it begins to compute paths, or at least before it
 * calls create_index_paths().
 */
double
pg_port_get_loop_count(PlannerInfo *root, Index cur_relid, Relids outer_relids)
{
	double		result;
	int			outer_relid;

	/* For a non-parameterized path, just return 1.0 quickly */
	if (outer_relids == NULL)
		return 1.0;

	result = 0.0;
	outer_relid = -1;
	while ((outer_relid = bms_next_member(outer_relids, outer_relid)) >= 0)
	{
		RelOptInfo *outer_rel;
		double		rowcount;

		/* Paranoia: ignore bogus relid indexes */
		if (outer_relid >= root->simple_rel_array_size)
			continue;
		outer_rel = root->simple_rel_array[outer_relid];
		if (outer_rel == NULL)
			continue;
		Assert(outer_rel->relid == outer_relid);	/* sanity check on array */

		/* Other relation could be proven empty, if so ignore */
		if (IS_DUMMY_REL(outer_rel))
			continue;

		/* Otherwise, rel's rows estimate should be valid by now */
		Assert(outer_rel->rows > 0);

		/* Check to see if rel is on the inside of any semijoins */
		rowcount = pg_port_adjust_rowcount_for_semijoins(root,
												 cur_relid,
												 outer_relid,
												 outer_rel->rows);

		/* Remember smallest row count estimate among the outer rels */
		if (result == 0.0 || result > rowcount)
			result = rowcount;
	}
	/* Return 1.0 if we found no valid relations (shouldn't happen) */
	return (result > 0.0) ? result : 1.0;
}

/*
 * Check to see if outer_relid is on the inside of any semijoin that cur_relid
 * is on the outside of.  If so, replace rowcount with the estimated number of
 * unique rows from the semijoin RHS (assuming that's smaller, which it might
 * not be).  The estimate is crude but it's the best we can do at this stage
 * of the proceedings.
 */
double
pg_port_adjust_rowcount_for_semijoins(PlannerInfo *root,
							  Index cur_relid,
							  Index outer_relid,
							  double rowcount)
{
	ListCell   *lc;

	foreach(lc, root->join_info_list)
	{
		SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);

		if (sjinfo->jointype == JOIN_SEMI &&
			bms_is_member(cur_relid, sjinfo->syn_lefthand) &&
			bms_is_member(outer_relid, sjinfo->syn_righthand))
		{
			/* Estimate number of unique-ified rows */
			double		nraw;
			double		nunique;

			nraw = pg_port_approximate_joinrel_size(root, sjinfo->syn_righthand);
			nunique = estimate_num_groups(root,
										  sjinfo->semi_rhs_exprs,
										  nraw,
										  NULL);
			if (rowcount > nunique)
				rowcount = nunique;
		}
	}
	return rowcount;
}

/*
 * Make an approximate estimate of the size of a joinrel.
 *
 * We don't have enough info at this point to get a good estimate, so we
 * just multiply the base relation sizes together.  Fortunately, this is
 * the right answer anyway for the most common case with a single relation
 * on the RHS of a semijoin.  Also, estimate_num_groups() has only a weak
 * dependency on its input_rows argument (it basically uses it as a clamp).
 * So we might be able to get a fairly decent end result even with a severe
 * overestimate of the RHS's raw size.
 */
double
pg_port_approximate_joinrel_size(PlannerInfo *root, Relids relids)
{
	double		rowcount = 1.0;
	int			relid;

	relid = -1;
	while ((relid = bms_next_member(relids, relid)) >= 0)
	{
		RelOptInfo *rel;

		/* Paranoia: ignore bogus relid indexes */
		if (relid >= root->simple_rel_array_size)
			continue;
		rel = root->simple_rel_array[relid];
		if (rel == NULL)
			continue;
		Assert(rel->relid == relid);	/* sanity check on array */

		/* Relation could be proven empty, if so ignore */
		if (IS_DUMMY_REL(rel))
			continue;

		/* Otherwise, rel's rows estimate should be valid by now */
		Assert(rel->rows > 0);

		/* Accumulate product */
		rowcount *= rel->rows;
	}
	return rowcount;
}

/*
 * pg_port_check_index_only
 *		Determine whether an index-only scan is possible for this index.
 */
bool
pg_port_check_index_only(RelOptInfo *rel, IndexOptInfo *index)
{
	bool		result;
	Bitmapset  *attrs_used = NULL;
	Bitmapset  *index_canreturn_attrs = NULL;
	Bitmapset  *index_cannotreturn_attrs = NULL;
	ListCell   *lc;
	int			i;

	/* Index-only scans must be enabled */
	if (!enable_indexonlyscan)
		return false;

	/*
	 * Check that all needed attributes of the relation are available from the
	 * index.
	 */

	/*
	 * First, identify all the attributes needed for joins or final output.
	 * Note: we must look at rel's targetlist, not the attr_needed data,
	 * because attr_needed isn't computed for inheritance child rels.
	 */
	pull_varattnos((Node *) rel->reltarget->exprs, rel->relid, &attrs_used);

	/*
	 * Add all the attributes used by restriction clauses; but consider only
	 * those clauses not implied by the index predicate, since ones that are
	 * so implied don't need to be checked explicitly in the plan.
	 *
	 * Note: attributes used only in index quals would not be needed at
	 * runtime either, if we are certain that the index is not lossy.  However
	 * it'd be complicated to account for that accurately, and it doesn't
	 * matter in most cases, since we'd conclude that such attributes are
	 * available from the index anyway.
	 */
	foreach(lc, index->indrestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, rel->relid, &attrs_used);
	}

	/*
	 * Construct a bitmapset of columns that the index can return back in an
	 * index-only scan.  If there are multiple index columns containing the
	 * same attribute, all of them must be capable of returning the value,
	 * since we might recheck operators on any of them.  (Potentially we could
	 * be smarter about that, but it's such a weird situation that it doesn't
	 * seem worth spending a lot of sweat on.)
	 */
	for (i = 0; i < index->ncolumns; i++)
	{
		int			attno = index->indexkeys[i];

		/*
		 * For the moment, we just ignore index expressions.  It might be nice
		 * to do something with them, later.
		 */
		if (attno == 0)
			continue;

		if (index->canreturn[i])
			index_canreturn_attrs =
				bms_add_member(index_canreturn_attrs,
							   attno - FirstLowInvalidHeapAttributeNumber);
		else
			index_cannotreturn_attrs =
				bms_add_member(index_cannotreturn_attrs,
							   attno - FirstLowInvalidHeapAttributeNumber);
	}

	index_canreturn_attrs = bms_del_members(index_canreturn_attrs,
											index_cannotreturn_attrs);

	/* Do we have all the necessary attributes? */
	result = bms_is_subset(attrs_used, index_canreturn_attrs);

	bms_free(attrs_used);
	bms_free(index_canreturn_attrs);
	bms_free(index_cannotreturn_attrs);

	return result;
}
