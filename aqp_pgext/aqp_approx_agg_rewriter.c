#include "aqp.h"

#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pg_list.h>
#include <parser/parse_coerce.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>

#include "aqp_planner.h"
#include "aqp_math.h"

typedef struct 
{
    bool    has_approx_agg;

    /* 
     * The sample size expression to swr if there is a tablesample swr clause.
     *
     * Currently only one of sample_size_expr or bern_sample_prob_expr can be
     * non-NULL.
     */
    Expr    *sample_size_expr;

    /* 
     * The sample percentage expression (100% as 100) if there is a tablesample
     * bernoulli clause
     *
     * See above.
     */
    Expr    *bern_sample_prob_expr;
} AQPFindAndRewriteApproxAggContext;

static Node *aqp_find_and_rewrite_approx_agg(Node *node, Expr *sample_size_expr,
                                             Expr *bern_sample_prob_expr,
                                             bool *p_has_approx_agg);
static Node *aqp_find_and_rewrite_approx_agg_impl(Node *node,
                                     AQPFindAndRewriteApproxAggContext *cxt);
static Node *aqp_rewrite_approx_sum(Aggref *agg,
                                    AQPFindAndRewriteApproxAggContext *cxt);
static Node *aqp_rewrite_approx_sum_half_ci(Aggref *agg,
                                    AQPFindAndRewriteApproxAggContext *cxt);
static Node *aqp_rewrite_approx_count(Aggref *agg,
                                    AQPFindAndRewriteApproxAggContext *cxt);
static Node *aqp_rewrite_approx_count_half_ci(Aggref *agg,
                                    AQPFindAndRewriteApproxAggContext *cxt);
static Expr *aqp_rewrite_agg_arg_as_ht_estimator(Expr *arg, Oid argtype,
                                    AQPFindAndRewriteApproxAggContext *cxt);
static Expr *aqp_cast_sample_size_expr_as_float8(Expr *sample_size_expr);
static bool aqp_find_and_rewrite_approx_agg_in_sublinks(Node *node);
static bool aqp_find_and_rewrite_approx_agg_in_sublinks_impl(Node *node,
                                                        bool *p_needs_rewrite);

bool
aqp_check_and_rewrite_approx_agg(Query *query)
{
    bool need_post_planner_rewrite = false;
    bool has_tablesample_this_level = false;
    ListCell *lc;
    Expr *sample_size_expr = NULL;
    Expr *bern_sample_prob_expr = NULL;
    
    /* We only rewrite SELECT statements. */
    if (query->commandType != CMD_SELECT)
        return false;

    /* 
     * 1. check for the existence tablesample clause, and/or recursively check
     * and rewrite subqueries.
     */
    foreach(lc, query->rtable)
    {
        RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

        switch (rte->rtekind)
        {
        case RTE_RELATION:
            if (rte->tablesample != NULL)
            {
                TableSampleClause *tsc = rte->tablesample;

                if (has_tablesample_this_level)
                {
                    /* 
                     * XXX We don't support joins of samples for now, though
                     * technically we can (in with joint probability
                     * computation).
                     */
                    elog(ERROR,
                        "only one tablesample is allowed in a FROM clause "
                        "when aqp extension is enabled, but found a second one "
                        "on table %s",
                        get_rel_name(rte->relid));
                }


                if (rte->tablesample->tsmhandler == aqp_swr_tsm_handler_oid)
                {
                    need_post_planner_rewrite = true;
                    has_tablesample_this_level = true;
                    
                    /* 
                     * Save the sample size expression as the first arg of swr():
                     * we may need this variable for rewriting approx_xxx()
                     * aggregation functions.
                     */
                    Assert(list_length(tsc->args) == 1);
                    sample_size_expr = (Expr *) linitial(tsc->args);
                }
                else if (rte->tablesample->tsmhandler == F_TSM_BERNOULLI_HANDLER)
                {
                    has_tablesample_this_level = true;
                    Assert(list_length(tsc->args) == 1);
                    bern_sample_prob_expr = (Expr *) linitial(tsc->args);
                }
            }
            break;

        case RTE_SUBQUERY:
            need_post_planner_rewrite |=
                aqp_check_and_rewrite_approx_agg(rte->subquery);
            break;

        default:
            /* nothing to do with other kind of RTE for now */
        }
    }

    /*
     * 2. Rewrite any aqp_approx_xxx() aggregation functions in the target
     * lists.
     *
     * TODO(zy) handling HAVING clause?
     */
    if (query->hasAggs)
    {
        bool has_approx_agg;

        query->targetList = (List *)
            aqp_find_and_rewrite_approx_agg((Node *) query->targetList,
                                            sample_size_expr,
                                            bern_sample_prob_expr,
                                            &has_approx_agg);

        if (has_approx_agg)
        {
            if (!has_tablesample_this_level)
            {
                elog(ERROR, "approximate aggregation operator is disallowed"
                            "without tablesample swr or bernoulli clause in "
                            "FROM list");
            }
            need_post_planner_rewrite = true;
        }
    }

    /*
     * 3. Check sublinks (subselect in an expression) if there's any. We
     * currently consider quals in jointrees (for now).
     */
    if (query->hasSubLinks)
    {
        bool needs_rewrite;
        
        needs_rewrite = aqp_find_and_rewrite_approx_agg_in_sublinks(
            query->jointree->quals);

        if (needs_rewrite)
            need_post_planner_rewrite = true;
    }
    
    return need_post_planner_rewrite;
}

static Node *
aqp_find_and_rewrite_approx_agg(Node *node,
                                Expr *sample_size_expr,
                                Expr *bern_sample_prob_expr,
                                bool *p_has_approx_agg)
{
    Node *res;
    AQPFindAndRewriteApproxAggContext cxt;

    cxt.has_approx_agg = false;
    cxt.sample_size_expr = sample_size_expr;
    cxt.bern_sample_prob_expr = bern_sample_prob_expr;
    res = expression_tree_mutator(node, aqp_find_and_rewrite_approx_agg_impl,
                                  &cxt);
    *p_has_approx_agg = cxt.has_approx_agg;
    return res;
}

static Node *
aqp_find_and_rewrite_approx_agg_impl(Node *node,
                                     AQPFindAndRewriteApproxAggContext *cxt)
{
    if (IsA(node, Aggref))
    {
        Aggref *agg = (Aggref *) node;

        if (agg->aggfnoid == aqp_approx_sum_oid)
        {
            cxt->has_approx_agg = true;
            return aqp_rewrite_approx_sum(agg, cxt);
        }

        if (agg->aggfnoid == aqp_approx_sum_half_ci_oid)
        {
            cxt->has_approx_agg = true;
            return aqp_rewrite_approx_sum_half_ci(agg, cxt);
        }

        if (agg->aggfnoid == aqp_approx_count_oid ||
            agg->aggfnoid == aqp_approx_count_any_oid)
        {
            cxt->has_approx_agg = true;
            return aqp_rewrite_approx_count(agg, cxt);
        }

        if (agg->aggfnoid == aqp_approx_count_half_ci_oid ||
            agg->aggfnoid == aqp_approx_count_any_half_ci_oid)
        {
            cxt->has_approx_agg = true;
            return aqp_rewrite_approx_count_half_ci(agg, cxt);
        }


        /* 
         * don't descend into Aggref -- there shouldn't be recursive aggregate
         * function calls
         */
        return node;
    }

    return expression_tree_mutator(node, aqp_find_and_rewrite_approx_agg_impl,
                                   cxt);
}

static Node *
aqp_rewrite_approx_sum(Aggref *agg, AQPFindAndRewriteApproxAggContext *cxt)
{
    Oid argtype;
    TargetEntry *arg_tle;
    Expr *arg_expr;
    Expr *res;

    if (!cxt->sample_size_expr && !cxt->bern_sample_prob_expr)
    {
        elog(ERROR, "approximate aggregation must not be used without "
                    "tablesample swr or tablesample bernoulli clause");
    }

    if (agg->aggorder != NIL ||
        agg->aggdistinct != NIL ||
        agg->aggdirectargs != NIL)
    {
        elog(ERROR, "ORDER BY, DISTINCT are not supported in approx_sum()");
    }
    
    Assert(list_length(agg->aggargtypes) == 1);

    argtype = linitial_oid(agg->aggargtypes);
    arg_tle = linitial_node(TargetEntry, agg->args);
    arg_expr = aqp_rewrite_agg_arg_as_ht_estimator(arg_tle->expr,
                                                   argtype,
                                                   cxt);
    
    /* Rewrite APPROX_SUM as SUM in place. */
    agg->aggfnoid = aqp_sum_float8_oid;
    arg_tle = makeTargetEntry(arg_expr, 1, NULL, false);
    agg->args = list_make1(arg_tle);
    agg->aggargtypes = list_make1_oid(FLOAT8OID);

    if (cxt->sample_size_expr)
    {
        /* simple random sample with replacement */
        Expr *sample_size_expr;

        /* cast(sample_size_expr AS FLOAT8) */
        sample_size_expr = aqp_cast_sample_size_expr_as_float8(
                                cxt->sample_size_expr);

        /* 
         * SUM((CAST arg as FLOAT8) / sample_prob()) /
         *  cast(sample_size_expr AS FLOAT8) */
        res = make_opclause(/*opno=*/aqp_float8_div_opno,
                            /*opresulttype=*/FLOAT8OID,
                            /*opretset=*/false,
                            /*leftop=*/(Expr *) agg,
                            /*rightop=*/ sample_size_expr,
                            /*opcollid=*/0,
                            /*inputcollid=*/0);
        ((OpExpr *) res)->opfuncid = aqp_float8_div_funcid;
    }
    else
    {
        /* bernoulli sampling */
        res = (Expr *) agg;
    }

    return (Node *) res;
}

static Node *
aqp_rewrite_approx_sum_half_ci(Aggref *agg,
                               AQPFindAndRewriteApproxAggContext *cxt)
{
    Oid agg_argtype;
    TargetEntry *agg_arg_tle;
    Expr *agg_arg;
    Expr *res;
    Expr *confidence_expr;
    Expr *sample_size_expr;

    Assert(list_length(agg->aggargtypes) == 2);

    if (cxt->bern_sample_prob_expr)
    {
        elog(ERROR, "no CLT-bound available for bernoulli sampling");
    }

    if (!cxt->sample_size_expr)
    {
        elog(ERROR, "approximate aggregation must not be used without "
                    "tablesample swr or tablesample bernoulli clause");
    }

    if (agg->aggorder != NIL ||
        agg->aggdistinct != NIL ||
        agg->aggdirectargs != NIL)
    {
        elog(ERROR, "ORDER BY, DISTINCT are not supported in approx_sum_half_ci()");
    }


    /* confidence level */
    Assert(lsecond_oid(agg->aggargtypes) == FLOAT8OID);
    confidence_expr = lsecond_node(TargetEntry, agg->args)->expr;

    /* 
     * ht estimator: cast arg as float 8 / sample_prob
     */
    agg_argtype = linitial_oid(agg->aggargtypes);
    agg_arg_tle = linitial_node(TargetEntry, agg->args);
    agg_arg = aqp_rewrite_agg_arg_as_ht_estimator(agg_arg_tle->expr,
                                                  agg_argtype,
                                                  cxt);

    /* simple random sample with replacement */

    /* aqp.float8_acuum(CAST arg AS FLOAT8 / sample_prob()) */
    agg->aggfnoid = aqp_float8_accum_oid;
    agg_arg_tle = makeTargetEntry(agg_arg, 1, NULL, false);
    agg->args = list_make1(agg_arg_tle);
    agg->aggargtypes = list_make1_oid(FLOAT8OID);
    
    /* sample_size */
    sample_size_expr = aqp_cast_sample_size_expr_as_float8(
                            cxt->sample_size_expr);

    /* construct final func call */
    res = (Expr *) makeFuncExpr(/*funcid=*/aqp_clt_half_ci_final_func_oid,
                                /*rettype=*/FLOAT8OID,
                                /*args=*/list_make3(agg,
                                                    confidence_expr,
                                                    sample_size_expr),
                                /*funccollid=*/InvalidOid,
                                /*inputcollid=*/InvalidOid,
                                /*CoercionForm=*/COERCE_EXPLICIT_CALL);

    return (Node *) res;
}

static Node *
aqp_rewrite_approx_count(Aggref *agg, AQPFindAndRewriteApproxAggContext *cxt)
{
    Expr *res;
    Expr *one;
    Expr *arg_expr;
    TargetEntry *arg_tle;
    Expr *aggfilter;

    if (!cxt->sample_size_expr && !cxt->bern_sample_prob_expr)
    {
        elog(ERROR, "approximate aggregation must not be used without "
                    "tablesample swr or tablesample bernoulli clause");
    }

    if (agg->aggorder != NIL ||
        agg->aggdistinct != NIL ||
        agg->aggdirectargs != NIL)
    {
        elog(ERROR, "ORDER BY, DISTINCT are not supported in approx_count()");
    }

    Assert(list_length(agg->aggargtypes) <= 1);
    Assert(agg->aggtype == FLOAT8OID);

    /* 
     * If there is an argument, we need to add a NULL test as the filter
     * because NULL values shouldn't be counted.
     */
    if (list_length(agg->aggargtypes) == 1)
    {
        NullTest *n = makeNode(NullTest);
        TargetEntry *tle = linitial_node(TargetEntry, agg->args);
        Oid argtype = linitial_oid(agg->aggargtypes);
        n->arg = tle->expr;
        n->nulltesttype = IS_NOT_NULL;
        n->location = -1;
        n->argisrow = type_is_rowtype(argtype);

        aggfilter = (Expr *) n;
    }
    else
    {
        aggfilter = NULL;
    }
    
    /* 
     * In case there is a pre-existing aggfilter in this aggregation,
     * we want to preserve that.
     */
    aggfilter = (Expr *) make_and_qual((Node *) aggfilter,
                                       (Node *) agg->aggfilter);
    
    /* 1.0 / sample_prob() */
    one = (Expr *) makeConst(FLOAT8OID,
                             0,
                             InvalidOid,
                             8,
                             Float8GetDatum(1.0),
                             false,
                             USE_FLOAT8_BYVAL);
    arg_expr = aqp_rewrite_agg_arg_as_ht_estimator(one, FLOAT8OID, cxt);
    
    agg->aggfnoid = aqp_sum_float8_oid;
    arg_tle = makeTargetEntry(arg_expr, 1, NULL, false);
    agg->args = list_make1(arg_tle);
    agg->aggargtypes = list_make1_oid(FLOAT8OID);
    agg->aggfilter = aggfilter;

    if (cxt->sample_size_expr)
    {
        /* simple random sample with replacement */
        Expr *sample_size_expr;
    
        /* cast(sample_size_expr AS FLOAT8) */
        sample_size_expr = aqp_cast_sample_size_expr_as_float8(
                                cxt->sample_size_expr);


        /* SUM(1.0 / sample_prob()) / cast(sample_size_expr AS FLOAT8) */
        res = make_opclause(/*opno=*/aqp_float8_div_opno,
                            /*opresulttype=*/FLOAT8OID,
                            /*opretset=*/false,
                            /*leftop=*/(Expr *) agg,
                            /*rightop=*/ sample_size_expr,
                            /*opcollid=*/0,
                            /*inputcollid=*/0);
        ((OpExpr *) res)->opfuncid = aqp_float8_div_funcid;
    }
    else
    {
        /* bernoulli sampling */
        res = (Expr *) agg;
    }

    return (Node *) res;
}

static Node *
aqp_rewrite_approx_count_half_ci(Aggref *agg,
                                 AQPFindAndRewriteApproxAggContext *cxt)
{
    bool is_count_star;
    Expr *confidence_expr;
    Expr *aggfilter;
    Expr *arg_expr;
    TargetEntry *arg_tle;
    Expr *one;
    Expr *res;
    Expr *sample_size_expr;

    if (cxt->bern_sample_prob_expr)
    {
        elog(ERROR, "no CLT-bound available for bernoulli sampling");
    }

    if (!cxt->sample_size_expr)
    {
        elog(ERROR, "approximate aggregation must not be used without "
                    "tablesample swr or tablesample bernoulli clause");
    }

    if (agg->aggorder != NIL ||
        agg->aggdistinct != NIL ||
        agg->aggdirectargs != NIL)
    {
        elog(ERROR, "ORDER BY, DISTINCT are not supported in approx_count()");
    }
    
    Assert(list_length(agg->aggargtypes) <= 2);
    is_count_star = list_length(agg->aggargtypes) == 1; 

    if (is_count_star)
    {
        confidence_expr = linitial_node(TargetEntry, agg->args)->expr;
        aggfilter = NULL;
    }
    else
    {
        NullTest *n;
        TargetEntry *tle;
        Oid argtype;

        confidence_expr = lsecond_node(TargetEntry, agg->args)->expr;

        n = makeNode(NullTest);
        tle = linitial_node(TargetEntry, agg->args);
        argtype = linitial_oid(agg->aggargtypes);
        n->arg = tle->expr;
        n->nulltesttype = IS_NOT_NULL;
        n->location = -1;
        n->argisrow = type_is_rowtype(argtype);
        aggfilter = (Expr *) n;
    }
    
    aggfilter = (Expr *) make_and_qual((Node *) aggfilter,
                                       (Node *) agg->aggfilter);


    /* 1.0 / sample_prob() */
    one = (Expr *) makeConst(FLOAT8OID,
                             0,
                             InvalidOid,
                             8,
                             Float8GetDatum(1.0),
                             false,
                             USE_FLOAT8_BYVAL);
    arg_expr = aqp_rewrite_agg_arg_as_ht_estimator(one, FLOAT8OID, cxt);

    agg->aggfnoid = aqp_float8_accum_oid;
    arg_tle = makeTargetEntry(arg_expr, 1, NULL, false);
    agg->args = list_make1(arg_tle);
    agg->aggargtypes = list_make1_oid(FLOAT8OID);
    agg->aggfilter = aggfilter;

    sample_size_expr = aqp_cast_sample_size_expr_as_float8(
                            cxt->sample_size_expr);

    /* construct final func call */
    res = (Expr *) makeFuncExpr(/*funcid=*/aqp_clt_half_ci_final_func_oid,
                                /*rettype=*/FLOAT8OID,
                                /*args=*/list_make3(agg,
                                                    confidence_expr,
                                                    sample_size_expr),
                                /*funccollid=*/InvalidOid,
                                /*inputcollid=*/InvalidOid,
                                /*CoercionForm=*/COERCE_EXPLICIT_CALL);
    return (Node *) res;
}

static Expr *
aqp_rewrite_agg_arg_as_ht_estimator(Expr *arg, Oid argtype,
                                    AQPFindAndRewriteApproxAggContext *cxt)
{
    Expr *sample_prob_expr;

    /* cast arg as FLOAT8 */
    if (argtype != FLOAT8OID)
    {
        arg = (Expr *)
            coerce_to_target_type(NULL, (Node *) arg, argtype,
                                  FLOAT8OID, 0,
                                  COERCION_IMPLICIT,
                                  COERCE_IMPLICIT_CAST,
                                  -1);
    }
    
    /* sample_prob */
    if (cxt->sample_size_expr)
    {
        /* simple random sample with replacement: sample_prob() */
        sample_prob_expr = (Expr *) makeFuncExpr(aqp_sample_prob_oid,
                                                 FLOAT8OID,
                                                 NIL,
                                                 0, 0,
                                                 COERCE_EXPLICIT_CALL);
    }
    else
    {
        Expr *leftop,
             *rightop;

        /* 
         * bernoulli sampling: bern_sample_prob_expr / 100.0
         * Note: bern_sample_prob_expr has type FLOAT4, so we need to use
         * float48div.
         */
        Assert(cxt->bern_sample_prob_expr);
        leftop = copyObject(cxt->bern_sample_prob_expr);
        rightop = (Expr *) makeConst(FLOAT8OID,
                                     0,
                                     InvalidOid,
                                     8,
                                     Float8GetDatum(100.0),
                                     false,
                                     USE_FLOAT8_BYVAL);
        sample_prob_expr = make_opclause(/*opno=*/aqp_float48_div_opno,
                                         /*opresulttype=*/FLOAT8OID,
                                         /*opretset=*/false,
                                         /*leftop=*/leftop,
                                         /*rightop=*/rightop,
                                         /*opcollid=*/0,
                                         /*inputcollid=*/0);
        ((OpExpr *) sample_prob_expr)->opfuncid = aqp_float48_div_funcid;
                                                
    }

    /* (CAST arg AS FLOAT8) / sample_prob */ 
    arg = make_opclause(/*opno=*/aqp_float8_div_opno,
                        /*opresulttype=*/FLOAT8OID,
                        /*opretset=*/false,
                        /*leftop=*/arg,
                        /*rightop=*/sample_prob_expr,
                        /*opcollid=*/0,
                        /*inputcollid=*/0);
    ((OpExpr *) arg)->opfuncid = aqp_float8_div_funcid;
    return arg;
}

static Expr *
aqp_cast_sample_size_expr_as_float8(Expr *sample_size_expr)
{
    Oid sample_size_origtype;

    sample_size_expr = copyObject(sample_size_expr);
    sample_size_origtype = exprType((Node *) sample_size_expr);
    if (sample_size_origtype != FLOAT8OID)
    {
        sample_size_expr = (Expr *)
            coerce_to_target_type(NULL,
                                  (Node *) sample_size_expr,
                                  sample_size_origtype,
                                  FLOAT8OID, 0,
                                  COERCION_IMPLICIT,
                                  COERCE_IMPLICIT_CAST,
                                  -1);
    }

    return sample_size_expr;
}

static bool
aqp_find_and_rewrite_approx_agg_in_sublinks(Node *node)
{
    bool needs_rewrite;
    (void) expression_tree_walker(
            node,
            aqp_find_and_rewrite_approx_agg_in_sublinks_impl,
            &needs_rewrite);
    return needs_rewrite;
}

static bool
aqp_find_and_rewrite_approx_agg_in_sublinks_impl(Node *node,
                                                 bool *p_needs_rewrite)
{
    if (node == NULL)
        return false;

    if (IsA(node, SubLink))
    {
        SubLink *s = (SubLink *) node;

        bool needs_rewrite =
            aqp_check_and_rewrite_approx_agg((Query *) s->subselect);
        if (needs_rewrite)
            *p_needs_rewrite = true;
        return false;
    }

    return expression_tree_walker(
            node,
            aqp_find_and_rewrite_approx_agg_in_sublinks_impl,
            p_needs_rewrite);
}

