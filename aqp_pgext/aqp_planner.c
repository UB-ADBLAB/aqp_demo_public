#include "aqp.h"

#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/print.h>
#include <optimizer/planner.h>
#include <parser/parse_func.h>
#include <utils/regproc.h>
#include <access/table.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>

#include "aqp_planner.h"
#include "aqp_swrscan.h"

typedef struct AQPSampleProbRewriterContext
{
    List *rtable;
    Plan* swr_scan;
    Plan* first_node_with_sample_prob;
    bool in_first_sample_prob_subtree;
    bool is_sample_func;
} AQPSampleProbRewriterContext;

typedef struct AQPCheckFuncExistsContext
{
    Oid funcid;
    bool found;
} AQPCheckFuncExistsContext;

typedef struct
{
    Index scanrelid;
    int natts;
} AQPRewriteDummyVarInAQPSWRScanForExplainContext;

static PlannedStmt* aqp_planner(Query* parse, 
                                const char* query_string,
                                int cursorOptions,
                                ParamListInfo boundParams);
static void aqp_sample_prob_rewriter(Plan *plan, List *rtable);
static void aqp_sample_prob_rewriter_imp(Plan *plan,
                                         AQPSampleProbRewriterContext *cxt);
static Var *aqp_create_sample_prob_var(List *targetlist, Index var_no);
static Node *aqp_replace_all_sample_prob_with_var(Node *node, Var *new_var);

static void aqp_locate_sample_prob_and_rewrite_swr_scan(
                Plan *plan, AQPSampleProbRewriterContext *cxt);
static void aqp_append_targetlist_entry(Plan *plan, Var *var);
static bool aqp_check_func_exists(Node *node, Oid oid);
static bool aqp_check_func_exists_imp(Node *node,
                                      AQPCheckFuncExistsContext *cxt);
static AttrNumber aqp_find_table_natts(List *rtable,
                                       Index scanrelid);
static Node* aqp_rewrite_dummy_var_in_aqp_swrscan_for_explain(Node *node,
                                                              Index scanrelid,
                                                              int natts);
static Node* aqp_rewrite_dummy_var_in_aqp_swrscan_for_explain_impl(
    Node *node,
    AQPRewriteDummyVarInAQPSWRScanForExplainContext *cxt);
static bool aqp_lookup_fn_oids(void);
static Oid aqp_lookup_fn_oid(const char *funcname, int nargs,
                             const Oid *argtypes);

static planner_hook_type prev_planner_hook = NULL;

bool aqp_fn_oid_cached = false;
Oid aqp_sample_prob_oid = InvalidOid;
Oid aqp_swr_tsm_handler_oid = InvalidOid;
Oid aqp_sum_float8_oid = InvalidOid;
Oid aqp_float8_mul_opno = InvalidOid;
Oid aqp_float8_mul_funcid = InvalidOid;
Oid aqp_float8_div_opno = InvalidOid;
Oid aqp_float8_div_funcid = InvalidOid;
Oid aqp_float48_div_opno = InvalidOid;
Oid aqp_float48_div_funcid = InvalidOid;
Oid aqp_erf_inv_oid = InvalidOid;

Oid aqp_approx_sum_oid = InvalidOid;
Oid aqp_approx_sum_half_ci_oid = InvalidOid;
Oid aqp_approx_count_oid = InvalidOid;
Oid aqp_approx_count_half_ci_oid = InvalidOid;
Oid aqp_approx_count_any_oid = InvalidOid;
Oid aqp_approx_count_any_half_ci_oid = InvalidOid;

Oid aqp_float8_accum_oid = InvalidOid;
Oid aqp_clt_half_ci_final_func_oid = InvalidOid;

void
aqp_setup_planner_hook(void)
{
    prev_planner_hook = planner_hook;
    planner_hook = aqp_planner;
}

static PlannedStmt*
aqp_planner(Query* parse, 
            const char* query_string,
            int cursorOptions,
            ParamListInfo boundParams)
{
    PlannedStmt *plan;
    bool need_post_planner_rewrite = false;

    if (!aqp_lookup_fn_oids())
    {
        ereport(WARNING,
                errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("aqp extension functions not found. "
                       "Plan rewriter disabled."));
    }
    
    /* 
     * We rewrite all approx_xxx aggregate functions before query planning.
     */
    if (aqp_fn_oid_cached)
    {
        need_post_planner_rewrite = aqp_check_and_rewrite_approx_agg(parse);

        if (Debug_print_parse || Debug_print_rewritten)
        {
            elog_node_display(LOG, "rewritten parse tree by module AQP",
                              parse, Debug_pretty_print);
        }
    }

    /*
        remember the previous hook then hook the new planner 
    */
    if(prev_planner_hook)
        plan = prev_planner_hook(parse, query_string,
                                 cursorOptions, boundParams);
    else
        plan = standard_planner(parse, query_string,
                                cursorOptions, boundParams);
    
    if (aqp_fn_oid_cached && need_post_planner_rewrite)
    {
        ListCell *lc;

        aqp_fix_swrscan_exprs(plan->planTree);
        aqp_sample_prob_rewriter(plan->planTree, plan->rtable);

        foreach(lc, plan->subplans)
        {
            Plan *subplan = (Plan *) lfirst(lc);
            aqp_fix_swrscan_exprs(subplan);
            aqp_sample_prob_rewriter(subplan, plan->rtable);
        }
    }

    return plan;
}

/* 
 * implement the plan rewriting logic for
 * 1) If there's a tablesample swr, add an extra output probability.  Note
 * that, tablesample swr should have been transformed into a swr scan or
 * swr index only scan node at this point.
 *
 * 2) For each node at or above the SWRScan or SWRIndexOnlyScan that is a
 * tablesample swr, append a new target to its target list that references the
 * probability column in its subplan (or indextlist). However, do not descend
 * into sub-queries.
 *
 * 3) If sample_prob() appears in any expression, replace it with a reference
 * to the probability output from its child plan (or indextlist).
 */
static void
aqp_sample_prob_rewriter(Plan *plan, List *rtable){
    AQPSampleProbRewriterContext cxt;
    cxt.rtable = rtable;
    cxt.first_node_with_sample_prob = NULL;
    cxt.is_sample_func = false;
    cxt.swr_scan =NULL;
    aqp_sample_prob_rewriter_imp(plan, &cxt);
}


static void
aqp_sample_prob_rewriter_imp(Plan *plan, AQPSampleProbRewriterContext *cxt)
{
    Plan *swr_scan;
    if(plan == NULL){
        return;
    }
   
    /*
        for the case of subquery scan in from list 
    */
    if (IsA(plan, SubqueryScan))
    {
        /* rewrite subplan */
        SubqueryScan *subscan = (SubqueryScan *) plan;
        Plan *subplan = subscan->subplan;
        
        /* 
         * Be sure to call the rewriter with a fresh cxt because subplan
         * is independent from the parent plan.
         */
        aqp_sample_prob_rewriter(subplan, cxt->rtable);
    }
     /*
        if there is no swrscan node, however there is a sample_prob() func, 
        this is an error situation
    */
    if (cxt->swr_scan != NULL)
    {
        if (aqp_check_func_exists((Node*) plan->targetlist, aqp_sample_prob_oid))
        {
            elog(ERROR, "sample_prob() appears without swr scan in subtree");    
        }
    }

    //do something before goto lefttree
    //wirte dow isn, check if sample prob, 
    // if have sample prob && first node with sample prob is null, 
    //remeber as first smaple probe
    aqp_locate_sample_prob_and_rewrite_swr_scan(plan, cxt);
    
    //write down wheather there is a index scan node before we go to left or right
    //if the node is not a index scan node;
    swr_scan = cxt->swr_scan;
    //get into left tree
    aqp_sample_prob_rewriter_imp(plan->lefttree, cxt);
    //case 2.1, before leftree ios is null, after enter left tree ios is not null
    //the ios is on lefttree
    if(swr_scan == NULL && cxt->swr_scan != NULL &&
        cxt->first_node_with_sample_prob != NULL &&
        cxt->in_first_sample_prob_subtree)
    {
        Var *sample_prob_var;
        //construct a var
        sample_prob_var = aqp_create_sample_prob_var(plan->lefttree->targetlist, OUTER_VAR);
        //change the target list for the son left tree
        if (plan != cxt->first_node_with_sample_prob)
            aqp_append_targetlist_entry(plan, sample_prob_var);
        //mutate targetlist entire 
        plan->targetlist = (List *) aqp_replace_all_sample_prob_with_var((Node *)plan->targetlist, sample_prob_var);
        if (plan != cxt->first_node_with_sample_prob)
            pfree(sample_prob_var);
    }

    swr_scan = cxt->swr_scan;
    //do something after goto lefttree and before goto righttree
    aqp_sample_prob_rewriter_imp(plan->righttree, cxt);
    /*
    case 2.2 before right tree ios is null, after right tree ios is not null, 
    ios is on righttree
    */
    if(swr_scan == NULL && cxt->swr_scan != NULL &&
        cxt->first_node_with_sample_prob != NULL &&
        cxt->in_first_sample_prob_subtree) {
        Var *sample_prob_var;
        //construct a var
        sample_prob_var = aqp_create_sample_prob_var(plan->righttree->targetlist, INNER_VAR);
        //change the target list for the son left tree
        if (plan != cxt->first_node_with_sample_prob)
            aqp_append_targetlist_entry(plan, sample_prob_var);
        //mutate targetlist entire 
        plan->targetlist = (List *) aqp_replace_all_sample_prob_with_var((Node *)plan->targetlist, sample_prob_var);
        if (plan != cxt->first_node_with_sample_prob)
            pfree(sample_prob_var);
    }

    if (cxt->swr_scan == NULL &&
            /*
             * If this node is the first node with sample_prob() call in the target
             * list, then we don't need to call aqp_check_func_exists() again
             * to check for that.
             */
            (cxt->first_node_with_sample_prob == plan ||
                aqp_check_func_exists((Node *) plan->targetlist,
                                      aqp_sample_prob_oid)))
    {
        elog(ERROR, "sample_prob() appears without swr scan in subtree");
    }

    if (cxt->first_node_with_sample_prob == plan)
    {
        cxt->in_first_sample_prob_subtree = false;
    }

    //case 1 before enter subtree is null, after subtree also null 
    // means there is no ios 
    //case 3 before enter subtree is not null, after enter subtree also not null 
// clear first node with sample probe if its my self 

}

static Var *
aqp_create_sample_prob_var(List *targetlist, Index var_no){
    TargetEntry *target_entry;
    Var *new_var;
    Assert(list_length(targetlist) > 0);
    target_entry = llast_node(TargetEntry, targetlist);
    Assert(IsA(target_entry->expr, Var));
    new_var = (Var*)copyObject(target_entry->expr);
    new_var->varno = var_no;
    new_var->varattno = target_entry->resno;
    return new_var;
}

// wirte a fun to go over TARGETLIST only. target list replace funcexpr with a VAR, where 
// var no is the resno from  
static Node *
aqp_replace_all_sample_prob_with_var(Node *node, Var *new_var){
    if (node == NULL)
        return NULL;
    if(IsA(node, FuncExpr)){
        FuncExpr *  func_node =  (FuncExpr*) node;
        if(func_node->funcid == aqp_sample_prob_oid)
        {
            Var *new_node = copyObject(new_var);
            return (Node *) new_node;
        }
    }
    return expression_tree_mutator(node, aqp_replace_all_sample_prob_with_var,
                                   new_var);
}


//check if the node is index only scan node and its a function call
static void 
aqp_locate_sample_prob_and_rewrite_swr_scan(Plan *plan,
                                            AQPSampleProbRewriterContext *cxt)
{   
    /*
        sample prob can locate at the same node with index scan node, thus, 
        first we need to check if this node contains a sample prob( has the sample_prob)
        function then check if the node is a index only scan node. 
    */ 
    
    if(cxt->first_node_with_sample_prob == NULL &&
        aqp_check_func_exists((Node *) plan->targetlist, aqp_sample_prob_oid)){
        cxt->first_node_with_sample_prob = plan;
        cxt->in_first_sample_prob_subtree = true;
    }

    // the node is swr index only scan  or swr scan 
    
    /*
     *  if the node is an swr indexonly scan node, it would be necessary to change
     *  the indextlist the targetlist could be empty so we dont change it here. 
     */
    /*
        if the plan is the aqp_swr index only scan
    */
    if (aqp_plan_is_aqp_swrindexonlyscan(plan))
    {
        /*
        if the node is SwrIndexOnly, cast the node to 
        an custom scan scan type 
        */
        CustomScan *scan = (CustomScan *) plan;
        if (cxt->first_node_with_sample_prob == NULL)
        {
            cxt->swr_scan = plan;
        }
        else
        {
            TargetEntry *extra_entry;
            Var *extra_var;
            if (cxt->swr_scan != NULL)
            {
                elog(ERROR, "ambiguous sample_prob() function call");
            }
            Assert(cxt->in_first_sample_prob_subtree); // TODO check this
            cxt->swr_scan = plan;

            extra_entry = makeNode(TargetEntry);
            extra_var = makeNode(Var);

            extra_var->varno =  INDEX_VAR;
            extra_var->varattno = AQPSampleProbAttributeNumber;
            extra_var->vartype = FLOAT8OID;
            extra_var->vartypmod = 0;
            extra_var->varcollid = InvalidOid;
            extra_var->varlevelsup =0;
            extra_var->varnosyn = 0;
            extra_var->varattnosyn = InvalidAttrNumber;
            extra_var->location = -1;

            extra_entry->expr = (Expr *) extra_var;
            extra_entry->resno = list_length(scan->custom_scan_tlist) + 1;
            extra_entry->resname = "sample_prob";
            extra_entry->ressortgroupref = 0;
            extra_entry->resorigcol = InvalidAttrNumber;
            extra_entry->resorigtbl = InvalidOid;
            extra_entry->resjunk = false;

            scan->custom_scan_tlist = lappend(scan->custom_scan_tlist, extra_entry);
            
            /* this will be used for */
            extra_var = copyObject(extra_var);
            extra_var->varattno = extra_entry->resno;

            /* 
             * If the target list has any sample_prob() call, we need to
             * replace it. Note that, this is necessary because we don't have
             * subtrees. That means, cxt->swr_scan is alreay found before
             * desceding into subtrees, and thus the code to rewrite
             * sample_prob() won't be triggered.
             */
            scan->scan.plan.targetlist = (List *)
                aqp_replace_all_sample_prob_with_var(
                    (Node *) scan->scan.plan.targetlist,
                    extra_var);

            if (cxt->first_node_with_sample_prob != plan)
            {
                /* 
                 * We are not the first node with a sample_prob() call, so
                 * we'll need to add one extra target entry to return the
                 * sample_prob column to the parent.
                 */
                aqp_append_targetlist_entry((Plan *) scan, copyObject(extra_var));
            }
            else
            {
                /* 
                 * This is not in the plan tree if we do not append it to the
                 * target list. Note that
                 * aqp_replace_all_sample_prob_with_var() always makes a copy
                 * of the variable.
                 */
                pfree(extra_var);
            }
        }
    }
    else if (aqp_plan_is_aqp_swrscan(plan))
    {
        CustomScan *scan = (CustomScan *) plan;
        if (cxt->first_node_with_sample_prob == NULL)
        {
            cxt->swr_scan = plan;
        }
        else
        {
            Var *extra_var;
            if (cxt->swr_scan != NULL)
            {
                elog(ERROR, "ambiguous sample_prob() function call");
            }
            Assert(cxt->in_first_sample_prob_subtree); // TODO check this
            cxt->swr_scan = plan;

            extra_var = makeNode(Var);

            extra_var->varno = scan->scan.scanrelid;
            extra_var->varattno =
                aqp_find_table_natts(cxt->rtable, scan->scan.scanrelid)
                + 1; /* dummy variable with attno == 1 + natts */
            extra_var->vartype = FLOAT8OID;
            extra_var->vartypmod = 0;
            extra_var->varcollid = InvalidOid;
            extra_var->varlevelsup =0;
            extra_var->varnosyn = 0;
            extra_var->varattnosyn = InvalidAttrNumber;
            extra_var->location = -1;

            /* 
             * If the target list has any sample_prob() call, we need to
             * replace it. Note that, this is necessary because we don't have
             * subtrees. That means, cxt->swr_scan is alreay found before
             * desceding into subtrees, and thus the code to rewrite
             * sample_prob() won't be triggered.
             */
            scan->scan.plan.targetlist = (List *)
                aqp_replace_all_sample_prob_with_var(
                    (Node *) scan->scan.plan.targetlist,
                    extra_var);

            if (cxt->first_node_with_sample_prob != plan)
            {
                /* 
                 * We are not the first node with a sample_prob() call, so
                 * we'll need to add one extra target entry to return the
                 * sample_prob column to the parent.
                 */
                aqp_append_targetlist_entry((Plan *) scan, extra_var);
            }
            else
            {
                pfree(extra_var);
            }
        }
    }
}

static void 
aqp_append_targetlist_entry(Plan *plan, Var *var)
{ 
    TargetEntry *tle = makeNode(TargetEntry);
    tle->expr = (Expr *) var;
    tle->resno = list_length(plan->targetlist) +1;
    tle->resname = "sample_prob";
    tle->ressortgroupref = 0;
    tle->resorigcol = InvalidAttrNumber;
    tle->resorigtbl = InvalidOid;
    tle->resjunk = false;
    plan->targetlist = lappend(plan->targetlist, tle);
}

static bool
aqp_check_func_exists(Node *node, Oid funcid){
    AQPCheckFuncExistsContext cxt;
    cxt.funcid = funcid;
    cxt.found = false;
    (void) expression_tree_walker(node, aqp_check_func_exists_imp, &cxt);
    return cxt.found;
}


/*
 * check if this funcexpr is the function
 */
static bool
aqp_check_func_exists_imp(Node *node, AQPCheckFuncExistsContext *cxt)
{
    if (node == NULL)
        return false;
    if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == cxt->funcid)
    {
        cxt->found = true;
        return true;
    }
    return expression_tree_walker(node, aqp_check_func_exists_imp, cxt);
}

static AttrNumber
aqp_find_table_natts(List *rtable, Index scanrelid){
    AttrNumber  attn ;
    RangeTblEntry *relational_rte =
        list_nth_node(RangeTblEntry, rtable, scanrelid-1);
    Relation relation = table_open(relational_rte->relid, NoLock);
    TupleDesc descr = RelationGetDescr(relation);
    attn =  descr->natts;
    table_close(relation, NoLock);
    return attn;
}

void
aqp_rewrite_sample_prob_dummy_var_for_explain(PlanState *state)
{
    check_stack_depth();

    if (state == NULL)
        return ;

    if (aqp_plan_is_aqp_swrscan(state->plan))
    {
        ScanState *scanstate = (ScanState *) state;
        TupleDesc descr = RelationGetDescr(scanstate->ss_currentRelation);
        Index scanrelid = ((Scan *) state->plan)->scanrelid;
        int natts = descr->natts;
        
        state->plan->targetlist = (List *)
            aqp_rewrite_dummy_var_in_aqp_swrscan_for_explain(
                (Node *) state->plan->targetlist, scanrelid, natts);
        return ;
    }
    else if (aqp_plan_is_aqp_swrindexonlyscan(state->plan))
    {
        CustomScan *cscan = (CustomScan *) state->plan;
        if (list_length(cscan->custom_scan_tlist) > 0)
        {
            TargetEntry *tle = llast_node(TargetEntry, cscan->custom_scan_tlist);
            if (IsA(tle->expr, Var) && ((Var *) tle->expr)->varno == INDEX_VAR)
            {
                FuncExpr *func;
                Var *var = (Var *) tle->expr;

                /* 
                 * This is the sample_prob column, and replace it with
                 * sample_prob() function call.
                 */
                Assert(var->varattno == AQPSampleProbAttributeNumber);
                
                func = makeFuncExpr(/*funcid=*/aqp_sample_prob_oid,
                                    /*rettype=*/var->vartype,
                                    /*args=*/NIL,
                                    /*funccollid=*/var->varcollid,
                                    /*inputcollid=*/InvalidOid,
                                    /*fformat=*/COERCE_EXPLICIT_CALL);
                tle->expr = (Expr *) func;
                /* 
                 * No worries about leaking the old Var since we are at the end
                 * of an EXPLAIN query.
                 */
            }
        }
        return ;
    }
    else if (IsA(state->plan, SubqueryScan))
    {
        aqp_rewrite_sample_prob_dummy_var_for_explain(
            ((SubqueryScanState *) state)->subplan);
    }

    aqp_rewrite_sample_prob_dummy_var_for_explain(state->lefttree);
    aqp_rewrite_sample_prob_dummy_var_for_explain(state->righttree);
}

static Node*
aqp_rewrite_dummy_var_in_aqp_swrscan_for_explain(Node *node, Index scanrelid,
                                                 int natts)
{
    AQPRewriteDummyVarInAQPSWRScanForExplainContext cxt;
    cxt.scanrelid = scanrelid;
    cxt.natts = natts;
    return aqp_rewrite_dummy_var_in_aqp_swrscan_for_explain_impl(node, &cxt);
}

static Node*
aqp_rewrite_dummy_var_in_aqp_swrscan_for_explain_impl(
    Node *node,
    AQPRewriteDummyVarInAQPSWRScanForExplainContext *cxt)
{
    if (node == NULL)
        return NULL;

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;
        if (var->varno == cxt->scanrelid && var->varattno == cxt->natts + 1)
        {
            FuncExpr *func = makeFuncExpr(/*funcid=*/aqp_sample_prob_oid,
                                          /*rettype=*/var->vartype,
                                          /*args=*/NIL,
                                          /*funccollid=*/var->varcollid,
                                          /*inputcollid=*/InvalidOid,
                                          /*fformat=*/COERCE_EXPLICIT_CALL);
            return (Node *) func;
        }
    }

    return expression_tree_mutator(node,
            aqp_rewrite_dummy_var_in_aqp_swrscan_for_explain_impl, cxt);
}

static Oid
aqp_lookup_fn_oid(const char *funcname, int nargs, const Oid *argtypes)
{
    List *name;
    Oid res;
    name = stringToQualifiedNameList(funcname);
    res = LookupFuncName(name, nargs, argtypes, /*missing_ok=*/true);
    list_free(name);
    return res;
}

static Oid
aqp_lookup_op_oid(const char *funcname, Oid oprleft, Oid oprright)
{
    List *name;
    Oid res;
    name = stringToQualifiedNameList(funcname);
    res = OpernameGetOprid(name, oprleft, oprright);
    list_free(name);
    return res;
}

static
bool aqp_lookup_fn_oids(void)
{
    Oid argtypes[4];

    if (aqp_fn_oid_cached)
        return true;

    aqp_fn_oid_cached = false;
    
    argtypes[0] = INTERNALOID;
    aqp_swr_tsm_handler_oid = aqp_lookup_fn_oid("aqp.swr", 1, argtypes);
    if (aqp_swr_tsm_handler_oid == InvalidOid)
        return false;

    aqp_sample_prob_oid = aqp_lookup_fn_oid("aqp.sample_prob", 0, NULL);
    if (aqp_sample_prob_oid == InvalidOid)
        return false;

    argtypes[0] = FLOAT8OID;
    aqp_sum_float8_oid = aqp_lookup_fn_oid("pg_catalog.sum", 1, argtypes);
    if (aqp_sum_float8_oid == InvalidOid)
        return false;
    
    aqp_float8_mul_opno = aqp_lookup_op_oid("pg_catalog.*",
                                            FLOAT8OID,
                                            FLOAT8OID);
    if (aqp_float8_mul_opno == InvalidOid)
        return false;
    aqp_float8_mul_funcid = get_opcode(aqp_float8_mul_opno);
    if (aqp_float8_mul_funcid == InvalidOid)
        return false;

    aqp_float8_div_opno = aqp_lookup_op_oid("pg_catalog./",
                                            FLOAT8OID,
                                            FLOAT8OID);
    if (aqp_float8_div_opno == InvalidOid)
        return false;
    aqp_float8_div_funcid = get_opcode(aqp_float8_div_opno);
    if (aqp_float8_div_funcid == InvalidOid)
        return false;

    argtypes[0] = FLOAT4OID;
    argtypes[1] = FLOAT8OID;
    aqp_float48_div_opno = aqp_lookup_op_oid("pg_catalog./",
                                             FLOAT4OID,
                                             FLOAT8OID);
    if (aqp_float48_div_opno == InvalidOid)
        return false;
    aqp_float48_div_funcid = get_opcode(aqp_float48_div_opno);
    if (aqp_float48_div_funcid == InvalidOid)
        return false;

    argtypes[0] = FLOAT8OID;
    aqp_erf_inv_oid = aqp_lookup_fn_oid("aqp.erf_inv", 1, argtypes);
    if (aqp_erf_inv_oid == InvalidOid)
        return false;
    
    argtypes[0] = ANYOID;
    aqp_approx_sum_oid = aqp_lookup_fn_oid("aqp.approx_sum", 1, argtypes);
    if (aqp_approx_sum_oid == InvalidOid)
        return false;

    argtypes[0] = ANYOID;
    argtypes[1] = FLOAT8OID;
    aqp_approx_sum_half_ci_oid = aqp_lookup_fn_oid("aqp.approx_sum_half_ci",
                                                   2, argtypes);
    if (aqp_approx_sum_half_ci_oid == InvalidOid)
        return false;
    
    aqp_approx_count_oid = aqp_lookup_fn_oid("aqp.approx_count", 0, argtypes);
    if (aqp_approx_count_oid == InvalidOid)
        return false;
    
    argtypes[0] = FLOAT8OID;
    aqp_approx_count_half_ci_oid =
        aqp_lookup_fn_oid("aqp.approx_count_star_half_ci", 1, argtypes);
    if (aqp_approx_count_half_ci_oid == InvalidOid)
        return false;
    
    argtypes[0] = ANYOID;
    aqp_approx_count_any_oid = aqp_lookup_fn_oid("aqp.approx_count", 1, argtypes);
    if (aqp_approx_count_any_oid == InvalidOid)
        return false;

    argtypes[0] = ANYOID;
    argtypes[1] = FLOAT8OID;
    aqp_approx_count_any_half_ci_oid =
        aqp_lookup_fn_oid("aqp.approx_count_half_ci", 2, argtypes);
    if (aqp_approx_count_any_half_ci_oid == InvalidOid)
        return false;

    argtypes[0] = FLOAT8OID;
    aqp_float8_accum_oid = aqp_lookup_fn_oid("aqp.float8_accum",
                                             1, argtypes);
    if (aqp_float8_accum_oid == InvalidOid)
        return false;

    argtypes[0] = FLOAT8ARRAYOID;
    argtypes[1] = FLOAT8OID;
    argtypes[2] = FLOAT8OID;
    aqp_clt_half_ci_final_func_oid =
        aqp_lookup_fn_oid("aqp.clt_half_ci_finalfunc",
                          3, argtypes);
    if (aqp_clt_half_ci_final_func_oid == InvalidOid)
        return false;

    aqp_fn_oid_cached = true;
    return true;
}

