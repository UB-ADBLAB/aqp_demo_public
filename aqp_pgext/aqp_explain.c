#include "aqp.h"

#include <commands/explain.h>
#include <utils/ruleutils.h>
#include <executor/execdesc.h>
#include <executor/executor.h>

#include "aqp_explain.h"
#include "pg_explain.h"
#include "aqp_planner.h"

ExplainOneQuery_hook_type prev_explain_one_query_hook = NULL;
ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;

static void aqp_explain_one_query(Query *query,
                                  int cursorOptions,
                                  IntoClause *into,
                                  ExplainState *es,
                                  const char *queryString,
                                  ParamListInfo params,
                                  QueryEnvironment *queryEnv);
static void aqp_executor_finish_for_explain(QueryDesc *queryDesc);

bool aqp_is_in_explain_verbose = false;

/*
 * To support EXPLAIN VERBOSE on the AQPSWRNode and AQPSWRScanNode, which may
 * contain dummy variables for sample_prob() at these scan nodes and explain
 * will end up with infinite recursion trying to resolve these variables, we
 * have to:
 *
 * 1. Hook into ExplainOneQuery such that we can set up a global flag to
 * indicate to the executor init and finish we're inside an explain.
 *
 * 2. If this is a regular EXPLAIN without ANALYZE, then the dummy variables
 * will be replaced in ExecutorInit.
 *
 * 3. If this is an EXPLAIN ANALYZE, then we have to defer that until
 * ExecutorFinish happens (note: not ExecutorEnd! printing of plan happens
 * before ExecutorEnd). Unlike any other Executor functions, corresponding
 * functions are called recursively on the plan states and/or plan, there is
 * no per-node ExecFinish. So we have to use a global hook and post-process
 * the plan tree after the real ExecutorFinish returns.
 */
void
aqp_setup_hooks_for_explain(void)
{
    prev_explain_one_query_hook = ExplainOneQuery_hook;
    ExplainOneQuery_hook = aqp_explain_one_query;

    prev_ExecutorFinish_hook = ExecutorFinish_hook;
    ExecutorFinish_hook = aqp_executor_finish_for_explain;
}

static void
aqp_explain_one_query(Query *query,
                      int cursorOptions,
                      IntoClause *into,
                      ExplainState *es,
                      const char *queryString,
                      ParamListInfo params,
                      QueryEnvironment *queryEnv)
{
    if (es->verbose)
    {
        /* otherwise we don't have need to rewrite it  */
        aqp_is_in_explain_verbose = true;
    }

    if (prev_explain_one_query_hook)
        prev_explain_one_query_hook(query, cursorOptions, into, es,
                                    queryString, params, queryEnv);
    else
        aqp_pgport_ExplainOneQuery(query, cursorOptions, into, es,
                                   queryString, params, queryEnv);
    
    aqp_is_in_explain_verbose = false; 
}

void
aqp_show_index_sample_scan_details(uint64 sample_size,
                                   Expr *sample_size_expr,
                                   Expr *repeatable_expr,
                                   PlanState *planstate,
                                   List *ancestors,
                                   ExplainState *es)
{
    char *repeatable_text = NULL;
    char *sample_size_text = NULL;

    bool useprefix;
    useprefix = list_length(es->rtable) > 1 || es->verbose;

    if (repeatable_expr)
    {
        List                *context;

        context = set_deparse_context_plan(es->deparse_cxt, planstate->plan,
                                           ancestors);
        repeatable_text= deparse_expression((Node *) repeatable_expr,
                                            context,
                                            useprefix,
                                            false);
    }
    if (sample_size_text)
    {
        List                *context;

        context = set_deparse_context_plan(es->deparse_cxt, planstate->plan,
                                           ancestors);
        sample_size_text = deparse_expression((Node *) sample_size_text,
                                              context,
                                              useprefix,
                                              false);
    }
    else
    {
        sample_size_text = psprintf(UINT64_FORMAT, sample_size);
    }
    
    if (es->format == EXPLAIN_FORMAT_TEXT)
    {
        aqp_pgport_ExplainIndentText(es);
        appendStringInfo(es->str, "Sampling: SWR (%s)", sample_size_text);
        if (repeatable_text)
            appendStringInfo(es->str, " REPEATABLE (%s)", repeatable_text);
        appendStringInfoChar(es->str, '\n');
    }
    else
    {
        ExplainPropertyText("Sampling Method", "SWR", es);
        ExplainPropertyText("Sampling Parameters", sample_size_text, es);
        if (repeatable_text)
            ExplainPropertyText("Repeatable Seed", repeatable_text, es);
    }
}

static void
aqp_executor_finish_for_explain(QueryDesc *queryDesc)
{
    if (prev_ExecutorFinish_hook)
        prev_ExecutorFinish_hook(queryDesc);
    else
        standard_ExecutorFinish(queryDesc);

    if (!(queryDesc->estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY) &&
        aqp_is_in_explain_verbose &&
        queryDesc->operation == CMD_SELECT)
    {
        Assert(queryDesc->plannedstmt != NULL);
        aqp_rewrite_sample_prob_dummy_var_for_explain(queryDesc->planstate);
    }
}

