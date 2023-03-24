#include "aqp.h"

#include <nodes/makefuncs.h>
#include <utils/ruleutils.h>
#include <tcop/tcopprot.h>

#include "aqp_explain.h"
#include "pg_explain.h"

/*
 * Show a generic expression
 */
void
aqp_pgport_show_expression(Node *node, const char *qlabel,
                           PlanState *planstate, List *ancestors,
                           bool useprefix, ExplainState *es)
{
    List       *context;
    char       *exprstr;

    /* Set up deparsing context */
    context = set_deparse_context_plan(es->deparse_cxt,
                                       planstate->plan,
                                       ancestors);

    /* Deparse the expression */
    exprstr = deparse_expression(node, context, useprefix, false);

    /* And add to es->str */
    ExplainPropertyText(qlabel, exprstr, es);
}

/*
 * Show a qualifier expression (which is a List with implicit AND semantics)
 */
void
aqp_pgport_show_qual(List *qual, const char *qlabel,
                     PlanState *planstate, List *ancestors,
                     bool useprefix, ExplainState *es)
{
    Node       *node;

    /* No work if empty qual */
    if (qual == NIL)
        return;

    /* Convert AND list to explicit AND */
    node = (Node *) make_ands_explicit(qual);

    /* And show it */
    aqp_pgport_show_expression(node, qlabel, planstate, ancestors,
                               useprefix, es);
}


/*
 * Show a qualifier expression for a scan plan node
 */
void
aqp_pgport_show_scan_qual(List *qual, const char *qlabel,
                          PlanState *planstate, List *ancestors,
                          ExplainState *es)
{
    bool        useprefix;

    useprefix = (IsA(planstate->plan, SubqueryScan) || es->verbose);
    aqp_pgport_show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Indent a text-format line.
 *
 * We indent by two spaces per indentation level.  However, when emitting
 * data for a parallel worker there might already be data on the current line
 * (cf. ExplainOpenWorker); in that case, don't indent any more.
 */
void
aqp_pgport_ExplainIndentText(ExplainState *es)
{
	Assert(es->format == EXPLAIN_FORMAT_TEXT);
	if (es->str->len == 0 || es->str->data[es->str->len - 1] == '\n')
		appendStringInfoSpaces(es->str, es->indent * 2);
}

/*
 * ExplainOneQuery -
 *	  print out the execution plan for one Query
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 */
void
aqp_pgport_ExplainOneQuery(Query *query, int cursorOptions,
                           IntoClause *into, ExplainState *es,
                           const char *queryString, ParamListInfo params,
                           QueryEnvironment *queryEnv)
{
    PlannedStmt *plan;
    instr_time	planstart,
                planduration;
    BufferUsage bufusage_start,
                bufusage;

    if (es->buffers)
        bufusage_start = pgBufferUsage;
    INSTR_TIME_SET_CURRENT(planstart);

    /* plan the query */
    plan = pg_plan_query(query, queryString, cursorOptions, params);

    INSTR_TIME_SET_CURRENT(planduration);
    INSTR_TIME_SUBTRACT(planduration, planstart);

    /* calc differences of buffer counters. */
    if (es->buffers)
    {
        memset(&bufusage, 0, sizeof(BufferUsage));
        BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
    }

    /* run it (if needed) and produce output */
    ExplainOnePlan(plan, into, es, queryString, params, queryEnv,
                   &planduration, (es->buffers ? &bufusage : NULL));
}
