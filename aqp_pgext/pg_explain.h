#ifndef PG_EXPLAIN_H
#define PG_EXPLAIN_H

#include <nodes/execnodes.h>
#include <nodes/pg_list.h>
#include <commands/explain.h>

extern void aqp_pgport_show_expression(Node *node, const char *qlabel,
                                       PlanState *planstate, List *ancestors,
                                       bool useprefix, ExplainState *es);
extern void aqp_pgport_show_qual(List *qual, const char *qlabel,
                                 PlanState *planstate, List *ancestors,
                                 bool useprefix, ExplainState *es);
extern void aqp_pgport_show_scan_qual(List *qual, const char *qlabel,
                                      PlanState *planstate, List *ancestors,
                                      ExplainState *es);
extern void aqp_pgport_ExplainIndentText(ExplainState *es);
extern void aqp_pgport_ExplainOneQuery(Query *query, int cursorOptions,
                                       IntoClause *into, ExplainState *es,
                                       const char *queryString,
                                       ParamListInfo params,
                                       QueryEnvironment *queryEnv);

#endif  /* PG_EXPLAIN_H */
