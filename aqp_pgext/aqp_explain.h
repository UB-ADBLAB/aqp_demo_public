#ifndef AQP_EXPLAIN_H
#define AQP_EXPLAIN_H

#include "aqp.h"

#include <commands/explain.h>

extern void aqp_setup_hooks_for_explain(void);

extern bool aqp_is_in_explain_verbose;

extern void aqp_show_index_sample_scan_details(uint64 sample_size,
                                               Expr *sample_size_expr,
                                               Expr *repeatable_expr,
                                               PlanState *planstate,
                                               List *ancestors,
                                               ExplainState *es);
#endif  /* AQP_EXPLAIN_H */
