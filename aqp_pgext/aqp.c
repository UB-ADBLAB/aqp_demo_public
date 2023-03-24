#include "aqp.h"

#include "aqp_planner.h"
#include "aqp_swrscan.h"
#include "aqp_sample_scan_state.h"
#include "aqp_explain.h"

PG_MODULE_MAGIC;

extern void _PG_init(void);

void _PG_init(void)
{
    /*elog(NOTICE, "loading aqp extension"); */
    aqp_setup_swr();
    aqp_setup_path_hook();
    aqp_setup_planner_hook();
    aqp_sample_scan_add_guc_entry();
    aqp_setup_hooks_for_explain();
}

