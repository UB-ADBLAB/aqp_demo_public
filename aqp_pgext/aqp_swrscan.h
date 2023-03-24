#ifndef AQP_SWRSCAN_H
#define AQP_SWRSCAN_H

#include <nodes/extensible.h>
#include <nodes/pathnodes.h>
#include <nodes/execnodes.h>
#include <nodes/pg_list.h>
#include <utils/sampling.h>

#include "aqp_sample_scan_state.h"

typedef struct AQPSWRPath {
    CustomPath      cpath;

    IndexOptInfo    *indexinfo;
    List            *indexclauses;
    bool            index_only_scan;

    uint64          sample_size;
    Expr            *repeatable_expr;
    Expr            *sample_size_expr;

    Cost            indextotalcost;
    Selectivity     indexselectivity;
} AQPSWRPath;

typedef struct AQPSWRScanPrivate {
    ExtensibleNode  extnode; 
    Oid             indexid;        /* OID of index to scan */
    List            *indexqual;     /* list of index quals (usually OpExprs) */
    List            *indexqualorig; /* the same in original form */
    uint64          sample_size;    /*
                                     * Number of samples to fetch if this is a
                                     * index sample scan and it is a known
                                     * constant.
                                     */
    Expr            *repeatable_expr; /* 
                                       * The repeatable expression for
                                       * tablesample.
                                       */
    Expr            *sample_size_expr; /*
                                        * Non-null if the sample size is not
                                        * constant.
                                        */
} AQPSWRScanPrivate;

typedef struct AQPSWRScanState {
    CustomScanState         css;

    /* execution state for indexqual expressions */
	ExprState               *indexqualorig;

    /* scan key structure for index quals */
	struct ScanKeyData      *scan_keys;

    /* number of scan keys */
	int			            num_scan_keys;

    /* info about runtime scan keys */
	IndexRuntimeKeyInfo     *runtime_keys;

    /* number of runtime scan keys */
	int			            num_runtime_keys;

    /* whether the runtime scan keys have been computed */
	bool		            runtime_keys_ready;

    /* the context for evaluting the runtime scan keys */
	ExprContext             *runtime_context;

    /* index relation descriptor */
	Relation	            indexrel;

    /* index scan descriptor  */
	struct IndexScanDescData *scandesc;

    /* want probability */
    bool                    want_prob;
    
    /* the sample scan state */
	AQPSampleScanState      sss;
} AQPSWRScanState;

#define AQPSWRScanPrivateName "AQPSWRScanPrivate"
#define AQPSWRScanName "AQPSWRScan"
#define AQPSWRScanStateName "AQPSWRScanState"
extern CustomScanMethods aqp_swrscan_methods;
extern CustomExecMethods aqp_swrscan_exec_methods;

typedef struct AQPSWRIndexOnlyScanPrivate {
    ExtensibleNode  extnode; 
    Oid             indexid;     /* OID of index to scan */
    List            *indexqual;  /* list of index quals (usually OpExprs) */
    uint64          sample_size; /*
                                  * Number of samples to fetch if this is a
                                  * index sample scan and it is a known
                                  * constant.
                                  */
    Expr            *repeatable_expr; /* 
                                       * The repeatable expression for
                                       * tablesample.
                                       */
    Expr            *sample_size_expr; /*
                                        * Non-null if the sample size is not
                                        * constant.
                                        */
} AQPSWRIndexOnlyScanPrivate;

typedef struct AQPSWRIndexOnlyScanState {
    CustomScanState         css;
    
    /* execution state for indexqual expressions */
	ExprState               *indexqual;

    /* scan key structure for index quals */
	struct ScanKeyData      *scan_keys;

    /* number of scan keys */
	int			            num_scan_keys;

    /* info about runtime scan keys */
	IndexRuntimeKeyInfo     *runtime_keys;

    /* number of runtime scan keys */
	int			            num_runtime_keys;

    /* whether the runtime scan keys have been computed */
	bool		            runtime_keys_ready;

    /* the context for evaluting the runtime scan keys */
	ExprContext             *runtime_context;

    /* index relation descriptor */
	Relation	            indexrel;

    /* index scan descriptor  */
	struct IndexScanDescData *scandesc;

    /* table slot for heap tuple fetch in case of need for visibility check */
	TupleTableSlot          *table_slot;
    
    /* buffer for visibility checks */
	Buffer		            vmbuffer;
    
    /* want probability */
    bool                    want_prob;
    
    /* the sample scan state */
	AQPSampleScanState      sss;
} AQPSWRIndexOnlyScanState;

#define AQPSWRIndexOnlyScanPrivateName "AQPSWRIndexOnlyScanPrivate"
#define AQPSWRIndexOnlyScanName "AQPSWRIndexOnlyScan"
#define AQPSWRIndexOnlyScanStateName "AQPSWRIndexOnlyScanState"
extern CustomScanMethods aqp_swrindexonlyscan_methods;
extern CustomExecMethods aqp_swrindexonlyscan_exec_methods;

extern Plan* aqp_plan_swr_path(PlannerInfo *root,
                               RelOptInfo *rel,
                               CustomPath *best_path,
                               List *tlist,
                               List *clauses,
                               List *custom_plans);
extern void aqp_fix_swrscan_exprs(Plan *stmt);
extern List* aqp_reparameterize_custom_path_by_child(PlannerInfo *root,
                                                     List *custom_private,
                                                     RelOptInfo *child_rel);

extern Node *aqp_swrscan_create_scan_state(CustomScan *cscan);
extern Node *aqp_swrindexonlyscan_create_scan_state(CustomScan *cscan);

static inline bool
aqp_plan_is_aqp_swrscan(Plan *plan)
{
    return plan != NULL && IsA(plan, CustomScan) &&
            ((CustomScan *) plan)->methods == &aqp_swrscan_methods;
}

static inline bool
aqp_plan_is_aqp_swrindexonlyscan(Plan *plan)
{
    return plan != NULL && IsA(plan, CustomScan) &&
        ((CustomScan *) plan)->methods == &aqp_swrindexonlyscan_methods;
}

/* 
 * These attribute numbers only appear in query plans and should never be
 * evaluated by projection expressions.
 */
#define AQPSampleProbAttributeNumber        (-10)
#define AQPInvSampleProbAttributeNumber     (-11)

#endif  /* AQP_SWRSCAN_H */
