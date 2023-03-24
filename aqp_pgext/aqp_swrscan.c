#include "aqp.h"

#include <miscadmin.h>
#include <access/abtree.h>
#include <access/genam.h>
#include <access/tableam.h>
#include <access/visibilitymap.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <executor/nodeIndexscan.h>
#include <nodes/readfuncs.h>
#include <storage/itemptr.h>
#include <utils/builtins.h>
#include <utils/epoch.h>
#include <utils/lsyscache.h>

#include "pg_explain.h"
#include "aqp_planner.h"
#include "aqp_swrscan.h"
#include "aqp_explain.h"

static void AQPSWRScanPrivate_copy(ExtensibleNode *newnode,
                                   const ExtensibleNode *oldnode);
static bool AQPSWRScanPrivate_equal(const ExtensibleNode *a,
                                    const ExtensibleNode *b);
static void AQPSWRScanPrivate_out(StringInfo str,
                                  const ExtensibleNode *node);
static void AQPSWRScanPrivate_read(ExtensibleNode *node);
static void AQPSWRIndexOnlyScanPrivate_copy(ExtensibleNode *newnode,
                                            const ExtensibleNode *oldnode);
static bool AQPSWRIndexOnlyScanPrivate_equal(const ExtensibleNode *a,
                                            const ExtensibleNode *b);
static void AQPSWRIndexOnlyScanPrivate_out(StringInfo str,
                                           const ExtensibleNode *node);
static void AQPSWRIndexOnlyScanPrivate_read(ExtensibleNode *node);
static void aqp_swr_scan_begin(CustomScanState *node,
                               EState *estate,
                               int eflags);
static TupleTableSlot* aqp_swr_scan_exec(PlanState *node);
static void aqp_swr_scan_end(CustomScanState *node);
static void aqp_swr_scan_rescan(CustomScanState *node);
static void aqp_swr_scan_explain(CustomScanState *node,
                                 List *ancestors,
                                 ExplainState *es);
static void aqp_swr_index_only_scan_begin(CustomScanState *node,
                                          EState *estate,
                                          int eflags);
static TupleTableSlot* aqp_swr_index_only_scan_exec(PlanState *node);
static void aqp_swr_index_only_scan_end(CustomScanState *node);
static void aqp_swr_index_only_scan_rescan(CustomScanState *node);
static void aqp_swr_index_only_scan_explain(CustomScanState *node,
                                            List *ancestors,
                                            ExplainState *es);

static ExtensibleNodeMethods aqp_swr_scan_private_methods = {
    AQPSWRScanPrivateName,
    sizeof(AQPSWRScanPrivate),
    AQPSWRScanPrivate_copy,
    AQPSWRScanPrivate_equal,
    AQPSWRScanPrivate_out,
    AQPSWRScanPrivate_read
};

CustomScanMethods aqp_swrscan_methods = {
    AQPSWRScanName,
    aqp_swrscan_create_scan_state
};

CustomExecMethods aqp_swrscan_exec_methods = {
    /*CustomName=*/AQPSWRScanStateName,
    /*BeginCustomScan=*/aqp_swr_scan_begin,
    /*ExecCustomScan=*/
        (TupleTableSlot*(*)(CustomScanState*)) aqp_swr_scan_exec,
    /*EndCustomScan=*/aqp_swr_scan_end,
    /*ReScanCustomScan=*/aqp_swr_scan_rescan,
    /*MarkPosCustomScan=*/NULL,
    /*RestrPosCustomScan=*/NULL,
    /*EstimateDSMCustomScan=*/NULL,
    /*InitializeDSMCustomScan=*/NULL,
    /*ReInitializeDSMCustomScan=*/NULL,
    /*InitializeWorkerCustomScan=*/NULL,
    /*ShutdownCustomScan=*/NULL,
    /*ExplainCustomScan=*/aqp_swr_scan_explain
};

static ExtensibleNodeMethods aqp_swr_index_only_scan_private_methods = {
    AQPSWRIndexOnlyScanPrivateName,
    sizeof(AQPSWRIndexOnlyScanPrivate),
    AQPSWRIndexOnlyScanPrivate_copy,
    AQPSWRIndexOnlyScanPrivate_equal,
    AQPSWRIndexOnlyScanPrivate_out,
    AQPSWRIndexOnlyScanPrivate_read
};

CustomScanMethods aqp_swrindexonlyscan_methods = {
    AQPSWRIndexOnlyScanName,
    aqp_swrindexonlyscan_create_scan_state
};

CustomExecMethods aqp_swrindexonlyscan_exec_methods = {
    /*CustomName=*/AQPSWRIndexOnlyScanStateName,
    /*BeginCustomScan=*/aqp_swr_index_only_scan_begin,
    /*ExecCustomScan=*/
        (TupleTableSlot*(*)(CustomScanState*)) aqp_swr_index_only_scan_exec,
    /*EndCustomScan=*/aqp_swr_index_only_scan_end,
    /*ReScanCustomScan=*/aqp_swr_index_only_scan_rescan,
    /*MarkPosCustomScan=*/NULL,
    /*RestrPosCustomScan=*/NULL,
    /*EstimateDSMCustomScan=*/NULL,
    /*InitializeDSMCustomScan=*/NULL,
    /*ReInitializeDSMCustomScan=*/NULL,
    /*InitializeWorkerCustomScan=*/NULL,
    /*ShutdownCustomScan=*/NULL,
    /*ExplainCustomScan=*/aqp_swr_index_only_scan_explain
};

static void 
AQPSWRScanPrivate_copy(ExtensibleNode *newnode_,
                       const ExtensibleNode *from_)
{
    AQPSWRScanPrivate   *newnode = (AQPSWRScanPrivate *) newnode_;
    AQPSWRScanPrivate   *from = (AQPSWRScanPrivate *) from_;
    
    newnode->indexid = from->indexid;
    newnode->indexqual = copyObjectImpl(from->indexqual);
    newnode->indexqualorig = copyObjectImpl(from->indexqualorig);
    newnode->sample_size = from->sample_size;
    newnode->repeatable_expr = copyObjectImpl(from->repeatable_expr);
    newnode->sample_size_expr = copyObjectImpl(from->sample_size_expr);
}

static bool
AQPSWRScanPrivate_equal(const ExtensibleNode *a,
                        const ExtensibleNode *b)
{
    elog(ERROR, "plan tree equal is not implemented");
    return false;
}

static void
AQPSWRScanPrivate_out(StringInfo str,
                      const ExtensibleNode *node_)
{
    AQPSWRScanPrivate   *node = (AQPSWRScanPrivate *) node_;
    
    appendStringInfo(str, " :indexid %u", node->indexid);
    appendStringInfoString(str, " :indexqual ");
    outNode(str, node->indexqual);
    appendStringInfoString(str, " :indexqualorig ");
    outNode(str, node->indexqualorig);
    appendStringInfo(str, " :sample_size " UINT64_FORMAT, node->sample_size);
    appendStringInfoString(str, " :repeatable_expr ");
    outNode(str, node->repeatable_expr);
    appendStringInfoString(str, " :sample_size_expr ");
    outNode(str, node->sample_size_expr);
}

static void
AQPSWRScanPrivate_read(ExtensibleNode *node_)
{
    AQPSWRScanPrivate   *local_node = (AQPSWRScanPrivate *) node_;
    const char *token;
    int length;
    
    token = pg_strtok(&length);
    token = pg_strtok(&length);
    local_node->indexid = atooid(token);

    token = pg_strtok(&length);
    local_node->indexqual = nodeRead(NULL, 0);

    token = pg_strtok(&length);
    local_node->indexqualorig = nodeRead(NULL, 0);

    token = pg_strtok(&length);
    token = pg_strtok(&length);
    local_node->sample_size = pg_strtouint64(token, NULL, 10);

    token = pg_strtok(&length);
    local_node->repeatable_expr = nodeRead(NULL, 0);

    token = pg_strtok(&length);
    local_node->sample_size_expr = nodeRead(NULL, 0);
}

static void 
AQPSWRIndexOnlyScanPrivate_copy(ExtensibleNode *newnode_,
                                const ExtensibleNode *from_)
{
    AQPSWRIndexOnlyScanPrivate *newnode = (AQPSWRIndexOnlyScanPrivate*) newnode_;
    AQPSWRIndexOnlyScanPrivate *from = (AQPSWRIndexOnlyScanPrivate*) from_;

    newnode->indexid = from->indexid;
    newnode->indexqual = copyObjectImpl(from->indexqual);
    newnode->sample_size = from->sample_size;
    newnode->repeatable_expr = copyObjectImpl(from->repeatable_expr);
    newnode->sample_size_expr = copyObjectImpl(from->sample_size_expr);
}

static bool
AQPSWRIndexOnlyScanPrivate_equal(const ExtensibleNode *a,
                                 const ExtensibleNode *b)
{
    elog(ERROR, "plan tree equal is not implemented");
    return false;
}

static void
AQPSWRIndexOnlyScanPrivate_out(StringInfo str,
                               const ExtensibleNode *node_)
{
    AQPSWRIndexOnlyScanPrivate *node = (AQPSWRIndexOnlyScanPrivate*) node_;

    appendStringInfo(str, " :indexid %u", node->indexid);
    appendStringInfoString(str, " :indexqual ");
    outNode(str, node->indexqual);
    appendStringInfo(str, " :sample_size " UINT64_FORMAT, node->sample_size);
    appendStringInfoString(str, " :repeatable_expr ");
    outNode(str, node->repeatable_expr);
    appendStringInfoString(str, " :sample_size_expr ");
    outNode(str, node->sample_size_expr);
}

static void
AQPSWRIndexOnlyScanPrivate_read(ExtensibleNode *node_)
{
    AQPSWRIndexOnlyScanPrivate *local_node = (AQPSWRIndexOnlyScanPrivate*) node_;
    const char *token;
    int length;
    
    token = pg_strtok(&length);
    token = pg_strtok(&length);
    local_node->indexid = atooid(token);

    token = pg_strtok(&length);
    local_node->indexqual = nodeRead(NULL, 0);

    token = pg_strtok(&length);
    token = pg_strtok(&length);
    local_node->sample_size = pg_strtouint64(token, NULL, 10);

    token = pg_strtok(&length);
    local_node->repeatable_expr = nodeRead(NULL, 0);

    token = pg_strtok(&length);
    local_node->sample_size_expr = nodeRead(NULL, 0);
}

Node *
aqp_swrscan_create_scan_state(CustomScan *cscan)
{
    AQPSWRScanState *state;

    state = palloc0(sizeof(AQPSWRScanState));
    state->css.ss.ps.type = T_CustomScanState;
    /*
     * ps.plan, ps.state, ps.ExecProcNode, css.flags will be set up by
     * ExecInitCustomScan() 
     */
    state->css.custom_ps = NIL;
    state->css.pscan_len = 0;
    state->css.methods = &aqp_swrscan_exec_methods;

    return (Node *) state;
}

static void
aqp_swr_scan_begin(CustomScanState *node,
                   EState *estate,
                   int eflags)
{
    LOCKMODE lockmode;
    AQPSWRScanState *state = (AQPSWRScanState *) node;
    CustomScan *scan = castNode(CustomScan, state->css.ss.ps.plan);
    AQPSWRScanPrivate *private =
        (AQPSWRScanPrivate *) linitial(scan->custom_private);
    Relation baserel;
    List    *targetlist_with_no_prob = NULL;
    int nredundant_tupleslot;
    int i;

    /*
     * At this point, ExecInitCustomScan() has:
     * 1) assigned the expression context for this node;
     * 2) opened the base relation (state->css.ss.ss_CurrentRelation);
     * 3) initialized the scan tuple slot the type derived from the heap
     * relation and using the TTSOpsVirtual (which is not what we want!).
     * Because the ttsops is fixed at this point, we have to discard everything
     * starting from 3) until 5)....
     * 4) initialized the result tuple slot as a virtual tuple with the heap
     * relation relid as the expected varno in the tlist;
     * 5) initialized the plan qual (the additional plan qual not covered
     * by the index).
     */
    
    /* 
     * NOTE(zy) similar to swr_index_only_scan, we want to save a few cycles
     * of the indirect call from ExecCustomScan
     */
    state->css.ss.ps.ExecProcNode = aqp_swr_scan_exec;
    
    /* 
     * Unlike index only scan, where the scan tuple slot is ok, we need to
     * replace the scan slot with heap tuple TTSOps.
     */
    Assert(state->css.ss.ss_ScanTupleSlot);
    
    /* 
     * Unfortunately, there's not too much we can do with this ExprState
     * created in ExecInitCustomScan(). Hopefully it's not wasting too much
     * memory.
     */
    state->css.ss.ps.qual = NULL;

    /*
     * The same goes with the expression inside the projection info.
     * However, let's at least free the proj info.
     */
    if (state->css.ss.ps.ps_ProjInfo)
    {
        pfree(state->css.ss.ps.ps_ProjInfo);
        state->css.ss.ps.ps_ProjInfo = NULL;
    }

    /* 
     * We can also remove the extra tuple slot from the tuple slot table. They
     * should be at the end of estate->es_tupleTable.
     */
    if (state->css.ss.ps.ps_ResultTupleSlot)
        nredundant_tupleslot = 2;
    else
        nredundant_tupleslot = 1;
    Assert(list_length(estate->es_tupleTable) >= nredundant_tupleslot);
    for (i = 0; i < nredundant_tupleslot; ++i)
    {
        TupleTableSlot *slot = (TupleTableSlot *) llast(estate->es_tupleTable);
        if (slot == state->css.ss.ps.ps_ResultTupleSlot ||
            slot == state->css.ss.ss_ScanTupleSlot)
        {
            estate->es_tupleTable = list_delete_last(estate->es_tupleTable);
            if (slot->tts_tupleDescriptor)
            {
               ReleaseTupleDesc(slot->tts_tupleDescriptor);
               slot->tts_tupleDescriptor = NULL;
            }
            if (!TTS_FIXED(slot))
            {
               if (slot->tts_values)
                   pfree(slot->tts_values);
               if (slot->tts_isnull)
                   pfree(slot->tts_isnull);
            }
            pfree(slot);
        }
        else
        {
            ereport(ERROR,
                    errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("AQPSWRScan failed to find the redundant tuple "
                           "slots made by ExecInitCustomScan()"));
        }
    }
    state->css.ss.ps.ps_ResultTupleSlot = NULL;
    state->css.ss.ss_ScanTupleSlot = NULL;

    /* 
     * OK, we're ready to do our own init. The following is similar to
     * ExecInitIndexScan().
     */
    baserel = state->css.ss.ss_currentRelation;
    state->css.ss.ss_currentScanDesc = NULL; /* no heap scan here */
    
    /*
     * Find out whether we want to include the probability column in the scan
     * tuple slot.
     *
     * NOTE(zy) We assume the plan rewriter always appends an extra entry in the
     * target list where varno == scan->scan.scanrelid and varattno == natts +
     * 1 where natts is the number of columns in the scan table, if it wants
     * to include the probability column in the scan tuple slot.
     */
    state->want_prob = false;
    if (list_length(scan->scan.plan.targetlist) != 0)
    {
        TargetEntry *tle = llast_node(TargetEntry, scan->scan.plan.targetlist);
        if (IsA(tle->expr, Var))
        {
            Var *var = (Var *) tle->expr;
            if (var->varno == scan->scan.scanrelid)
            {
                TupleDesc desc = RelationGetDescr(baserel);
                if (var->varattno == desc->natts + 1)
                {
                    /* ok, this is the probability column */
                    state->want_prob = true;

                    /* 
                     * A shorter targetlist without the probability column that
                     * will be used for initializing the projection info.
                     */
                    targetlist_with_no_prob =
                        list_copy(scan->scan.plan.targetlist);
                    targetlist_with_no_prob =
                        list_delete_last(targetlist_with_no_prob);
                }
            }
        }
    }
    
    /*
     * Initialize scan slot (and scan type).
     */
    ExecInitScanTupleSlot(estate, &state->css.ss,
                          RelationGetDescr(baserel),
                          table_slot_callbacks(baserel));

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&state->css.ss.ps);
    if (state->want_prob)
    {
        /* 
         * Has one extra prob column. We need to use the shorter targetlist
         * to initialize the projection info so that it won't try to fetch
         * an non-existent column.
         *
         * Here, we must assign a projection info because the true target
         * list never matches the scan rel tuple descriptor.
         */
        Assert(!state->css.ss.ps.ps_ResultTupleSlot);
        ExecInitResultSlot(&state->css.ss.ps, &TTSOpsVirtual);
        state->css.ss.ps.resultops = &TTSOpsVirtual;
        state->css.ss.ps.resultopsfixed = true;
        state->css.ss.ps.resultopsset = true;

        state->css.ss.ps.ps_ProjInfo =
            ExecBuildProjectionInfo(targetlist_with_no_prob,
                                    state->css.ss.ps.ps_ExprContext,
                                    state->css.ss.ps.ps_ResultTupleSlot,
                                    &state->css.ss.ps,
                                    state->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor);
    }
    else
    {
        /* No prob column. We can initialize the projection info as is. */
        ExecAssignScanProjectionInfo(&state->css.ss);
    }

    /*
     * Initialize child expressions.
     */
    state->css.ss.ps.qual =
        ExecInitQual(scan->scan.plan.qual, &state->css.ss.ps);
    state->indexqualorig =
        ExecInitQual(private->indexqualorig, &state->css.ss.ps);

    /* 
     * EXPLAIN stops here without opening the index.
     * See nodeIndexscan.c for rationale.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
    {
        /* 
         * Explain only, we need to replace the Vars with invalid
         * references into the heap tuple with sample_prob() in the
         * query plan right now, before it is sent for printing.
         */
        if (aqp_is_in_explain_verbose)
            aqp_rewrite_sample_prob_dummy_var_for_explain(&state->css.ss.ps);
        return ;
    }

    if (!IsMVCCSnapshot(estate->es_snapshot))
    {
        /*
         * It's not ok to use non-MVCC snapshot for index sample scan
         * because, otherwise, there might be multiple valid tuples in
         * one HOT chain matching one TID. That breaks the assumption that
         * one TID sampled TID from the index produces exactly one tuple.
         *
         * Do this check as early as possible to prevent wasting efforts
         * in all the initialization. However, we don't really care if this
         * is an EXPLAIN command.
         */
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("TABLESAMPLE SWR() must be used under MVCC "
                        "snapshot")));
    }

    /* open the index */
    lockmode = exec_rt_fetch(scan->scan.scanrelid, estate)->rellockmode;
    state->indexrel = index_open(private->indexid, lockmode);

    /* index-specific scan state */
    state->runtime_keys_ready = false;
    state->num_runtime_keys = 0;
    state->runtime_keys = NULL;

    /* 
     * index scan keys for the indexqual
     *
     * XXX(zy) We don't allow array keys currently. Can we? 
     */
    ExecIndexBuildScanKeys((PlanState *) state,
                           state->indexrel,
                           private->indexqual,
                           /*isorderby=*/false,
                           &state->scan_keys,
                           &state->num_scan_keys,
                           &state->runtime_keys,
                           &state->num_runtime_keys,
                           /*arrayKeys=*/NULL,
                           /*numArrayKeys=*/NULL);

    if (state->num_runtime_keys > 0)
    {
        state->runtime_context = CreateExprContext(estate);
    }
    else
    {
        state->runtime_context = NULL;
    }

    /* initialize the sample scan states */
    aqp_sample_scan_init(&state->sss,
                         (PlanState*) state,
                         private->repeatable_expr,
                         private->sample_size,
                         private->sample_size_expr);
    aqp_sample_scan_start(&state->sss,
                          state->css.ss.ps.ps_ExprContext);

    /* create the scan desc */ 
    state->scandesc = index_beginscan(state->css.ss.ss_currentRelation,
                                      state->indexrel,
                                      state->css.ss.ps.state->es_snapshot,
                                      state->num_scan_keys,
                                      /*norderbys=*/0);

    /* no runtime keys. We can pass the scan keys to AM at this time */
    if (state->num_runtime_keys == 0)
    {
        index_rescan(state->scandesc,
                     state->scan_keys,
                     state->num_scan_keys,
                     /*orderbys=*/NULL,
                     /*norderbys=*/0);
    }
}

static TupleTableSlot*
aqp_swr_scan_exec(PlanState *node)
{
    AQPSWRScanState *state = (AQPSWRScanState *) node;
    ExprState       *qual;
    ProjectionInfo  *projInfo;
    ExprContext     *econtext;
    IndexScanDesc   scandesc;
    TupleTableSlot  *slot;

    if (aqp_sample_scan_no_more_samples(&state->sss))
    {
        /* we've got enough samples */
        return NULL;
    }

    qual = state->css.ss.ps.qual;
    projInfo = state->css.ss.ps.ps_ProjInfo;
    econtext = state->css.ss.ps.ps_ExprContext;
    scandesc = state->scandesc;
    slot = state->css.ss.ss_ScanTupleSlot;

    /* first call: set up runtime keys */
    if (state->num_runtime_keys > 0 && !state->runtime_keys_ready)
    {
        aqp_sample_scan_mark_rescan_for_runtime_keys_only(&state->sss);
        aqp_swr_index_only_scan_rescan(&state->css);
    }

#define REJECT_TUPLE() \
    if (state->want_prob) \
    { \
        ExecStoreAllNullTuple(slot); \
        aqp_sample_scan_got_new_sample(&state->sss); \
        if (projInfo) { \
            TupleTableSlot *result_slot; \
            econtext->ecxt_scantuple = slot; \
            result_slot = ExecProject(projInfo); \
            result_slot->tts_isnull[result_slot->tts_nvalid - 1] = true; \
            return result_slot; \
        } \
        return slot; \
    } \
    continue;

    for (;;)
    {
        double random_number;
        ItemPointer tid;

        ResetExprContext(econtext);
    
        /* 
         * We now do a separate call to index_samplenext_tid here
         * because we may have to fetch the heap tuple into a separate
         * tuple slot when the caller asks for the sampling probability.
         * In that case, we will have to deform the tuple into a virtual tuple
         * so that we can add the extra column to the scan tuple.
         */
        random_number = aqp_sample_scan_next_random_number(&state->sss);
        tid = index_samplenext_tid(scandesc, random_number);

        /* warn the user if the rejection rate is too high */
        aqp_sample_scan_check_for_high_rejection_rate(&state->sss);

        epoch_maybe_refresh();

        CHECK_FOR_INTERRUPTS();

        if (tid == NULL)
        {
            /* AB-tree rejection */
            REJECT_TUPLE();
        }

        /* now fetch the heap tuple */
        if (!index_fetch_heap(scandesc, slot))
        {
            /* not visible */
            REJECT_TUPLE();
        }
        
        Assert(!scandesc->xs_recheck);
        
        /* now evaluate the plan qual and projection if any */
        econtext->ecxt_scantuple = slot;
        if (qual == NULL || ExecQual(qual, econtext))
        {
            /* passes qual or no qual */
            aqp_sample_scan_got_new_sample(&state->sss);

            if (state->want_prob)
            {
                TupleTableSlot *result_slot;
                double inv_prob = ((ABTScanOpaque) scandesc->opaque)->inv_prob;

                econtext->ecxt_scantuple = slot;
                result_slot = ExecProject(projInfo);
                result_slot->tts_isnull[result_slot->tts_nvalid - 1] = false;
                result_slot->tts_values[result_slot->tts_nvalid - 1] =
                    Float8GetDatum(1.0 / inv_prob);
                return result_slot;
            }
            else
            {
                if (projInfo)
                {
                    return ExecProject(projInfo);
                }
                else
                {
                    return slot;
                }
            }
        }
        else
        {
            REJECT_TUPLE();
        }
    }


#undef REJECT_TUPLE

    elog(ERROR, "unreachable");
    return NULL;
}

static void
aqp_swr_scan_end(CustomScanState *node)
{
    AQPSWRScanState *state = (AQPSWRScanState *) node;
    
    /* 
     * NOTE(zy) we have to set result tuple slot to something with
     * a valid ttsops, or ExecEndCustomScan() will blindly invoke
     * ExecClearTuple on a NULL pointer!
     *
     * We don't need to invoke clear either of the scan or the result tuple
     * though. It will be handled by ExecEndCustomScan.
     */
    if (!state->css.ss.ps.ps_ResultTupleSlot)
        state->css.ss.ps.ps_ResultTupleSlot =
            state->css.ss.ss_ScanTupleSlot;

    if (state->scandesc)
        index_endscan(state->scandesc);
    if (state->indexrel)
        index_close(state->indexrel, NoLock);
}

static void
aqp_swr_scan_rescan(CustomScanState *node)
{
    AQPSWRScanState *state = (AQPSWRScanState *) node;
    
    /* recompute the runtime keys on each rescan if there's any */
    if (state->num_runtime_keys > 0)
    {
        ExprContext *econtext = state->runtime_context;
        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(econtext,
                                 state->runtime_keys,
                                 state->num_runtime_keys);
    }
    state->runtime_keys_ready = true;

    if (state->scandesc)
    {
        index_rescan(state->scandesc,
                     state->scan_keys,
                     state->num_scan_keys,
                     /*orderbys=*/NULL,
                     /*norderbys=*/0);
    }

    aqp_sample_scan_start(&state->sss, state->css.ss.ps.ps_ExprContext);
}

static void
aqp_swr_scan_explain(CustomScanState *node,
                     List *ancestors,
                     ExplainState *es)
{
    CustomScan *cscan = (CustomScan*) node->ss.ps.plan;
    AQPSWRScanPrivate *private =
        (AQPSWRScanPrivate *) linitial(cscan->custom_private);
    const char *index_name;

    index_name = get_rel_name(private->indexid);
    if (!index_name)
        elog(ERROR, "cache lookup failed for index %u", private->indexid);
    ExplainPropertyText("Index Name", index_name, es);

    aqp_pgport_show_scan_qual(private->indexqualorig, "Index Cond",
                              &node->ss.ps, ancestors, es);
    aqp_show_index_sample_scan_details(private->sample_size,
                                       private->sample_size_expr,
                                       private->repeatable_expr,
                                       &node->ss.ps,
                                       ancestors,
                                       es);
}

Node *
aqp_swrindexonlyscan_create_scan_state(CustomScan *cscan)
{
    AQPSWRIndexOnlyScanState *state;
    
    state = palloc0(sizeof(AQPSWRIndexOnlyScanState));
    state->css.ss.ps.type = T_CustomScanState;
    /*
     * ps.plan, ps.state, ps.ExecProcNode, css.flags will be set up by
     * ExecInitCustomScan() 
     */
    state->css.custom_ps = NIL;
    state->css.pscan_len = 0;
    state->css.methods = &aqp_swrindexonlyscan_exec_methods;
    
    return (Node*) state;
}

static void
aqp_swr_index_only_scan_begin(CustomScanState *node,
                              EState *estate,
                              int eflags)
{
    LOCKMODE lockmode;
    AQPSWRIndexOnlyScanState *state = (AQPSWRIndexOnlyScanState *) node;
    CustomScan *scan = castNode(CustomScan, state->css.ss.ps.plan);
    AQPSWRIndexOnlyScanPrivate *private =
        (AQPSWRIndexOnlyScanPrivate *) linitial(scan->custom_private);
    int scan_tlist_len;
    int itupdesc_natts;

    /*
     * At this point, ExecInitCustomScan() has:
     * 1) assigned the expression context for this node;
     * 2) opened the base relation (state->css.ss.ss_CurrentRelation);
     * 3) initialized the scan tuple slot with type derived from
     * `scan->custom_scan_tlist`;
     * 4) initialized the result tuple slot as a virtual tuple with INDEX_VAR
     * as the expected varno in the tlist;
     * 5) initialized the plan qual (the additional plan qual not covered
     * by the index).
     */
    
    /* 
     * NOTE(zy) maybe save a few cycles of the indirect call from
     * ExecCustomScan
     */
    state->css.ss.ps.ExecProcNode = aqp_swr_index_only_scan_exec;
    
    /* no heap scan; just to be extra cautious in case this is set */
    state->css.ss.ss_currentScanDesc = NULL;
    
    /* another table slot for visibility check */
    state->table_slot = 
        ExecAllocTableSlot(&estate->es_tupleTable,
                           RelationGetDescr(state->css.ss.ss_currentRelation),
                           table_slot_callbacks(state->css.ss.ss_currentRelation));

    /* need to initialize indexqual */
    state->indexqual =
        ExecInitQual(private->indexqual, (PlanState *) state);

    /* 
     * NOTE(zy) we may have an extra result tuple slot if the projection info
     * is NULL because of matching indextlist and target list.
     */

    /* 
     * EXPLAIN stops here without opening the index.
     * See nodeIndexonlyscan.c for rationale.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
    {
        /* 
         * Explain only, we need to replace the Vars with invalid
         * references into the heap tuple with sample_prob() in the
         * query plan right now, before it is sent for printing.
         */
        if (aqp_is_in_explain_verbose)
            aqp_rewrite_sample_prob_dummy_var_for_explain((PlanState *) state);
        return ;
    }

    if (!IsMVCCSnapshot(estate->es_snapshot))
    {
        /*
         * It's not ok to use non-MVCC snapshot for index sample scan
         * because, otherwise, there might be multiple valid tuples in
         * one HOT chain matching one TID. That breaks the assumption that
         * one TID sampled TID from the index produces exactly one tuple.
         *
         * Do this check as early as possible to prevent wasting efforts
         * in all the initialization. However, we don't really care if this
         * is an EXPLAIN command.
         */
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("TABLESAMPLE SWR() must be used under MVCC "
                        "snapshot")));
    }
    
    /* open the index */
    lockmode = exec_rt_fetch(scan->scan.scanrelid, estate)->rellockmode;
    state->indexrel = index_open(private->indexid, lockmode);
    
    /* index-specific scan state */
    state->runtime_keys_ready = false;
    state->num_runtime_keys = 0;
    state->runtime_keys = NULL;
    
    /* 
     * index scan keys for the indexqual
     *
     * XXX(zy) We don't allow array keys currently. Can we? 
     */
    ExecIndexBuildScanKeys((PlanState *) state,
                           state->indexrel,
                           private->indexqual,
                           /*isorderby=*/false,
                           &state->scan_keys,
                           &state->num_scan_keys,
                           &state->runtime_keys,
                           &state->num_runtime_keys,
                           /*arrayKeys=*/NULL,
                           /*numArrayKeys=*/NULL);
    
    if (state->num_runtime_keys > 0)
    {
        state->runtime_context = CreateExprContext(estate);
    }
    else
    {
        state->runtime_context = NULL;
    }

    state->vmbuffer = InvalidBuffer;
    
    /* initialize the sample scan states */
    aqp_sample_scan_init(&state->sss,
                         (PlanState*) state,
                         private->repeatable_expr,
                         private->sample_size,
                         private->sample_size_expr);
    aqp_sample_scan_start(&state->sss,
                          state->css.ss.ps.ps_ExprContext);



    /* create the scan desc */ 
    state->scandesc = index_beginscan(state->css.ss.ss_currentRelation,
                                      state->indexrel,
                                      state->css.ss.ps.state->es_snapshot,
                                      state->num_scan_keys,
                                      /*norderbys=*/0);
    state->scandesc->xs_want_itup = true;
    
    /* no runtime keys. We can pass the scan keys to AM at this time */
    if (state->num_runtime_keys == 0)
    {
        index_rescan(state->scandesc,
                     state->scan_keys,
                     state->num_scan_keys,
                     /*orderbys=*/NULL,
                     /*norderbys=*/0);
    }
    
    scan_tlist_len = list_length(scan->custom_scan_tlist);
    itupdesc_natts = state->scandesc->xs_itupdesc->natts;
    if (list_length(scan->custom_scan_tlist) > itupdesc_natts)
    {
        if (scan_tlist_len != itupdesc_natts + 1)
        {
            ereport(ERROR,
                    errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("AQPSWRIndexOnlyScan scan tlist length != : "
                           "itupdesc->natts + 1: %d %d",
                           scan_tlist_len,
                           itupdesc_natts));
        }
        state->want_prob = true;
    }
    else
    {
        if (scan_tlist_len != itupdesc_natts)
        {
            ereport(ERROR,
                    errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("AQPSWRIndexOnlyScan scan tlist length != : "
                           "itupdesc->natts: %d %d",
                           scan_tlist_len,
                           itupdesc_natts));
        }
        state->want_prob = false;
    }
}

static TupleTableSlot*
aqp_swr_index_only_scan_exec(PlanState *node)
{
    AQPSWRIndexOnlyScanState *state = (AQPSWRIndexOnlyScanState *) node;
    ExprState       *qual;
    ProjectionInfo  *projInfo;
    ExprContext     *econtext;
    IndexScanDesc   scandesc;
    TupleTableSlot  *slot;

    if (aqp_sample_scan_no_more_samples(&state->sss))
    {
        /* we've got enough samples */
        return NULL;
    }

    qual = state->css.ss.ps.qual;
    projInfo = state->css.ss.ps.ps_ProjInfo;
    econtext = state->css.ss.ps.ps_ExprContext;
    scandesc = state->scandesc;
    slot = state->css.ss.ss_ScanTupleSlot;
    
    /* first call: set up runtime keys */
    if (state->num_runtime_keys > 0 && !state->runtime_keys_ready)
    {
        aqp_sample_scan_mark_rescan_for_runtime_keys_only(&state->sss);
        aqp_swr_index_only_scan_rescan(&state->css);
    }

#define REJECT_TUPLE() \
    if (state->want_prob) \
    { \
        ExecStoreAllNullTuple(slot); \
        aqp_sample_scan_got_new_sample(&state->sss); \
        if (projInfo) { \
            econtext->ecxt_scantuple = slot; \
            return ExecProject(projInfo); \
        } \
        return slot; \
    } \
    continue;

    for (;;)
    {
        /*bool    tuple_from_heap; */
        double random_number;
        ItemPointer tid;

        ResetExprContext(econtext); 

        random_number = aqp_sample_scan_next_random_number(&state->sss);
        tid = index_samplenext_tid(scandesc, random_number);
    
        /* warn the user if the rejection rate is too high */
        aqp_sample_scan_check_for_high_rejection_rate(&state->sss);

        epoch_maybe_refresh();

        CHECK_FOR_INTERRUPTS();

        if (tid == NULL)
        {
            /* AB-tree rejection */
            REJECT_TUPLE();
        }

        /*tuple_from_heap = false; */
        
        /* 
         * See nodeIndexonlyscan.c for notes on the memory ordering effects
         * and why no lock or barrier is needed for checking visibility map.
         */
        if (!VM_ALL_VISIBLE(scandesc->heapRelation,
                            ItemPointerGetBlockNumber(tid),
                            &state->vmbuffer))
        {
            /* 
             * Not all heap tuples on this heap page is visible.
             * Go for the heap tuple to run visibility check.
             */
            if (!index_fetch_heap(scandesc, state->table_slot))
            {
                /* not visible */
                REJECT_TUPLE();
            }
            ExecClearTuple(state->table_slot);
            
            /* 
             * Same as nodeIndexonlyscan.c. We're not expecting non-MVCC
             * snapshots.
             */
            Assert(!scandesc->xs_heap_continue);
            if (scandesc->xs_heap_continue)
                elog(ERROR, "non-MVCC snapshots are not supported in index-only swr sample scan");

            /*tuple_from_heap = true; */
        }


        
        /* AB-tree never sets xs_hitup or ask us to recheck for index quals */
        Assert(scandesc->xs_hitup == NULL);
        Assert(!scandesc->xs_recheck);
        Assert(scandesc->xs_itup);

        /* fill data into the result slot */
        ExecClearTuple(slot);
        index_deform_tuple(scandesc->xs_itup,
                           scandesc->xs_itupdesc,
                           slot->tts_values,
                           slot->tts_isnull);
        if (state->want_prob)
        {
            double inv_prob = ((ABTScanOpaque) scandesc->opaque)->inv_prob;
            /* store the probability into the last column if we're asked to */
            slot->tts_values[scandesc->xs_itupdesc->natts] =
                Float8GetDatum(1.0 / inv_prob);
            slot->tts_isnull[scandesc->xs_itupdesc->natts] = false;
        }
        ExecStoreVirtualTuple(slot);
        
        /* 
         * XXX(zy) temporarily removed because I don't think page level
         * SI lock would make transactions with sampling serializable.
         *
         * Table level SI lock definitely works but maybe that's too
         * conservative.
         *
         * This requires further investigation.
         */
        /* take the SILock if we haven't read the tuple from heap */
        /*if (!tuple_from_heap)
            PredicateLockPage(scandesc->heapRelation,
                              ItemPointerGetBlockNumber(tid),
                              state->css.ss.ps.state->es_snapshot); */
        
        /* now evaluate the plan qual and projection if any */
        econtext->ecxt_scantuple = slot;
        if (qual == NULL || ExecQual(qual, econtext))
        {
            /* passes qual or no qual */
            aqp_sample_scan_got_new_sample(&state->sss);
            if (projInfo)
            {
                return ExecProject(projInfo);
            }
            else
            {
                return slot;
            }
        }
        else
        {
            /* oops, rejected by the qual */
            REJECT_TUPLE();
        }
        
    }
#undef REJECT_TUPLE
    
    elog(ERROR, "unreachable");
    return NULL;
}

static void
aqp_swr_index_only_scan_end(CustomScanState *node)
{
    AQPSWRIndexOnlyScanState *state = (AQPSWRIndexOnlyScanState *) node;
    
    /* release visibility map buffer pin */
    if (state->vmbuffer != InvalidBuffer)
    {
        ReleaseBuffer(state->vmbuffer);
        state->vmbuffer = InvalidBuffer;
    }

    /* no need to call ExecFreeExprContext (see that for reason) */
    
    /* 
     * NOTE(zy) we have to set result tuple slot to something with
     * a valid ttsops, or ExecEndCustomScan() will blindly invoke
     * ExecClearTuple on a NULL pointer!
     *
     * Note that table_slot is cleared on every heap fetch, so we don't
     * need to clear it again here.
     *
     * We don't need to invoke clear either of the scan or the result tuple
     * though. It will be handled by ExecEndCustomScan.
     */
    if (!state->css.ss.ps.ps_ResultTupleSlot)
        state->css.ss.ps.ps_ResultTupleSlot =
            state->css.ss.ss_ScanTupleSlot;

    if (state->scandesc)
        index_endscan(state->scandesc);
    if (state->indexrel)
        index_close(state->indexrel, NoLock);

}

static void
aqp_swr_index_only_scan_rescan(CustomScanState *node)
{
    AQPSWRIndexOnlyScanState *state = (AQPSWRIndexOnlyScanState *) node;
    
    /* recompute the runtime keys on each rescan if there's any */
    if (state->num_runtime_keys > 0)
    {
        ExprContext *econtext = state->runtime_context;
        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(econtext,
                                 state->runtime_keys,
                                 state->num_runtime_keys);
    }
    state->runtime_keys_ready = true;

    if (state->scandesc)
    {
        index_rescan(state->scandesc,
                     state->scan_keys,
                     state->num_scan_keys,
                     /*orderbys=*/NULL,
                     /*norderbys=*/0);
    }

    aqp_sample_scan_start(&state->sss, state->css.ss.ps.ps_ExprContext);
}

static void
aqp_swr_index_only_scan_explain(CustomScanState *node,
                                List *ancestors,
                                ExplainState *es)
{
    CustomScan *cscan = (CustomScan*) node->ss.ps.plan;
    AQPSWRIndexOnlyScanPrivate *private =
        (AQPSWRIndexOnlyScanPrivate *) linitial(cscan->custom_private);
    const char *index_name;

    index_name = get_rel_name(private->indexid);
    if (!index_name)
        elog(ERROR, "cache lookup failed for index %u", private->indexid);
    ExplainPropertyText("Index Name", index_name, es);

    aqp_pgport_show_scan_qual(private->indexqual, "Index Cond",
                              &node->ss.ps, ancestors, es);
    aqp_show_index_sample_scan_details(private->sample_size,
                                       private->sample_size_expr,
                                       private->repeatable_expr,
                                       &node->ss.ps,
                                       ancestors,
                                       es);
}

void
aqp_setup_swr(void)
{
    RegisterExtensibleNodeMethods(&aqp_swr_scan_private_methods);
    RegisterExtensibleNodeMethods(&aqp_swr_index_only_scan_private_methods);
    RegisterCustomScanMethods(&aqp_swrscan_methods);
    RegisterCustomScanMethods(&aqp_swrindexonlyscan_methods);
}

