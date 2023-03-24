/*-------------------------------------------------------------------------
 *
 * abtree.c
 *	  An aggregate B-Tree implementation based on Lehman and Yao's btree
 *	  management algorithm for Postgres.
 *
 *	  Copied and adapted from src/backend/access/nbtree/nbtree.c.
 *
 * NOTES
 *	  This file contains only the public interface routines.
 *
 * IDENTIFICATION
 *	  src/backend/access/abtree/abtree.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/abtree.h"
#include "access/abtxlog.h"
#include "access/relscan.h"
#include "access/xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/condition_variable.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "utils/memutils.h"
#include "utils/timeout.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>


bool abtree_enable_metrics_collection = false;
int abtree_metrics_reporter_delay = 5000;
int abtree_metrics_reporter_port = 3002;
pg_atomic_uint64 *abtree_ninserts = NULL;
pg_atomic_uint64 *abtree_nsamples_accepted = NULL;
pg_atomic_uint64 *abtree_nsamples_rejected = NULL;
int _abt_conn_fd = -1;
int _abt_svc_fd = -1;

static void _abt_metrics_reporter_cleanup_port(int status, Datum arg);

/* Working state needed by abtvacuumpage */
typedef struct
{
	IndexVacuumInfo *info;
	IndexBulkDeleteResult *stats;
	IndexBulkDeleteCallback callback;
	void	   *callback_state;
	ABTCycleId	cycleid;
	BlockNumber totFreePages;	/* true total # of free pages */
	TransactionId oldestABtpoXact;
	MemoryContext pagedelcontext;
	ABTCachedAggInfo agg_info;
} ABTVacState;

/*
 * ABTPARALLEL_NOT_INITIALIZED indicates that the scan has not started.
 *
 * ABTPARALLEL_ADVANCING indicates that some process is advancing the scan to
 * a new page; others must wait.
 *
 * ABTPARALLEL_IDLE indicates that no backend is currently advancing the scan
 * to a new page; some process can start doing that.
 *
 * ABTPARALLEL_DONE indicates that the scan is complete (including error exit).
 * We reach this state once for every distinct combination of array keys.
 */
typedef enum
{
	ABTPARALLEL_NOT_INITIALIZED,
	ABTPARALLEL_ADVANCING,
	ABTPARALLEL_IDLE,
	ABTPARALLEL_DONE
} ABTPS_State;

/*
 * ABTParallelScanDescData contains btree specific shared information required
 * for parallel scan.
 */
typedef struct ABTParallelScanDescData
{
	BlockNumber btps_scanPage;	/* latest or next page to be scanned */
	ABTPS_State	btps_pageStatus;	/* indicates whether next page is
									 * available for scan. see above for
									 * possible states of parallel scan. */
	int			btps_arrayKeyCount; /* count indicating number of array scan
									 * keys processed by parallel scan */
	slock_t		btps_mutex;		/* protects above variables */
	ConditionVariable btps_cv;	/* used to synchronize parallel scan */
}			ABTParallelScanDescData;

typedef struct ABTParallelScanDescData *ABTParallelScanDesc;


static void abtvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
						 IndexBulkDeleteCallback callback, void *callback_state,
						 ABTCycleId cycleid);
static void abtvacuumpage(ABTVacState *vstate, BlockNumber scanblkno);
static ABTVacuumPosting abtreevacuumposting(ABTVacState *vstate,
										  IndexTuple posting,
										  OffsetNumber updatedoffset,
										  int *nremaining,
                                          Datum *deleted_sum,
                                          bool *is_first_deleted);


/*
 * Btree handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
abthandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = ABTNProcs;
	amroutine->amoptsprocnum = ABTOPTIONS_PROC;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = true;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = true;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false; /* TODO flip to true? */
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = abtbuild;
	amroutine->ambuildempty = abtbuildempty;

	amroutine->aminsert = abtinsert;
	amroutine->ambulkdelete = abtbulkdelete;
	amroutine->amvacuumcleanup = abtvacuumcleanup;
	amroutine->amcanreturn = abtcanreturn;
	amroutine->amcostestimate = abtcostestimate;
	amroutine->amoptions = abtoptions;
	amroutine->amproperty = abtproperty;
	amroutine->ambuildphasename = abtbuildphasename;
	amroutine->amvalidate = abtvalidate;
	amroutine->ambeginscan = abtbeginscan;
	amroutine->amrescan = abtrescan;
	amroutine->amgettuple = abtgettuple;
	amroutine->amsampletuple = abtsampletuple;
	amroutine->amgetbitmap = abtgetbitmap;
	amroutine->amendscan = abtendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	/*amroutine->ammarkpos = abtmarkpos;
	amroutine->amrestrpos = abtrestrpos; 
	amroutine->amestimateparallelscan = abtestimateparallelscan;
	amroutine->aminitparallelscan = abtinitparallelscan;
	amroutine->amparallelrescan = abtparallelrescan; */

	PG_RETURN_POINTER(amroutine);
}

/*
 *	btbuildempty() -- build an empty btree index in the initialization fork
 */
void
abtbuildempty(Relation index)
{
	Page		metapage;
	ABTOptions	*options;
	Datum		agg_support_datum;
	ABTAggSupport	agg_support;

	/* Get agg support. */
	options = (ABTOptions *) index->rd_options;
	agg_support_datum = OidFunctionCall1(options->agg_support_fn,
		ObjectIdGetDatum(options->aggregation_type.typeOid));
	agg_support = (ABTAggSupport) DatumGetPointer(agg_support_datum);
	Assert(agg_support);

	/* Construct metapage. */
	metapage = (Page) palloc(BLCKSZ);
	_abt_initmetapage(agg_support, metapage, ABTP_NONE, 0,
					  _abt_allequalimage(index, false));
	pfree(agg_support);

	/*
	 * Write the page and log it.  It might seem that an immediate sync would
	 * be sufficient to guarantee that the file exists on disk, but recovery
	 * itself might remove it while replaying, for example, an
	 * XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE record.  Therefore, we need
	 * this even when wal_level=minimal.
	 */
	PageSetChecksumInplace(metapage, ABTREE_METAPAGE);
	smgrwrite(index->rd_smgr, INIT_FORKNUM, ABTREE_METAPAGE,
			  (char *) metapage, true);
	log_newpage(&index->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
				ABTREE_METAPAGE, metapage, true);

	/*
	 * An immediate sync is required even if we xlog'd the page, because the
	 * write did not go through shared_buffers and therefore a concurrent
	 * checkpoint may have moved the redo pointer past our xlog record.
	 */
	smgrimmedsync(index->rd_smgr, INIT_FORKNUM);
}

/*
 *	btinsert() -- insert an index tuple into a btree.
 *
 *		Descend the tree recursively, find the appropriate location for our
 *		new tuple, and put it there.
 */
bool
abtinsert(Relation rel, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique,
		 IndexInfo *indexInfo)
{
	bool				result;
	IndexTuple			itup;
	ABTCachedAggInfo	agg_info;
	
	/* Make a local copy of the cached agg info. */
	if (!indexInfo->ii_AmCache)
	{
		indexInfo->ii_AmCache =
			_abt_copy_cached_agg_info(rel, indexInfo->ii_Context);
	}
	agg_info = (ABTCachedAggInfo) indexInfo->ii_AmCache;

	itup = index_form_tuple_with_extra(RelationGetDescr(rel),
		values, isnull, agg_info->extra_space_for_nonpivot_tuple, 'm');
	/* generate an index tuple */
	if (agg_info->leaf_has_agg)
	{
		Datum	agg_val;

		/* 
		 * Reserve the space for the leaf level aggregation value.
		 * Keep in sync with _abt_spool().
		 */

		/* And set the value. */
		agg_val = (agg_info->agg_support.abtas_map.fn)(itup);
		_abt_set_nonpivot_tuple_aggregation_value(itup, agg_val, agg_info);
	}
	itup->t_tid = *ht_ctid;

	/* 
	 * We initially set the xmin in the non-pivot tuple as invalid so that
	 * concurrent split cannot count this tuple towards the aggregations while
	 * it's insertion is still in flight. We'll do a second descend to install
	 * its xmin.
	 */
	*ABTreeNonPivotTupleGetXminPtr(itup) = InvalidTransactionId;

	result = _abt_doinsert(rel, itup, checkUnique, heapRel, agg_info);

	pfree(itup);
    
    if (abtree_enable_metrics_collection)
    {
        _abt_inc_ninserts();
    }
	return result;
}

/*
 *	btgettuple() -- Get the next tuple in the scan.
 */
bool
abtgettuple(IndexScanDesc scan, ScanDirection dir)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	bool		res;

	/* btree indexes are never lossy */
	scan->xs_recheck = false;

	/*
	 * If we have any array keys, initialize them during first call for a
	 * scan.  We can't do this in btrescan because we don't know the scan
	 * direction at that time.
	 */
	if (so->numArrayKeys && !ABTScanPosIsValid(so->currPos))
	{
		/* punt if we have any unsatisfiable array keys */
		if (so->numArrayKeys < 0)
			return false;

		_abt_start_array_keys(scan, dir);
	}

	/* This loop handles advancing to the next array elements, if any */
	do
	{
		/*
		 * If we've already initialized this scan, we can just advance it in
		 * the appropriate direction.  If we haven't done so yet, we call
		 * _abt_first() to get the first item in the scan.
		 */
		if (!ABTScanPosIsValid(so->currPos))
			res = _abt_first(scan, dir);
		else
		{
			/*
			 * Check to see if we should kill the previously-fetched tuple.
			 */
			if (scan->kill_prior_tuple)
			{
				/*
				 * Yes, remember it for later. (We'll deal with all such
				 * tuples at once right before leaving the index page.)  The
				 * test for numKilled overrun is not just paranoia: if the
				 * caller reverses direction in the indexscan then the same
				 * item might get entered multiple times. It's not worth
				 * trying to optimize that, so we don't detect it, but instead
				 * just forget any excess entries.
				 */
				if (so->killedItems == NULL)
					so->killedItems = (int *)
						palloc(MaxTIDsPerABTreePage * sizeof(int));
				if (so->numKilled < MaxTIDsPerABTreePage)
					so->killedItems[so->numKilled++] = so->currPos.itemIndex;
			}

			/*
			 * Now continue the scan.
			 */
			res = _abt_next(scan, dir);
		}

		/* If we have a tuple, return it ... */
		if (res)
			break;
		/* ... otherwise see if we have more array keys to deal with */
	} while (so->numArrayKeys && _abt_advance_array_keys(scan, dir));

	return res;
}

/*
 * abtsampletuple() -- Sample the next tuple.
 */
bool
abtsampletuple(IndexScanDesc scan, double random_number)
{
	ABTScanOpaque	so = (ABTScanOpaque) scan->opaque;
	bool			res;

	scan->xs_recheck = false;

	if (!so->snapshot_prepared)
	{
		so->snapshot = _abt_prepare_snapshot_for_sampling(scan->xs_snapshot);
		so->snapshot_prepared = true;
	}

	if (so->numArrayKeys)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Currently we can't handle array key for sampling in "
						"abtree.")));
	}

	if (!so->prefix_sums)
	{
		int			max_num_items;
		/* 
		 * If aggregation type is passed by reference, allocate a new
		 * context to hold the prefix sum values.
		 */
		Assert(!so->prefix_sums_context);
		if (!so->agg_info->agg_byval)
			so->prefix_sums_context =
				AllocSetContextCreate(CurrentMemoryContext,
									  "ABTree prefix sums context",
									  ALLOCSET_SMALL_SIZES);
		
		Assert(!so->prefix_sums);
		/* 
		 * Just be extremely conservative about the maximum number of items. 
		 * We can't include the tuple header overhead because, when the root
		 * page is a leaf, there might be posting tuples that amortizes the
		 * tuple header overhead.
		 */
		max_num_items = (int)((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) -
							   MAXALIGN(sizeof(ABTPageOpaqueData)) +
							   (so->agg_info->agg_typlen - 1)) /
							  so->agg_info->agg_typlen);
#ifdef USE_ASSERT_CHECKING
		so->max_num_items = max_num_items;
#endif
		so->prefix_sums = palloc(max_num_items * sizeof(uint64));
		so->item_indexes = palloc(max_num_items * sizeof(uint32));
	}
	
	/* We haven't preprocessed the key since last rescan. Do it here. */
	if (!so->preprocessed)
	{
		_abt_preprocess_keys(scan);
		_abt_build_start_inskey(scan, ForwardScanDirection);
		if (so->inskey)
		{
			Assert(!so->goback);
			so->inskey_low = so->inskey;
			so->inskey = NULL;
		}
		_abt_build_start_inskey(scan, BackwardScanDirection);
		if (so->inskey)
		{
			Assert(so->goback);
			so->inskey_high = so->inskey;
			so->inskey = NULL;
		}
	}
	
	/* Someone may have discovered that the previous fetched tuple is dead. */
	if (scan->kill_prior_tuple)
	{
		Assert(so->killedItems == NULL);
		so->killedItems = &so->currPos.itemIndex;
		so->numKilled = 1;
		_abt_killitems(scan);
		/* Reset the killedItems because it points to some temporary location */
		so->killedItems = NULL;
	}
	
	if (ABTScanPosIsValid(so->currPos))
	{
		ABTScanPosUnpinIfPinned(so->currPos);
		ABTScanPosInvalidate(so->currPos);
	}

	res = _abt_sample(scan, random_number);
    if (abtree_enable_metrics_collection)
    {
        if (res)
            _abt_inc_nsamples_accepted();
        else
            _abt_inc_nsamples_rejected();
    }
	return res;
}

/*
 * btgetbitmap() -- gets all matching tuples, and adds them to a bitmap
 */
int64
abtgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	int64		ntids = 0;
	ItemPointer heapTid;

	/*
	 * If we have any array keys, initialize them.
	 */
	if (so->numArrayKeys)
	{
		/* punt if we have any unsatisfiable array keys */
		if (so->numArrayKeys < 0)
			return ntids;

		_abt_start_array_keys(scan, ForwardScanDirection);
	}

	/* This loop handles advancing to the next array elements, if any */
	do
	{
		/* Fetch the first page & tuple */
		if (_abt_first(scan, ForwardScanDirection))
		{
			/* Save tuple ID, and continue scanning */
			heapTid = &scan->xs_heaptid;
			tbm_add_tuples(tbm, heapTid, 1, false);
			ntids++;

			for (;;)
			{
				/*
				 * Advance to next tuple within page.  This is the same as the
				 * easy case in _abt_next().
				 */
				if (++so->currPos.itemIndex > so->currPos.lastItem)
				{
					/* let _abt_next do the heavy lifting */
					if (!_abt_next(scan, ForwardScanDirection))
						break;
				}

				/* Save tuple ID, and continue scanning */
				heapTid = &so->currPos.items[so->currPos.itemIndex].heapTid;
				tbm_add_tuples(tbm, heapTid, 1, false);
				ntids++;
			}
		}
		/* Now see if we have more array keys to deal with */
	} while (so->numArrayKeys && _abt_advance_array_keys(scan, ForwardScanDirection));

	return ntids;
}

/*
 *	btbeginscan() -- start a scan on a btree index
 */
IndexScanDesc
abtbeginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	ABTScanOpaque so;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	/* get the scan */
	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	/* allocate private workspace */
	so = (ABTScanOpaque) palloc(sizeof(ABTScanOpaqueData));
	ABTScanPosInvalidate(so->currPos);
	ABTScanPosInvalidate(so->markPos);
	if (scan->numberOfKeys > 0)
		so->keyData = (ScanKey) palloc(scan->numberOfKeys * sizeof(ScanKeyData));
	else
		so->keyData = NULL;

	so->arrayKeyData = NULL;	/* assume no array keys for now */
	so->numArrayKeys = 0;
	so->arrayKeys = NULL;
	so->arrayContext = NULL;

	so->killedItems = NULL;		/* until needed */
	so->numKilled = 0;

	/*
	 * We don't know yet whether the scan will be index-only, so we do not
	 * allocate the tuple workspace arrays until btrescan.  However, we set up
	 * scan->xs_itupdesc whether we'll need it or not, since that's so cheap.
	 */
	so->currTuples = so->markTuples = NULL;

	so->inskey = NULL;
	so->inskey_pivot = _abt_mkscankey(rel, NULL);
	_abt_metaversion(rel, &so->inskey_pivot->heapkeyspace,
						  &so->inskey_pivot->allequalimage);
	
	/* agg_info is now needed by all access interfaces. */
	so->agg_info = _abt_copy_cached_agg_info(scan->indexRelation,
											 CurrentMemoryContext);
	so->inskey_low = NULL;
	so->inskey_high = NULL;
	so->prefix_sums_context = NULL;
	so->prefix_sums = NULL;
	so->item_indexes = NULL;
	
	so->snapshot_prepared = false;
	so->snapshot = NULL;

	scan->xs_itupdesc = RelationGetDescr(rel);

	scan->opaque = so;

	return scan;
}

/*
 *	btrescan() -- rescan an index relation
 */
void
abtrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys) {
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;

	/* we aren't holding any read locks, but gotta drop the pins */
	if (ABTScanPosIsValid(so->currPos))
	{
		/* Before leaving current page, deal with any killed items */
		if (so->numKilled > 0)
			_abt_killitems(scan);
		ABTScanPosUnpinIfPinned(so->currPos);
		ABTScanPosInvalidate(so->currPos);
	}

	so->markItemIndex = -1;
	so->arrayKeyCount = 0;
	ABTScanPosUnpinIfPinned(so->markPos);
	ABTScanPosInvalidate(so->markPos);

	/*
	 * Allocate tuple workspace arrays, if needed for an index-only scan and
	 * not already done in a previous rescan call.  To save on palloc
	 * overhead, both workspaces are allocated as one palloc block; only this
	 * function and btendscan know that.
	 *
	 * NOTE: this data structure also makes it safe to return data from a
	 * "name" column, even though btree name_ops uses an underlying storage
	 * datatype of cstring.  The risk there is that "name" is supposed to be
	 * padded to NAMEDATALEN, but the actual index tuple is probably shorter.
	 * However, since we only return data out of tuples sitting in the
	 * currTuples array, a fetch of NAMEDATALEN bytes can at worst pull some
	 * data out of the markTuples array --- running off the end of memory for
	 * a SIGSEGV is not possible.  Yeah, this is ugly as sin, but it beats
	 * adding special-case treatment for name_ops elsewhere.
	 */
	if (scan->xs_want_itup && so->currTuples == NULL)
	{
		so->currTuples = (char *) palloc(BLCKSZ * 2);
		so->markTuples = so->currTuples + BLCKSZ;
	}

	/*
	 * Reset the scan keys. Note that keys ordering stuff moved to _abt_first.
	 * - vadim 05/05/97
	 */
	if (scankey && scan->numberOfKeys > 0)
		memmove(scan->keyData,
				scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	so->numberOfKeys = 0;		/* until _abt_preprocess_keys sets it */
	so->preprocessed = false;

	/* If any keys are SK_SEARCHARRAY type, set up array-key info */
	_abt_preprocess_array_keys(scan);

	if (so->inskey)
	{
		/* Free the inskey created before this rescan call. */
		pfree(so->inskey);
		so->inskey = NULL;
	}

	if (so->inskey_low)
	{
		pfree(so->inskey_low);
		so->inskey_low = NULL;
	}

	if (so->inskey_high)
	{
		pfree(so->inskey_high);
		so->inskey_high = NULL;
	}

	if (so->snapshot)
		pfree(so->snapshot);

	so->snapshot = NULL;
	so->snapshot_prepared = false;
}

/*
 *	btendscan() -- close down a scan
 */
void
abtendscan(IndexScanDesc scan)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;

	/* we aren't holding any read locks, but gotta drop the pins */
	if (ABTScanPosIsValid(so->currPos))
	{
		/* Before leaving current page, deal with any killed items */
		if (so->numKilled > 0)
			_abt_killitems(scan);
		ABTScanPosUnpinIfPinned(so->currPos);
	}

	so->markItemIndex = -1;
	ABTScanPosUnpinIfPinned(so->markPos);

	/* No need to invalidate positions, the RAM is about to be freed. */

	/* Release storage */
	if (so->keyData != NULL)
		pfree(so->keyData);
	/* so->arrayKeyData and so->arrayKeys are in arrayContext */
	if (so->arrayContext != NULL)
		MemoryContextDelete(so->arrayContext);
	if (so->killedItems != NULL)
		pfree(so->killedItems);
	if (so->currTuples != NULL)
		pfree(so->currTuples);
	if (so->inskey != NULL)
		pfree(so->inskey);
	if (so->inskey_low != NULL)
		pfree(so->inskey_low);
	if (so->inskey_high != NULL)
		pfree(so->inskey_high);
	if (so->inskey_pivot != NULL)
		pfree(so->inskey_pivot);
	if (so->agg_info != NULL)
		pfree(so->agg_info);
	/* The pass-by-ref datums in so->prefix_sums are in prefix_sums_context */
	if (so->prefix_sums != NULL)
		pfree(so->prefix_sums);
	if (so->prefix_sums_context != NULL)
		MemoryContextDelete(so->prefix_sums_context);
	if (so->item_indexes != NULL)
		pfree(so->item_indexes);
	if (so->snapshot != NULL)
		pfree(so->snapshot);
	/* so->markTuples should not be pfree'd, see btrescan */
	pfree(so);
}

/*
 *	btmarkpos() -- save current scan position
 */
void
abtmarkpos(IndexScanDesc scan)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;

	/* There may be an old mark with a pin (but no lock). */
	ABTScanPosUnpinIfPinned(so->markPos);

	/*
	 * Just record the current itemIndex.  If we later step to next page
	 * before releasing the marked position, _abt_steppage makes a full copy of
	 * the currPos struct in markPos.  If (as often happens) the mark is moved
	 * before we leave the page, we don't have to do that work.
	 */
	if (ABTScanPosIsValid(so->currPos))
		so->markItemIndex = so->currPos.itemIndex;
	else
	{
		ABTScanPosInvalidate(so->markPos);
		so->markItemIndex = -1;
	}

	/* Also record the current positions of any array keys */
	if (so->numArrayKeys)
		_abt_mark_array_keys(scan);
}

/*
 *	btrestrpos() -- restore scan to last saved position
 */
void
abtrestrpos(IndexScanDesc scan)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;

	/* Restore the marked positions of any array keys */
	if (so->numArrayKeys)
		_abt_restore_array_keys(scan);

	if (so->markItemIndex >= 0)
	{
		/*
		 * The scan has never moved to a new page since the last mark.  Just
		 * restore the itemIndex.
		 *
		 * NB: In this case we can't count on anything in so->markPos to be
		 * accurate.
		 */
		so->currPos.itemIndex = so->markItemIndex;
	}
	else
	{
		/*
		 * The scan moved to a new page after last mark or restore, and we are
		 * now restoring to the marked page.  We aren't holding any read
		 * locks, but if we're still holding the pin for the current position,
		 * we must drop it.
		 */
		if (ABTScanPosIsValid(so->currPos))
		{
			/* Before leaving current page, deal with any killed items */
			if (so->numKilled > 0)
				_abt_killitems(scan);
			ABTScanPosUnpinIfPinned(so->currPos);
		}

		if (ABTScanPosIsValid(so->markPos))
		{
			/* bump pin on mark buffer for assignment to current buffer */
			if (ABTScanPosIsPinned(so->markPos))
				IncrBufferRefCount(so->markPos.buf);
			memcpy(&so->currPos, &so->markPos,
				   offsetof(ABTScanPosData, items[1]) +
				   so->markPos.lastItem * sizeof(ABTScanPosItem));
			if (so->currTuples)
				memcpy(so->currTuples, so->markTuples,
					   so->markPos.nextTupleOffset);
		}
		else
			ABTScanPosInvalidate(so->currPos);
	}
}

/*
 * btestimateparallelscan -- estimate storage for ABTParallelScanDescData
 */
Size
abtestimateparallelscan(void)
{
	return sizeof(ABTParallelScanDescData);
}

/*
 * btinitparallelscan -- initialize ABTParallelScanDesc for parallel btree scan
 */
void
abtinitparallelscan(void *target)
{
	ABTParallelScanDesc bt_target = (ABTParallelScanDesc) target;

	SpinLockInit(&bt_target->btps_mutex);
	bt_target->btps_scanPage = InvalidBlockNumber;
	bt_target->btps_pageStatus = ABTPARALLEL_NOT_INITIALIZED;
	bt_target->btps_arrayKeyCount = 0;
	ConditionVariableInit(&bt_target->btps_cv);
}

/*
 *	btparallelrescan() -- reset parallel scan
 */
void
abtparallelrescan(IndexScanDesc scan)
{
	ABTParallelScanDesc btscan;
	ParallelIndexScanDesc parallel_scan = scan->parallel_scan;

	Assert(parallel_scan);

	btscan = (ABTParallelScanDesc) OffsetToPointer((void *) parallel_scan,
												  parallel_scan->ps_offset);

	/*
	 * In theory, we don't need to acquire the spinlock here, because there
	 * shouldn't be any other workers running at this point, but we do so for
	 * consistency.
	 */
	SpinLockAcquire(&btscan->btps_mutex);
	btscan->btps_scanPage = InvalidBlockNumber;
	btscan->btps_pageStatus = ABTPARALLEL_NOT_INITIALIZED;
	btscan->btps_arrayKeyCount = 0;
	SpinLockRelease(&btscan->btps_mutex);
}

/*
 * _abt_parallel_seize() -- Begin the process of advancing the scan to a new
 *		page.  Other scans must wait until we call _abt_parallel_release()
 *		or _abt_parallel_done().
 *
 * The return value is true if we successfully seized the scan and false
 * if we did not.  The latter case occurs if no pages remain for the current
 * set of scankeys.
 *
 * If the return value is true, *pageno returns the next or current page
 * of the scan (depending on the scan direction).  An invalid block number
 * means the scan hasn't yet started, and ABTP_NONE means we've reached the end.
 * The first time a participating process reaches the last page, it will return
 * true and set *pageno to ABTP_NONE; after that, further attempts to seize the
 * scan will return false.
 *
 * Callers should ignore the value of pageno if the return value is false.
 */
bool
_abt_parallel_seize(IndexScanDesc scan, BlockNumber *pageno)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	ABTPS_State	pageStatus;
	bool		exit_loop = false;
	bool		status = true;
	ParallelIndexScanDesc parallel_scan = scan->parallel_scan;
	ABTParallelScanDesc btscan;

	*pageno = ABTP_NONE;

	btscan = (ABTParallelScanDesc) OffsetToPointer((void *) parallel_scan,
												  parallel_scan->ps_offset);

	while (1)
	{
		SpinLockAcquire(&btscan->btps_mutex);
		pageStatus = btscan->btps_pageStatus;

		if (so->arrayKeyCount < btscan->btps_arrayKeyCount)
		{
			/* Parallel scan has already advanced to a new set of scankeys. */
			status = false;
		}
		else if (pageStatus == ABTPARALLEL_DONE)
		{
			/*
			 * We're done with this set of scankeys.  This may be the end, or
			 * there could be more sets to try.
			 */
			status = false;
		}
		else if (pageStatus != ABTPARALLEL_ADVANCING)
		{
			/*
			 * We have successfully seized control of the scan for the purpose
			 * of advancing it to a new page!
			 */
			btscan->btps_pageStatus = ABTPARALLEL_ADVANCING;
			*pageno = btscan->btps_scanPage;
			exit_loop = true;
		}
		SpinLockRelease(&btscan->btps_mutex);
		if (exit_loop || !status)
			break;
		ConditionVariableSleep(&btscan->btps_cv, WAIT_EVENT_ABTREE_PAGE);
	}
	ConditionVariableCancelSleep();

	return status;
}

/*
 * _abt_parallel_release() -- Complete the process of advancing the scan to a
 *		new page.  We now have the new value btps_scanPage; some other backend
 *		can now begin advancing the scan.
 */
void
_abt_parallel_release(IndexScanDesc scan, BlockNumber scan_page)
{
	ParallelIndexScanDesc parallel_scan = scan->parallel_scan;
	ABTParallelScanDesc btscan;

	btscan = (ABTParallelScanDesc) OffsetToPointer((void *) parallel_scan,
												  parallel_scan->ps_offset);

	SpinLockAcquire(&btscan->btps_mutex);
	btscan->btps_scanPage = scan_page;
	btscan->btps_pageStatus = ABTPARALLEL_IDLE;
	SpinLockRelease(&btscan->btps_mutex);
	ConditionVariableSignal(&btscan->btps_cv);
}

/*
 * _abt_parallel_done() -- Mark the parallel scan as complete.
 *
 * When there are no pages left to scan, this function should be called to
 * notify other workers.  Otherwise, they might wait forever for the scan to
 * advance to the next page.
 */
void
_abt_parallel_done(IndexScanDesc scan)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	ParallelIndexScanDesc parallel_scan = scan->parallel_scan;
	ABTParallelScanDesc btscan;
	bool		status_changed = false;

	/* Do nothing, for non-parallel scans */
	if (parallel_scan == NULL)
		return;

	btscan = (ABTParallelScanDesc) OffsetToPointer((void *) parallel_scan,
												  parallel_scan->ps_offset);

	/*
	 * Mark the parallel scan as done for this combination of scan keys,
	 * unless some other process already did so.  See also
	 * _abt_advance_array_keys.
	 */
	SpinLockAcquire(&btscan->btps_mutex);
	if (so->arrayKeyCount >= btscan->btps_arrayKeyCount &&
		btscan->btps_pageStatus != ABTPARALLEL_DONE)
	{
		btscan->btps_pageStatus = ABTPARALLEL_DONE;
		status_changed = true;
	}
	SpinLockRelease(&btscan->btps_mutex);

	/* wake up all the workers associated with this parallel scan */
	if (status_changed)
		ConditionVariableBroadcast(&btscan->btps_cv);
}

/*
 * _abt_parallel_advance_array_keys() -- Advances the parallel scan for array
 *			keys.
 *
 * Updates the count of array keys processed for both local and parallel
 * scans.
 */
void
_abt_parallel_advance_array_keys(IndexScanDesc scan)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	ParallelIndexScanDesc parallel_scan = scan->parallel_scan;
	ABTParallelScanDesc btscan;

	btscan = (ABTParallelScanDesc) OffsetToPointer((void *) parallel_scan,
												  parallel_scan->ps_offset);

	so->arrayKeyCount++;
	SpinLockAcquire(&btscan->btps_mutex);
	if (btscan->btps_pageStatus == ABTPARALLEL_DONE)
	{
		btscan->btps_scanPage = InvalidBlockNumber;
		btscan->btps_pageStatus = ABTPARALLEL_NOT_INITIALIZED;
		btscan->btps_arrayKeyCount++;
	}
	SpinLockRelease(&btscan->btps_mutex);
}

/*
 * _abt_vacuum_needs_cleanup() -- Checks if index needs cleanup
 *
 * Called by btvacuumcleanup when btbulkdelete was never called because no
 * tuples need to be deleted.
 *
 * When we return false, VACUUM can even skip the cleanup-only call to
 * abtvacuumscan (i.e. there will be no abtvacuumscan call for this index at
 * all).  Otherwise, a cleanup-only abtvacuumscan call is required.
 */
static bool
_abt_vacuum_needs_cleanup(IndexVacuumInfo *info)
{
	Buffer		metabuf;
	Page		metapg;
	ABTMetaPageData *metad;
	bool		result = false;

	metabuf = _abt_getbuf(info->index, ABTREE_METAPAGE, ABT_READ);
	metapg = BufferGetPage(metabuf);
	metad = ABTPageGetMeta(metapg);

	if (metad->abtm_version < ABTREE_NOVAC_VERSION)
	{
		/*
		 * Do cleanup if metapage needs upgrade, because we don't have
		 * cleanup-related meta-information yet.
		 */
		result = true;
	}
	else if (TransactionIdIsValid(metad->abtm_oldest_abtpo_xact) &&
			 TransactionIdPrecedes(metad->abtm_oldest_abtpo_xact,
								   RecentGlobalXmin))
	{
		/*
		 * If any oldest a.xact from a previously deleted page in the index
		 * is older than RecentGlobalXmin, then at least one deleted page can
		 * be recycled -- don't skip cleanup.
		 */
		result = true;
	}
	else
	{
		ABTOptions  *relopts;
		float8		cleanup_scale_factor;
		float8		prev_num_heap_tuples;

		/*
		 * If table receives enough insertions and no cleanup was performed,
		 * then index would appear have stale statistics.  If scale factor is
		 * set, we avoid that by performing cleanup if the number of inserted
		 * tuples exceeds vacuum_cleanup_index_scale_factor fraction of
		 * original tuples count.
		 */
		relopts = (ABTOptions *) info->index->rd_options;
		cleanup_scale_factor = (relopts &&
								relopts->vacuum_cleanup_index_scale_factor >= 0)
			? relopts->vacuum_cleanup_index_scale_factor
			: vacuum_cleanup_index_scale_factor;
		prev_num_heap_tuples = metad->abtm_last_cleanup_num_heap_tuples;

		if (cleanup_scale_factor <= 0 ||
			prev_num_heap_tuples <= 0 ||
			(info->num_heap_tuples - prev_num_heap_tuples) /
			prev_num_heap_tuples >= cleanup_scale_factor)
			result = true;
	}

	_abt_relbuf(info->index, metabuf);
	return result;
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *
abtbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			 IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	rel = info->index;
	ABTCycleId	cycleid;

	/* allocate stats if first time through, else re-use existing struct */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/* Establish the vacuum cycle ID to use for this scan */
	/* The ENSURE stuff ensures we clean up shared memory on failure */
	PG_ENSURE_ERROR_CLEANUP(_abt_end_vacuum_callback, PointerGetDatum(rel));
	{
		cycleid = _abt_start_vacuum(rel);

		abtvacuumscan(info, stats, callback, callback_state, cycleid);
	}
	PG_END_ENSURE_ERROR_CLEANUP(_abt_end_vacuum_callback, PointerGetDatum(rel));
	_abt_end_vacuum(rel);

	return stats;
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *
abtvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	/* No-op in ANALYZE ONLY mode */
	if (info->analyze_only)
		return stats;

	/*
	 * If btbulkdelete was called, we need not do anything, just return the
	 * stats from the latest btbulkdelete call.  If it wasn't called, we might
	 * still need to do a pass over the index, to recycle any newly-recyclable
	 * pages or to obtain index statistics.  _abt_vacuum_needs_cleanup
	 * determines if either are needed.
	 *
	 * Since we aren't going to actually delete any leaf items, there's no
	 * need to go through all the vacuum-cycle-ID pushups.
	 */
	if (stats == NULL)
	{
		/* Check if we need a cleanup */
		if (!_abt_vacuum_needs_cleanup(info))
			return NULL;

		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		abtvacuumscan(info, stats, NULL, NULL, 0);
	}

	/*
	 * It's quite possible for us to be fooled by concurrent page splits into
	 * double-counting some index tuples, so disbelieve any total that exceeds
	 * the underlying heap's count ... if we know that accurately.  Otherwise
	 * this might just make matters worse.
	 *
	 * Posting list tuples are another source of inaccuracy.  Cleanup-only
	 * abtvacuumscan calls assume that the number of index tuples can be used
	 * as num_index_tuples, even though num_index_tuples is supposed to
	 * represent the number of TIDs in the index.  This naive approach can
	 * underestimate the number of tuples in the index.
	 */
	if (!info->estimated_count)
	{
		if (stats->num_index_tuples > info->num_heap_tuples)
			stats->num_index_tuples = info->num_heap_tuples;
	}

	return stats;
}

/*
 * abtvacuumscan --- scan the index for VACUUMing purposes
 *
 * This combines the functions of looking for leaf tuples that are deletable
 * according to the vacuum callback, looking for empty pages that can be
 * deleted, and looking for old deleted pages that can be recycled.  Both
 * btbulkdelete and btvacuumcleanup invoke this (the latter only if no
 * btbulkdelete call occurred and _abt_vacuum_needs_cleanup returned true).
 * Note that this is also where the metadata used by _abt_vacuum_needs_cleanup
 * is maintained.
 *
 * The caller is responsible for initially allocating/zeroing a stats struct
 * and for obtaining a vacuum cycle ID if necessary.
 */
static void
abtvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			 IndexBulkDeleteCallback callback, void *callback_state,
			 ABTCycleId cycleid)
{
	Relation	rel = info->index;
	ABTVacState	vstate;
	BlockNumber num_pages;
	BlockNumber scanblkno;
	bool		needLock;

	/*
	 * Reset counts that will be incremented during the scan; needed in case
	 * of multiple scans during a single VACUUM command
	 */
	stats->estimated_count = false;
	stats->num_index_tuples = 0;
	stats->pages_deleted = 0;

	/* Set up info to pass down to abtvacuumpage */
	vstate.info = info;
	vstate.stats = stats;
	vstate.callback = callback;
	vstate.callback_state = callback_state;
	vstate.cycleid = cycleid;
	vstate.totFreePages = 0;
	vstate.oldestABtpoXact = InvalidTransactionId;

	/* Create a temporary memory context to run _abt_pagedel in */
	vstate.pagedelcontext = AllocSetContextCreate(CurrentMemoryContext,
												  "_abt_pagedel",
												  ALLOCSET_DEFAULT_SIZES);
	
	/* 
	 * Make a copy of the cached agg info (or fetch from meta page if not
	 * already).
	 */
	vstate.agg_info = _abt_copy_cached_agg_info(rel, CurrentMemoryContext);

	/*
	 * The outer loop iterates over all index pages except the metapage, in
	 * physical order (we hope the kernel will cooperate in providing
	 * read-ahead for speed).  It is critical that we visit all leaf pages,
	 * including ones added after we start the scan, else we might fail to
	 * delete some deletable tuples.  Hence, we must repeatedly check the
	 * relation length.  We must acquire the relation-extension lock while
	 * doing so to avoid a race condition: if someone else is extending the
	 * relation, there is a window where bufmgr/smgr have created a new
	 * all-zero page but it hasn't yet been write-locked by _abt_getbuf(). If
	 * we manage to scan such a page here, we'll improperly assume it can be
	 * recycled.  Taking the lock synchronizes things enough to prevent a
	 * problem: either num_pages won't include the new page, or _abt_getbuf
	 * already has write lock on the buffer and it will be fully initialized
	 * before we can examine it.  (See also vacuumlazy.c, which has the same
	 * issue.)	Also, we need not worry if a page is added immediately after
	 * we look; the page splitting code already has write-lock on the left
	 * page before it adds a right page, so we must already have processed any
	 * tuples due to be moved into such a page.
	 *
	 * We can skip locking for new or temp relations, however, since no one
	 * else could be accessing them.
	 */
	needLock = !RELATION_IS_LOCAL(rel);

	scanblkno = ABTREE_METAPAGE + 1;
	for (;;)
	{
		/* Get the current relation length */
		if (needLock)
			LockRelationForExtension(rel, ExclusiveLock);
		num_pages = RelationGetNumberOfBlocks(rel);
		if (needLock)
			UnlockRelationForExtension(rel, ExclusiveLock);

		if (info->report_progress)
			pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_TOTAL,
										 num_pages);

		/* Quit if we've scanned the whole relation */
		if (scanblkno >= num_pages)
			break;
		/* Iterate over pages, then loop back to recheck length */
		for (; scanblkno < num_pages; scanblkno++)
		{
			abtvacuumpage(&vstate, scanblkno);
			if (info->report_progress)
				pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
											 scanblkno);
		}
	}

	MemoryContextDelete(vstate.pagedelcontext);

	/*
	 * If we found any recyclable pages (and recorded them in the FSM), then
	 * forcibly update the upper-level FSM pages to ensure that searchers can
	 * find them.  It's possible that the pages were also found during
	 * previous scans and so this is a waste of time, but it's cheap enough
	 * relative to scanning the index that it shouldn't matter much, and
	 * making sure that free pages are available sooner not later seems
	 * worthwhile.
	 *
	 * Note that if no recyclable pages exist, we don't bother vacuuming the
	 * FSM at all.
	 */
	if (vstate.totFreePages > 0)
		IndexFreeSpaceMapVacuum(rel);

	/*
	 * Maintain the oldest .xact and a count of the current number of heap
	 * tuples in the metapage (for the benefit of _abt_vacuum_needs_cleanup).
	 *
	 * The page with the oldest .xact is typically a page deleted by this
	 * VACUUM operation, since pages deleted by a previous VACUUM operation
	 * tend to be placed in the FSM (by the current VACUUM operation) -- such
	 * pages are not candidates to be the oldest .xact.  (Note that pages
	 * placed in the FSM are reported as deleted pages in the bulk delete
	 * statistics, despite not counting as deleted pages for the purposes of
	 * determining the oldest .xact.)
	 */
	_abt_update_meta_cleanup_info(rel, vstate.oldestABtpoXact,
								 info->num_heap_tuples);

	/* update statistics */
	stats->num_pages = num_pages;
	stats->pages_free = vstate.totFreePages;

	/* free the local copy of agg info */
	pfree(vstate.agg_info);
}

/*
 * abtvacuumpage --- VACUUM one page
 *
 * This processes a single page for abtvacuumscan().  In some cases we must
 * backtrack to re-examine and VACUUM pages that were the scanblkno during
 * a previous call here.  This is how we handle page splits (that happened
 * after our cycleid was acquired) whose right half page happened to reuse
 * a block that we might have processed at some point before it was
 * recycled (i.e. before the page split).
 */
static void
abtvacuumpage(ABTVacState *vstate, BlockNumber scanblkno)
{
	IndexVacuumInfo *info = vstate->info;
	IndexBulkDeleteResult *stats = vstate->stats;
	IndexBulkDeleteCallback callback = vstate->callback;
	void	   *callback_state = vstate->callback_state;
	Relation	rel = info->index;
	bool		attempt_pagedel;
	BlockNumber blkno,
				backtrack_to;
	Buffer		buf;
	Page		page;
	ABTPageOpaque opaque;
	ABTCachedAggInfo agg_info = vstate->agg_info;

	blkno = scanblkno;

backtrack:

	attempt_pagedel = false;
	backtrack_to = ABTP_NONE;

	/* call vacuum_delay_point while not holding any buffer lock */
	vacuum_delay_point();

	/*
	 * We can't use _abt_getbuf() here because it always applies
	 * _abt_checkpage(), which will barf on an all-zero page. We want to
	 * recycle all-zero pages, not fail.  Also, we want to use a nondefault
	 * buffer access strategy.
	 */
	buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
							 info->strategy);
	LockBuffer(buf, ABT_READ);
	page = BufferGetPage(buf);
	opaque = NULL;
	if (!PageIsNew(page))
	{
		_abt_checkpage(rel, buf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	}

	Assert(blkno <= scanblkno);
	if (blkno != scanblkno)
	{
		/*
		 * We're backtracking.
		 *
		 * We followed a right link to a sibling leaf page (a page that
		 * happens to be from a block located before scanblkno).  The only
		 * case we want to do anything with is a live leaf page having the
		 * current vacuum cycle ID.
		 *
		 * The page had better be in a state that's consistent with what we
		 * expect.  Check for conditions that imply corruption in passing.  It
		 * can't be half-dead because only an interrupted VACUUM process can
		 * leave pages in that state, so we'd definitely have dealt with it
		 * back when the page was the scanblkno page (half-dead pages are
		 * always marked fully deleted by _abt_pagedel()).  This assumes that
		 * there can be only one vacuum process running at a time.
		 */
		if (!opaque || !ABT_P_ISLEAF(opaque) || ABT_P_ISHALFDEAD(opaque))
		{
			Assert(false);
			ereport(LOG,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg_internal("right sibling %u of scanblkno %u unexpectedly in an inconsistent state in index \"%s\"",
									 blkno, scanblkno, RelationGetRelationName(rel))));
			_abt_relbuf(rel, buf);
			return;
		}

		/*
		 * We may have already processed the page in an earlier call, when the
		 * page was scanblkno.  This happens when the leaf page split occurred
		 * after the scan began, but before the right sibling page became the
		 * scanblkno.
		 *
		 * Page may also have been deleted by current abtvacuumpage() call,
		 * since _abt_pagedel() sometimes deletes the right sibling page of
		 * scanblkno in passing (it does so after we decided where to
		 * backtrack to).  We don't need to process this page as a deleted
		 * page a second time now (in fact, it would be wrong to count it as a
		 * deleted page in the bulk delete statistics a second time).
		 */
		if (opaque->abtpo_cycleid != vstate->cycleid || ABT_P_ISDELETED(opaque))
		{
			/* Done with current scanblkno (and all lower split pages) */
			_abt_relbuf(rel, buf);
			return;
		}
	}

	/* Page is valid, see what to do with it */
	if (_abt_page_recyclable(page))
	{
		/* Okay to recycle this page (which could be leaf or internal) */
		RecordFreeIndexPage(rel, blkno);
		vstate->totFreePages++;
		stats->pages_deleted++;
	}
	else if (ABT_P_ISDELETED(opaque))
	{
		/*
		 * Already deleted page (which could be leaf or internal).  Can't
		 * recycle yet.
		 */
		stats->pages_deleted++;

		/* Maintain the oldest .xact */
		if (!TransactionIdIsValid(vstate->oldestABtpoXact) ||
			TransactionIdPrecedes(opaque->abtpo.xact, vstate->oldestABtpoXact))
			vstate->oldestABtpoXact = opaque->abtpo.xact;
	}
	else if (ABT_P_ISHALFDEAD(opaque))
	{
		/*
		 * Half-dead leaf page.  Try to delete now.  Might update
		 * oldestABtpoXact and pages_deleted below.
		 */
		attempt_pagedel = true;
	}
	else if (ABT_P_ISLEAF(opaque))
	{
		OffsetNumber deletable[MaxIndexTuplesPerPage];
		int			ndeletable;
		ABTVacuumPosting updatable[MaxIndexTuplesPerPage];
		int			nupdatable;
		OffsetNumber offnum,
					minoff,
					maxoff;
		int			nhtidsdead,
					nhtidslive;
        Datum       deleted_sum;
        bool        is_first_deleted;

		/*
		 * Trade in the initial read lock for a super-exclusive write lock on
		 * this page.  We must get such a lock on every leaf page over the
		 * course of the vacuum scan, whether or not it actually contains any
		 * deletable tuples --- see nbtree/README.
		 */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		LockBufferForCleanup(buf);

		/*
		 * Check whether we need to backtrack to earlier pages.  What we are
		 * concerned about is a page split that happened since we started the
		 * vacuum scan.  If the split moved tuples on the right half of the
		 * split (i.e. the tuples that sort high) to a block that we already
		 * passed over, then we might have missed the tuples.  We need to
		 * backtrack now.  (Must do this before possibly clearing _cycleid
		 * or deleting scanblkno page below!)
		 */
		if (vstate->cycleid != 0 &&
			opaque->abtpo_cycleid == vstate->cycleid &&
			!(opaque->abtpo_flags & ABTP_SPLIT_END) &&
			!ABT_P_RIGHTMOST(opaque) &&
			opaque->abtpo_next < scanblkno)
			backtrack_to = opaque->abtpo_next;

		/*
		 * When each VACUUM begins, it determines an OldestXmin cutoff value.
		 * Tuples before the cutoff are removed by VACUUM.  Scan over all
		 * items to see which ones need to be deleted according to cutoff
		 * point using callback.
		 */
		ndeletable = 0;
		nupdatable = 0;
		minoff = ABT_P_FIRSTDATAKEY(opaque);
		maxoff = PageGetMaxOffsetNumber(page);
		nhtidsdead = 0;
		nhtidslive = 0;
		if (callback)
		{
            is_first_deleted = true;
			for (offnum = minoff;
				 offnum <= maxoff;
				 offnum = OffsetNumberNext(offnum))
			{
				IndexTuple	itup;

				itup = (IndexTuple) PageGetItem(page,
												PageGetItemId(page, offnum));

				/*
				 * Hot Standby assumes that it's okay that XLOG_ABTREE_VACUUM
				 * records do not produce their own conflicts.  This is safe
				 * as long as the callback function only considers whether the
				 * index tuple refers to pre-cutoff heap tuples that were
				 * certainly already pruned away during VACUUM's initial heap
				 * scan by the time we get here. (XLOG_HEAP2_CLEANUP_INFO
				 * records produce conflicts using a latestRemovedXid value
				 * for the entire VACUUM, so there is no need to produce our
				 * own conflict now.)
				 *
				 * Backends with snapshots acquired after a VACUUM starts but
				 * before it finishes could have a RecentGlobalXmin with a
				 * later xid than the VACUUM's OldestXmin cutoff.  These
				 * backends might happen to opportunistically mark some index
				 * tuples LP_DEAD before we reach them, even though they may
				 * be after our cutoff.  We don't try to kill these "extra"
				 * index tuples in _abt_delitems_vacuum().  This keep things
				 * simple, and allows us to always avoid generating our own
				 * conflicts.
				 */
				Assert(!ABTreeTupleIsPivot(itup));
				if (!ABTreeTupleIsPosting(itup))
				{
					/* Regular tuple, standard table TID representation */
					if (callback(&itup->t_tid, callback_state))
					{
						deletable[ndeletable++] = offnum;
						nhtidsdead++;

                        _abt_accum_index_tuple_value(agg_info,
                                                     &deleted_sum,
                                                     itup,
                                                     /*is_leaf = */true,
                                                     is_first_deleted);
                        is_first_deleted = false;
					}
					else
						nhtidslive++;
				}
				else
				{
					ABTVacuumPosting vacposting;
					int			nremaining;

					/* Posting list tuple */
					vacposting = abtreevacuumposting(vstate, itup, offnum,
													&nremaining,
                                                    &deleted_sum,
                                                    &is_first_deleted);
					if (vacposting == NULL)
					{
						/*
						 * All table TIDs from the posting tuple remain, so no
						 * delete or update required
						 */
						Assert(nremaining == ABTreeTupleGetNPosting(itup));
					}
					else if (nremaining > 0)
					{

						/*
						 * Store metadata about posting list tuple in
						 * updatable array for entire page.  Existing tuple
						 * will be updated during the later call to
						 * _abt_delitems_vacuum().
						 */
						Assert(nremaining < ABTreeTupleGetNPosting(itup));
						updatable[nupdatable++] = vacposting;
						nhtidsdead += ABTreeTupleGetNPosting(itup) - nremaining;
					}
					else
					{
						/*
						 * All table TIDs from the posting list must be
						 * deleted.  We'll delete the index tuple completely
						 * (no update required).
						 */
						Assert(nremaining == 0);
						deletable[ndeletable++] = offnum;
						nhtidsdead += ABTreeTupleGetNPosting(itup);
						pfree(vacposting);
					}

					nhtidslive += nremaining;
				}
			}
		}

		/*
		 * Apply any needed deletes or updates.  We issue just one
		 * _abt_delitems_vacuum() call per page, so as to minimize WAL traffic.
		 */
		if (ndeletable > 0 || nupdatable > 0)
		{
			Assert(nhtidsdead >= ndeletable + nupdatable);
			_abt_delitems_vacuum(rel, buf, deletable, ndeletable, updatable,
								nupdatable, agg_info);

			stats->tuples_removed += nhtidsdead;
			/* must recompute maxoff */
			maxoff = PageGetMaxOffsetNumber(page);

			/* can't leak memory here */
			for (int i = 0; i < nupdatable; i++)
				pfree(updatable[i]);
    
            /* decrement the aggregations in the parent pages */
            _abt_decrement_aggs_for_vacuum(rel, buf, deleted_sum, agg_info);
		}
		else
		{
			/*
			 * If the leaf page has been split during this vacuum cycle, it
			 * seems worth expending a write to clear _cycleid even if we
			 * don't have any deletions to do.  (If we do, _abt_delitems_vacuum
			 * takes care of this.)  This ensures we won't process the page
			 * again.
			 *
			 * We treat this like a hint-bit update because there's no need to
			 * WAL-log it.
			 */
			Assert(nhtidsdead == 0);
			if (vstate->cycleid != 0 &&
				opaque->abtpo_cycleid == vstate->cycleid)
			{
				opaque->abtpo_cycleid = 0;
				MarkBufferDirtyHint(buf, true);
			}
		}

		/*
		 * If the leaf page is now empty, try to delete it; else count the
		 * live tuples (live table TIDs in posting lists are counted as
		 * separate live tuples).  We don't delete when backtracking, though,
		 * since that would require teaching _abt_pagedel() about backtracking
		 * (doesn't seem worth adding more complexity to deal with that).
		 *
		 * We don't count the number of live TIDs during cleanup-only calls to
		 * abtvacuumscan (i.e. when callback is not set).  We count the number
		 * of index tuples directly instead.  This avoids the expense of
		 * directly examining all of the tuples on each page.
		 */
		if (minoff > maxoff)
			attempt_pagedel = (blkno == scanblkno);
		else if (callback)
			stats->num_index_tuples += nhtidslive;
		else
			stats->num_index_tuples += maxoff - minoff + 1;

		Assert(!attempt_pagedel || nhtidslive == 0);
	}

	if (attempt_pagedel)
	{
		MemoryContext oldcontext;

		/* Run pagedel in a temp context to avoid memory leakage */
		MemoryContextReset(vstate->pagedelcontext);
		oldcontext = MemoryContextSwitchTo(vstate->pagedelcontext);

		/*
		 * We trust the _abt_pagedel return value because it does not include
		 * any page that a future call here from abtvacuumscan is expected to
		 * count.  There will be no double-counting.
		 */
		Assert(blkno == scanblkno);
		stats->pages_deleted += _abt_pagedel(rel, buf,
											&vstate->oldestABtpoXact,
											agg_info);

		MemoryContextSwitchTo(oldcontext);
		/* pagedel released buffer, so we shouldn't */
	}
	else
		_abt_relbuf(rel, buf);

	if (backtrack_to != ABTP_NONE)
	{
		blkno = backtrack_to;
		goto backtrack;
	}
}

/*
 * abtreevacuumposting --- determine TIDs still needed in posting list
 *
 * Returns metadata describing how to build replacement tuple without the TIDs
 * that VACUUM needs to delete.  Returned value is NULL in the common case
 * where no changes are needed to caller's posting list tuple (we avoid
 * allocating memory here as an optimization).
 *
 * The number of TIDs that should remain in the posting list tuple is set for
 * caller in *nremaining.
 */
static ABTVacuumPosting
abtreevacuumposting(ABTVacState *vstate, IndexTuple posting,
				   OffsetNumber updatedoffset, int *nremaining,
                   Datum *deleted_sum, bool *is_first_deleted)
{
	int			live = 0;
	int			nitem = ABTreeTupleGetNPosting(posting);
	ItemPointer items = ABTreeTupleGetPosting(posting);
	ABTVacuumPosting vacposting = NULL;
    ABTCachedAggInfo  agg_info = vstate->agg_info;
    Pointer     agg_arr;
    Pointer     agg_value_ptr;
	
	/* 
	 * We only need to accumulate the deleted_sum one item at a time if there's
	 * aggregation stored in the leaf node. Otherwise, we can add the number of
	 * dead tuples times the default aggregation value to the deleted_sum.
	 */
	if (agg_info->leaf_has_agg)
		agg_arr = ABTreePostingTupleGetAggPtr(posting, agg_info);

	for (int i = 0; i < nitem; i++)
	{
		if (!vstate->callback(items + i, vstate->callback_state))
		{
			/* Live table TID */
			live++;
		}
		else if (vacposting == NULL)
		{
			/*
			 * First dead table TID encountered.
			 *
			 * It's now clear that we need to delete one or more dead table
			 * TIDs, so start maintaining metadata describing how to update
			 * existing posting list tuple.
			 */
			vacposting = palloc(offsetof(ABTVacuumPostingData, deletetids) +
								nitem * sizeof(uint16));

			vacposting->itup = posting;
			vacposting->updatedoffset = updatedoffset;
			vacposting->ndeletedtids = 0;
			vacposting->deletetids[vacposting->ndeletedtids++] = i;
			
			if (agg_info->leaf_has_agg)
			{
				agg_value_ptr = agg_arr + agg_info->agg_stride * i;
				_abt_accum_value(agg_info, deleted_sum,
								 fetch_att(agg_value_ptr, agg_info->agg_byval,
										   agg_info->agg_typlen),
								 *is_first_deleted);
				*is_first_deleted = false;
			}
		}
		else
		{
			/* Second or subsequent dead table TID */
			vacposting->deletetids[vacposting->ndeletedtids++] = i;
	
			if (agg_info->leaf_has_agg)
			{
				agg_value_ptr = agg_arr + agg_info->agg_stride * i;
				_abt_accum_value(agg_info, deleted_sum,
								 fetch_att(agg_value_ptr, agg_info->agg_byval,
										   agg_info->agg_typlen),
								 false);
			}
		}
	}

	if (!agg_info->leaf_has_agg && live < nitem)
	{
		Datum val_to_add = ABTreeComputeProductOfAggAndInt2(
			agg_info, agg_info->agg_map_val, nitem - live);
		_abt_accum_value(agg_info, deleted_sum, val_to_add, *is_first_deleted);
		*is_first_deleted = false;
	}

	*nremaining = live;
	return vacposting;
}

/*
 *	btcanreturn() -- Check whether btree indexes support index-only scans.
 *
 * btrees always do, so this is trivial.
 */
bool
abtcanreturn(Relation index, int attno)
{
	return true;
}

void
ABTreeRegisterMetricsReporter(void)
{
    BackgroundWorker    bgw;

    if (!abtree_enable_metrics_collection)
        return ;

    MemSet(&bgw, 0, sizeof(bgw));
    snprintf(bgw.bgw_name, BGW_MAXLEN, "abtree metrics reporter");
    snprintf(bgw.bgw_type, BGW_MAXLEN, "abt_metrics_reporter");
    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;

    bgw.bgw_start_time = BgWorkerStart_ConsistentState;
    bgw.bgw_restart_time = 10;
    snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
    snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ABTreeMetricsReporterMain");
    bgw.bgw_main_arg = Int32GetDatum(0);
    bgw.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&bgw);
}

static void
_abt_error_loop(void)
{
    ereport(WARNING,
        errmsg("abtree metrics reporter disabled"));
    while (!ShutdownRequestPending)
    {
        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                         0,
                         WAIT_EVENT_PG_SLEEP);
        ResetLatch(MyLatch);
    }
    proc_exit(0);
}

static void
_abt_metrics_reporter_cleanup_port(int status, Datum arg)
{
    if (_abt_conn_fd >= 0)
    {
        close(_abt_conn_fd);
        _abt_conn_fd = -1;
    }

    if (_abt_svc_fd >= 0)
    {
        close(_abt_svc_fd);
        _abt_conn_fd = -1;
    }
}

void
ABTreeMetricsReporterMain(Datum main_arg)
{
    struct sockaddr_in svc_addr;
    StringInfo strout;

	/*
	 * Set up signal handlers. We only need to respond to shutdown requests.
	 */
	pqsignal(SIGHUP, SIG_IGN); /* no config reload allowed */
	pqsignal(SIGINT, SIG_IGN); /* nothing to cancel */
    /* We don't want SA_RESTART for SIGTERM so that we can break out of a
     * blocking accept() call. pqsignal_pm does not set that unless the signal
     * handler is default or ignore. */
	pqsignal_pm(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts();
	/* other signals (SIGPIPE, SIGUSR1, SIGCHLD and SIGUSR2) are standard */
	BackgroundWorkerUnblockSignals();
    
    on_proc_exit(_abt_metrics_reporter_cleanup_port, 0);

    strout = makeStringInfo(); 

	svc_addr.sin_family = AF_INET;
    svc_addr.sin_port = htons(abtree_metrics_reporter_port);
    svc_addr.sin_addr.s_addr = INADDR_ANY;
    MemSet(svc_addr.sin_zero, '\0', 8);

	// create a socket. // is setting socket options necessary?
    _abt_svc_fd = socket(AF_INET, SOCK_STREAM, 0);
    if(_abt_svc_fd < 0)
    {
        int errno_ = errno;
        ereport(WARNING,
            errmsg("failed to create a socket: %s", strerror(errno_)));
        _abt_error_loop();
    }

 
	if(bind(_abt_svc_fd,
            (struct sockaddr*) &svc_addr,
            sizeof(struct sockaddr_in)) < 0) 
	{
        int errno_ = errno;
        elog(WARNING, "failed to bind socket to port %d: %s",
                      abtree_metrics_reporter_port, strerror(errno_));
        close(_abt_svc_fd);
        _abt_svc_fd = -1;
        _abt_error_loop();
	}

	// lets listen for incoming connections
    if(listen(_abt_svc_fd, 1) < 0) {
        int errno_ = errno;
        elog(WARNING, "failed to listen on socket: %s", strerror(errno_));
        close(_abt_svc_fd);
        _abt_svc_fd = -1;
        _abt_error_loop();
    }

    while (!ShutdownRequestPending)
    {
        uint64 ninserts = 0,
               nsamples_accepted = 0,
               nsamples_rejected = 0;
        int i;
        ssize_t sz;
		struct timeval tp;
        int64 usecs_since_epoch = 0;
        char *data;
        int len;

		if(_abt_conn_fd < 0) {
            _abt_conn_fd = accept(_abt_svc_fd, NULL, NULL);
            if (_abt_conn_fd < 0) {
                int errno_ = errno;
                if (errno_ == EINTR || errno_ == ECONNABORTED)
                {
                    continue;
                }
                elog(WARNING, "failed to accept connection: %s",
                              strerror(errno_));
                close(_abt_svc_fd);
                _abt_error_loop();
            }
            ResetLatch(MyLatch);
        }

        gettimeofday(&tp, NULL);
        usecs_since_epoch =
            ((int64_t) tp.tv_sec) * ((int64_t) 1000000) +
            ((int64_t) tp.tv_usec);
        

        for (i = 0; i < (int) MaxBackends; ++i)
        {
            ninserts += pg_atomic_read_u64(&abtree_ninserts[i]); 
            nsamples_accepted +=
                pg_atomic_read_u64(&abtree_nsamples_accepted[i]);
            nsamples_rejected +=
                pg_atomic_read_u64(&abtree_nsamples_rejected[i]);
        }
        /* TODO replace this with socket write */
        /*elog(LOG, "abtree: %lu, %lu, %lu", ninserts, nsamples_accepted,
                                   nsamples_rejected); */
        resetStringInfo(strout);
        appendStringInfo(strout, "[%ld, %lu, %lu, %lu]\n",
                         usecs_since_epoch,
                         ninserts,
                         nsamples_accepted,
                         nsamples_rejected);
        data = strout->data;
        len = strout->len;
        
        while (len > 0)
        {
            sz = send(_abt_conn_fd, strout->data, strout->len, MSG_NOSIGNAL);
            if (sz == -1)
            {
                int errno_ =  errno;
                if (errno_ == EINTR)
                {
                    /* try again */
                    continue;
                }
                
                if (errno_ == EPIPE || errno_ == ECONNRESET)
                {
                    close(_abt_conn_fd);
                    _abt_conn_fd = -1;
                    break;
                }
            
                elog(WARNING, "unexpected error from send(): %s",
                              strerror(errno_));
                close(_abt_conn_fd);
                _abt_conn_fd = -1;
                _abt_error_loop();
            }
            else
            {
                /* only managed to sent part of the data; try sending the rest
                 * to the remote peer */
                data += sz;
                len -= sz;
            }
        }

        if (_abt_conn_fd == -1)
            continue;

        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         abtree_metrics_reporter_delay,
                         WAIT_EVENT_PG_SLEEP);
        ResetLatch(MyLatch);
    }

    proc_exit(0);
}

