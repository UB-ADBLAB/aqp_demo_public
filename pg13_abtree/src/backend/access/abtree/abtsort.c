/*-------------------------------------------------------------------------
 *
 * abtsort.c
 *		Build a btree from sorted input by loading leaf pages sequentially.
 *
 *		Copied and adapted from src/backend/access/nbtree/nbtsort.c.
 *
 * NOTES
 *
 * We use tuplesort.c to sort the given index tuples into order.
 * Then we scan the index tuples in order and build the btree pages
 * for each level.  We load source tuples into leaf-level pages.
 * Whenever we fill a page at one level, we add a link to it to its
 * parent level (starting a new parent level if necessary).  When
 * done, we write out each final page on each level, adding it to
 * its parent level.  When we have only one page on a level, it must be
 * the root -- it can be attached to the btree metapage and we are done.
 *
 * It is not wise to pack the pages entirely full, since then *any*
 * insertion would cause a split (and not only of the leaf page; the need
 * for a split would cascade right up the tree).  The steady-state load
 * factor for btrees is usually estimated at 70%.  We choose to pack leaf
 * pages to the user-controllable fill factor (default 90%) while upper pages
 * are always packed to 70%.  This gives us reasonable density (there aren't
 * many upper pages if the keys are reasonable-size) without risking a lot of
 * cascading splits during early insertions.
 *
 * Formerly the index pages being built were kept in shared buffers, but
 * that is of no value (since other backends have no interest in them yet)
 * and it created locking problems for CHECKPOINT, because the upper-level
 * pages were held exclusive-locked for long periods.  Now we just build
 * the pages in local memory and smgrwrite or smgrextend them as we finish
 * them.  They will need to be re-read into shared buffers on first use after
 * the build finishes.
 *
 * This code isn't concerned about the FSM at all. The caller is responsible
 * for initializing that.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/abtree/abtsort.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/abtree.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "commands/progress.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"		/* pgrminclude ignore */
#include "utils/rel.h"
#include "utils/sortsupport.h"
#include "utils/tuplesort.h"


/* Magic numbers for parallel state sharing */
#define PARALLEL_KEY_ABTREE_SHARED		UINT64CONST(0xA000000000000001)
#define PARALLEL_KEY_TUPLESORT			UINT64CONST(0xA000000000000002)
#define PARALLEL_KEY_TUPLESORT_SPOOL2	UINT64CONST(0xA000000000000003)
#define PARALLEL_KEY_QUERY_TEXT			UINT64CONST(0xA000000000000004)
#define PARALLEL_KEY_WAL_USAGE			UINT64CONST(0xA000000000000005)
#define PARALLEL_KEY_BUFFER_USAGE		UINT64CONST(0xA000000000000006)

/*
 * DISABLE_LEADER_PARTICIPATION disables the leader's participation in
 * parallel index builds.  This may be useful as a debugging aid.
#undef DISABLE_LEADER_PARTICIPATION
 */

/*
 * Status record for spooling/sorting phase.  (Note we may have two of
 * these due to the special requirements for uniqueness-checking with
 * dead tuples.)
 */
typedef struct ABTSpool
{
	Tuplesortstate *sortstate;	/* state data for tuplesort.c */
	Relation	heap;
	Relation	index;
	bool		isunique;
} ABTSpool;

/*
 * Status for index builds performed in parallel.  This is allocated in a
 * dynamic shared memory segment.  Note that there is a separate tuplesort TOC
 * entry, private to tuplesort.c but allocated by this module on its behalf.
 */
typedef struct ABTShared
{
	/*
	 * These fields are not modified during the sort.  They primarily exist
	 * for the benefit of worker processes that need to create ABTSpool state
	 * corresponding to that used by the leader.
	 */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isunique;
	bool		isconcurrent;
	int			scantuplesortstates;

	/*
	 * workersdonecv is used to monitor the progress of workers.  All parallel
	 * participants must indicate that they are done before leader can use
	 * mutable state that workers maintain during scan (and before leader can
	 * proceed to tuplesort_performsort()).
	 */
	ConditionVariable workersdonecv;

	/*
	 * mutex protects all fields before heapdesc.
	 *
	 * These fields contain status information of interest to B-Tree index
	 * builds that must work just the same when an index is built in parallel.
	 */
	slock_t		mutex;

	/*
	 * Mutable state that is maintained by workers, and reported back to
	 * leader at end of parallel scan.
	 *
	 * nparticipantsdone is number of worker processes finished.
	 *
	 * reltuples is the total number of input heap tuples.
	 *
	 * havedead indicates if RECENTLY_DEAD tuples were encountered during
	 * build.
	 *
	 * indtuples is the total number of tuples that made it into the index.
	 *
	 * brokenhotchain indicates if any worker detected a broken HOT chain
	 * during build.
	 */
	int			nparticipantsdone;
	double		reltuples;
	bool		havedead;
	double		indtuples;
	bool		brokenhotchain;

	/*
	 * ParallelTableScanDescData data follows. Can't directly embed here, as
	 * implementations of the parallel table scan desc interface might need
	 * stronger alignment.
	 */
} ABTShared;

/*
 * Return pointer to a ABTShared's parallel table scan.
 *
 * c.f. shm_toc_allocate as to why BUFFERALIGN is used, rather than just
 * MAXALIGN.
 */
#define ParallelTableScanFromABTShared(shared) \
	(ParallelTableScanDesc) ((char *) (shared) + BUFFERALIGN(sizeof(ABTShared)))

/*
 * Status for leader in parallel index build.
 */
typedef struct ABTLeader
{
	/* parallel context itself */
	ParallelContext *pcxt;

	/*
	 * nparticipanttuplesorts is the exact number of worker processes
	 * successfully launched, plus one leader process if it participates as a
	 * worker (only DISABLE_LEADER_PARTICIPATION builds avoid leader
	 * participating as a worker).
	 */
	int			nparticipanttuplesorts;

	/*
	 * Leader process convenience pointers to shared state (leader avoids TOC
	 * lookups).
	 *
	 * btshared is the shared state for entire build.  sharedsort is the
	 * shared, tuplesort-managed state passed to each process tuplesort.
	 * sharedsort2 is the corresponding btspool2 shared state, used only when
	 * building unique indexes.  snapshot is the snapshot used by the scan iff
	 * an MVCC snapshot is required.
	 */
	ABTShared   *btshared;
	Sharedsort *sharedsort;
	Sharedsort *sharedsort2;
	Snapshot	snapshot;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
} ABTLeader;

/*
 * Working state for btbuild and its callback.
 *
 * When parallel CREATE INDEX is used, there is a ABTBuildState for each
 * participant.
 */
typedef struct ABTBuildState
{
	bool		isunique;
	bool		havedead;
	Relation	heap;
	ABTSpool    *spool;

	/*
	 * spool2 is needed only when the index is a unique index. Dead tuples are
	 * put into spool2 instead of spool in order to avoid uniqueness check.
	 */
	ABTSpool    *spool2;
	double		indtuples;

	/*
	 * btleader is only present when a parallel index build is performed, and
	 * only in the leader process. (Actually, only the leader has a
	 * ABTBuildState.  Workers have their own spool and spool2, though.)
	 */
	ABTLeader   *btleader;

	ABTCachedAggInfo	agg_info;
} ABTBuildState;

/*
 * Status record for a btree page being built.  We have one of these
 * for each active tree level.
 */
typedef struct ABTPageState
{
	Page		abtps_page;		/* workspace for page building */
	BlockNumber abtps_blkno;		/* block # to write this page at */
	IndexTuple	abtps_lowkey;	/* page's strict lower bound pivot tuple */
	OffsetNumber abtps_lastoff;	/* last item offset loaded */
	Size		abtps_lastextra; /* last item's extra posting list space */
	uint32		abtps_level;		/* tree level (0 = leaf) */
	Size		abtps_full;		/* "full" if less than this much free space */
	struct ABTPageState *abtps_next;	/* link to parent level, if any */
	
	/*
	 * The accumulated aggregation value for the current page except for
	 * the last inserted item.
	 */
	Datum		abtps_agg;
} ABTPageState;

/*
 * Overall status record for index writing phase.
 */
typedef struct ABTWriteState
{
	Relation	heap;
	Relation	index;
	ABTScanInsert inskey;		/* generic insertion scankey */
	bool		btws_use_wal;	/* dump pages to WAL? */
	BlockNumber btws_pages_alloced; /* # pages allocated */
	BlockNumber btws_pages_written; /* # pages written out */
	Page		btws_zeropage;	/* workspace for filling zeroes */
} ABTWriteState;


static double _abt_spools_heapscan(Relation heap, Relation index,
								  ABTBuildState *buildstate, IndexInfo *indexInfo);
static void _abt_spooldestroy(ABTSpool *btspool);
static void _abt_spool(ABTCachedAggInfo agg_info,
					   ABTSpool *btspool, ItemPointer self,
					   Datum *values, bool *isnull);
static void _abt_leafbuild(ABTBuildState *state);
static void _abt_build_callback(Relation index, ItemPointer tid, Datum *values,
							   bool *isnull, bool tupleIsAlive, void *state);
static Page _abt_blnewpage(uint32 level);
static ABTPageState *_abt_pagestate(ABTWriteState *wstate, uint32 level,
									ABTCachedAggInfo agg_info);
static void _abt_slideleft(Page page);
static void _abt_sortaddtup(Page page, Size itemsize,
						   IndexTuple itup, OffsetNumber itup_off,
						   bool newfirstdataitem,
						   ABTCachedAggInfo agg_info);
static void _abt_buildadd(ABTWriteState *wstate,
						  ABTPageState *state,
						  IndexTuple itup, Size truncextra,
						  ABTCachedAggInfo agg_info);
static void _abt_sort_dedup_finish_pending(ABTWriteState *wstate,
										   ABTPageState *state,
										   ABTDedupState dstate,
										   ABTCachedAggInfo agg_info);
static void _abt_uppershutdown(ABTWriteState *wstate,
							   ABTPageState *state, ABTCachedAggInfo);
static void _abt_load(ABTBuildState *buildstate, ABTWriteState *wstate);
static void _abt_begin_parallel(ABTBuildState *buildstate, bool isconcurrent,
							   int request);
static void _abt_end_parallel(ABTLeader *btleader);
static Size _abt_parallel_estimate_shared(Relation heap, Snapshot snapshot);
static double _abt_parallel_heapscan(ABTBuildState *buildstate,
									bool *brokenhotchain);
static void _abt_leader_participate_as_worker(ABTBuildState *buildstate);
static void _abt_parallel_scan_and_sort(ABTSpool *btspool, ABTSpool *btspool2,
									   ABTShared *btshared, Sharedsort *sharedsort,
									   Sharedsort *sharedsort2, int sortmem,
									   bool progress);
static void _abt_accum_last_tuple_on_page(ABTPageState *page_state,
										  OffsetNumber last_off,
										  ABTCachedAggInfo agg_info);

/*
 *	btbuild() -- build a new btree index.
 */
IndexBuildResult *
abtbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	ABTBuildState buildstate;
	double		reltuples;
	ABTOptions		*reloptions;
	Datum			agg_support_datum;
	ABTAggSupport	agg_support;
	ABTCachedAggInfo	agg_info;

#ifdef ABTREE_BUILD_STATS
	if (log_btree_build_stats)
		ResetUsage();
#endif							/* ABTREE_BUILD_STATS */

	buildstate.isunique = indexInfo->ii_Unique;
	buildstate.havedead = false;
	buildstate.heap = heap;
	buildstate.spool = NULL;
	buildstate.spool2 = NULL;
	buildstate.indtuples = 0;
	buildstate.btleader = NULL;
	
	/* 
	 * Get the agg support struct. Here we keep a private copy of agg_support
	 * in the buildstate until we are ready to write the metapage. It needs
	 * to be freed at the end.
	 */
	reloptions = (ABTOptions *) index->rd_options;
	agg_support_datum = OidFunctionCall1(reloptions->agg_support_fn,
		ObjectIdGetDatum(reloptions->aggregation_type.typeOid));
	agg_support = (ABTAggSupport) DatumGetPointer(agg_support_datum);
	Assert(agg_support);
	
	/* 
	 * Set up the cached agg info here.
	 */
	agg_info = palloc(ABTGetCachedAggInfoSize(agg_support));
	_abt_fill_agg_info(agg_info, agg_support);
	buildstate.agg_info = agg_info;
	pfree(agg_support);

	/*
	 * We expect to be called exactly once for any index relation. If that's
	 * not the case, big trouble's what we have.
	 */
	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	reltuples = _abt_spools_heapscan(heap, index, &buildstate, indexInfo);

	/*
	 * Finish the build by (1) completing the sort of the spool file, (2)
	 * inserting the sorted tuples into btree pages and (3) building the upper
	 * levels.  Finally, it may also be necessary to end use of parallelism.
	 */
	_abt_leafbuild(&buildstate);
	_abt_spooldestroy(buildstate.spool);
	if (buildstate.spool2)
		_abt_spooldestroy(buildstate.spool2);
	if (buildstate.btleader)
		_abt_end_parallel(buildstate.btleader);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

#ifdef ABTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("ABTREE BUILD STATS");
		ResetUsage();
	}
#endif							/* ABTREE_BUILD_STATS */

	return result;
}

/*
 * Create and initialize one or two spool structures, and save them in caller's
 * buildstate argument.  May also fill-in fields within indexInfo used by index
 * builds.
 *
 * Scans the heap, possibly in parallel, filling spools with IndexTuples.  This
 * routine encapsulates all aspects of managing parallelism.  Caller need only
 * call _abt_end_parallel() in parallel case after it is done with spool/spool2.
 *
 * Returns the total number of heap tuples scanned.
 */
static double
_abt_spools_heapscan(Relation heap, Relation index, ABTBuildState *buildstate,
					IndexInfo *indexInfo)
{
	ABTSpool    *btspool = (ABTSpool *) palloc0(sizeof(ABTSpool));
	SortCoordinate coordinate = NULL;
	double		reltuples = 0;

	/*
	 * We size the sort area as maintenance_work_mem rather than work_mem to
	 * speed index creation.  This should be OK since a single backend can't
	 * run multiple index creations in parallel (see also: notes on
	 * parallelism and maintenance_work_mem below).
	 */
	btspool->heap = heap;
	btspool->index = index;
	btspool->isunique = indexInfo->ii_Unique;

	/* Save as primary spool */
	buildstate->spool = btspool;

	/* Report table scan phase started */
	pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
								 PROGRESS_ABTREE_PHASE_INDEXBUILD_TABLESCAN);

	/* Attempt to launch parallel worker scan when required */
	if (indexInfo->ii_ParallelWorkers > 0)
		_abt_begin_parallel(buildstate, indexInfo->ii_Concurrent,
						   indexInfo->ii_ParallelWorkers);

	/*
	 * If parallel build requested and at least one worker process was
	 * successfully launched, set up coordination state
	 */
	if (buildstate->btleader)
	{
		coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
		coordinate->isWorker = false;
		coordinate->nParticipants =
			buildstate->btleader->nparticipanttuplesorts;
		coordinate->sharedsort = buildstate->btleader->sharedsort;
	}

	/*
	 * Begin serial/leader tuplesort.
	 *
	 * In cases where parallelism is involved, the leader receives the same
	 * share of maintenance_work_mem as a serial sort (it is generally treated
	 * in the same way as a serial sort once we return).  Parallel worker
	 * Tuplesortstates will have received only a fraction of
	 * maintenance_work_mem, though.
	 *
	 * We rely on the lifetime of the Leader Tuplesortstate almost not
	 * overlapping with any worker Tuplesortstate's lifetime.  There may be
	 * some small overlap, but that's okay because we rely on leader
	 * Tuplesortstate only allocating a small, fixed amount of memory here.
	 * When its tuplesort_performsort() is called (by our caller), and
	 * significant amounts of memory are likely to be used, all workers must
	 * have already freed almost all memory held by their Tuplesortstates
	 * (they are about to go away completely, too).  The overall effect is
	 * that maintenance_work_mem always represents an absolute high watermark
	 * on the amount of memory used by a CREATE INDEX operation, regardless of
	 * the use of parallelism or any other factor.
	 */
	buildstate->spool->sortstate =
		tuplesort_begin_index_btree(heap, index, buildstate->isunique,
									maintenance_work_mem, coordinate,
									false);

	/*
	 * If building a unique index, put dead tuples in a second spool to keep
	 * them out of the uniqueness check.  We expect that the second spool (for
	 * dead tuples) won't get very full, so we give it only work_mem.
	 */
	if (indexInfo->ii_Unique)
	{
		ABTSpool    *btspool2 = (ABTSpool *) palloc0(sizeof(ABTSpool));
		SortCoordinate coordinate2 = NULL;

		/* Initialize secondary spool */
		btspool2->heap = heap;
		btspool2->index = index;
		btspool2->isunique = false;
		/* Save as secondary spool */
		buildstate->spool2 = btspool2;

		if (buildstate->btleader)
		{
			/*
			 * Set up non-private state that is passed to
			 * tuplesort_begin_index_btree() about the basic high level
			 * coordination of a parallel sort.
			 */
			coordinate2 = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
			coordinate2->isWorker = false;
			coordinate2->nParticipants =
				buildstate->btleader->nparticipanttuplesorts;
			coordinate2->sharedsort = buildstate->btleader->sharedsort2;
		}

		/*
		 * We expect that the second one (for dead tuples) won't get very
		 * full, so we give it only work_mem
		 */
		buildstate->spool2->sortstate =
			tuplesort_begin_index_btree(heap, index, false, work_mem,
										coordinate2, false);
	}

	/* Fill spool using either serial or parallel heap scan */
	if (!buildstate->btleader)
		reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
										   _abt_build_callback, (void *) buildstate,
										   NULL);
	else
		reltuples = _abt_parallel_heapscan(buildstate,
										  &indexInfo->ii_BrokenHotChain);

	/*
	 * Set the progress target for the next phase.  Reset the block number
	 * values set by table_index_build_scan
	 */
	{
		const int	index[] = {
			PROGRESS_CREATEIDX_TUPLES_TOTAL,
			PROGRESS_SCAN_BLOCKS_TOTAL,
			PROGRESS_SCAN_BLOCKS_DONE
		};
		const int64 val[] = {
			buildstate->indtuples,
			0, 0
		};

		pgstat_progress_update_multi_param(3, index, val);
	}

	/* okay, all heap tuples are spooled */
	if (buildstate->spool2 && !buildstate->havedead)
	{
		/* spool2 turns out to be unnecessary */
		_abt_spooldestroy(buildstate->spool2);
		buildstate->spool2 = NULL;
	}

	return reltuples;
}

/*
 * clean up a spool structure and its substructures.
 */
static void
_abt_spooldestroy(ABTSpool *btspool)
{
	tuplesort_end(btspool->sortstate);
	pfree(btspool);
}

/*
 * spool an index entry into the sort file.
 */
static void
_abt_spool(ABTCachedAggInfo agg_info,
		   ABTSpool *btspool, ItemPointer self, Datum *values, bool *isnull)
{
	tuplesort_putindextuplevalues_with_extra(
		btspool->sortstate, btspool->index,
		self, values, isnull, agg_info->extra_space_for_nonpivot_tuple,
		'm');
}

/*
 * given a spool loaded by successive calls to _abt_spool,
 * create an entire btree.
 */
static void
_abt_leafbuild(ABTBuildState *buildstate)
{
	ABTWriteState wstate;
	ABTSpool *btspool = buildstate->spool;
	ABTSpool *btspool2 = buildstate->spool2;

#ifdef ABTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("ABTREE BUILD (Spool) STATISTICS");
		ResetUsage();
	}
#endif							/* ABTREE_BUILD_STATS */

	pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
								 PROGRESS_ABTREE_PHASE_PERFORMSORT_1);
	tuplesort_performsort(btspool->sortstate);
	if (btspool2)
	{
		pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
									 PROGRESS_ABTREE_PHASE_PERFORMSORT_2);
		tuplesort_performsort(btspool2->sortstate);
	}

	wstate.heap = btspool->heap;
	wstate.index = btspool->index;
	wstate.inskey = _abt_mkscankey(wstate.index, NULL);
	/* _abt_mkscankey() won't set allequalimage without metapage */
	wstate.inskey->allequalimage = _abt_allequalimage(wstate.index, true);
	wstate.btws_use_wal = RelationNeedsWAL(wstate.index);

	/* reserve the metapage */
	wstate.btws_pages_alloced = ABTREE_METAPAGE + 1;
	wstate.btws_pages_written = 0;
	wstate.btws_zeropage = NULL;	/* until needed */

	pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
								 PROGRESS_ABTREE_PHASE_LEAF_LOAD);
	_abt_load(buildstate, &wstate);
}

/*
 * Per-tuple callback for table_index_build_scan
 */
static void
_abt_build_callback(Relation index,
				   ItemPointer tid,
				   Datum *values,
				   bool *isnull,
				   bool tupleIsAlive,
				   void *state)
{
	ABTBuildState *buildstate = (ABTBuildState *) state;

	/*
	 * insert the index tuple into the appropriate spool file for subsequent
	 * processing
	 */
	if (tupleIsAlive || buildstate->spool2 == NULL)
		_abt_spool(buildstate->agg_info,
				   buildstate->spool, tid, values, isnull);
	else
	{
		/* dead tuples are put into spool2 */
		buildstate->havedead = true;
		_abt_spool(buildstate->agg_info,
				   buildstate->spool2, tid, values, isnull);
	}

	buildstate->indtuples += 1;
}

/*
 * allocate workspace for a new, clean btree page, not linked to any siblings.
 */
static Page
_abt_blnewpage(uint32 level)
{
	Page		page;
	ABTPageOpaque opaque;

	page = (Page) palloc(BLCKSZ);

	/* Zero the page and set up standard page header info */
	_abt_pageinit(page, BLCKSZ);

	/* Initialize ABT opaque state */
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	opaque->abtpo_prev = opaque->abtpo_next = ABTP_NONE;
	opaque->abtpo.level = level;
	opaque->abtpo_flags = (level > 0) ? 0 : ABTP_LEAF;
	opaque->abtpo_cycleid = 0;
	opaque->abtpo_last_update_id = ABT_MIN_LAST_UPDATE_ID;

	/* Make the ABTP_HIKEY line pointer appear allocated */
	((PageHeader) page)->pd_lower += sizeof(ItemIdData);

	return page;
}

/*
 * emit a completed btree page, and release the working storage.
 */
static void
_abt_blwritepage(ABTWriteState *wstate, Page page, BlockNumber blkno)
{
	/* Ensure rd_smgr is open (could have been closed by relcache flush!) */
	RelationOpenSmgr(wstate->index);

	/* XLOG stuff */
	if (wstate->btws_use_wal)
	{
		/* We use the XLOG_FPI record type for this */
		log_newpage(&wstate->index->rd_node, MAIN_FORKNUM, blkno, page, true);
	}

	/*
	 * If we have to write pages nonsequentially, fill in the space with
	 * zeroes until we come back and overwrite.  This is not logically
	 * necessary on standard Unix filesystems (unwritten space will read as
	 * zeroes anyway), but it should help to avoid fragmentation. The dummy
	 * pages aren't WAL-logged though.
	 */
	while (blkno > wstate->btws_pages_written)
	{
		if (!wstate->btws_zeropage)
			wstate->btws_zeropage = (Page) palloc0(BLCKSZ);
		/* don't set checksum for all-zero page */
		smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM,
				   wstate->btws_pages_written++,
				   (char *) wstate->btws_zeropage,
				   true);
	}

	PageSetChecksumInplace(page, blkno);

	/*
	 * Now write the page.  There's no need for smgr to schedule an fsync for
	 * this write; we'll do it ourselves before ending the build.
	 */
	if (blkno == wstate->btws_pages_written)
	{
		/* extending the file... */
		smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, blkno,
				   (char *) page, true);
		wstate->btws_pages_written++;
	}
	else
	{
		/* overwriting a block we zero-filled before */
		smgrwrite(wstate->index->rd_smgr, MAIN_FORKNUM, blkno,
				  (char *) page, true);
	}

	pfree(page);
}

/*
 * allocate and initialize a new ABTPageState.  the returned structure
 * is suitable for immediate use by _abt_buildadd.
 */
static ABTPageState *
_abt_pagestate(ABTWriteState *wstate, uint32 level, ABTCachedAggInfo agg_info)
{
	ABTPageState *state = (ABTPageState *) palloc0(sizeof(ABTPageState));

	/* create initial page for level */
	state->abtps_page = _abt_blnewpage(level);

	/* and assign it a page position */
	state->abtps_blkno = wstate->btws_pages_alloced++;

	state->abtps_lowkey = NULL;
	/* initialize lastoff so first item goes into ABTP_FIRSTKEY */
	state->abtps_lastoff = ABTP_HIKEY;
	state->abtps_lastextra = 0;
	state->abtps_level = level;
	/* set "full" threshold based on level.  See notes at head of file. */
	if (level > 0)
		state->abtps_full = (BLCKSZ * (100 - ABTREE_NONLEAF_FILLFACTOR) / 100);
	else
		state->abtps_full = ABTGetTargetPageFreeSpace(wstate->index);

	/* no parent level, yet */
	state->abtps_next = NULL;

	/* 
	 * no need to init abtps_agg here. It will be init'd upon first call
	 * to _abt_accum_last_tuple_on_page() with a lastoff != ABTP_HIKEY.
	 */

	return state;
}

/*
 * slide an array of ItemIds back one slot (from ABTP_FIRSTKEY to
 * ABTP_HIKEY, overwriting ABTP_HIKEY).  we need to do this when we discover
 * that we have built an ItemId array in what has turned out to be a
 * ABTP_RIGHTMOST page.
 */
static void
_abt_slideleft(Page page)
{
	OffsetNumber off;
	OffsetNumber maxoff;
	ItemId		previi;
	ItemId		thisii;

	if (!PageIsEmpty(page))
	{
		maxoff = PageGetMaxOffsetNumber(page);
		previi = PageGetItemId(page, ABTP_HIKEY);
		for (off = ABTP_FIRSTKEY; off <= maxoff; off = OffsetNumberNext(off))
		{
			thisii = PageGetItemId(page, off);
			*previi = *thisii;
			previi = thisii;
		}
		((PageHeader) page)->pd_lower -= sizeof(ItemIdData);
	}
}

/*
 * Add an item to a page being built.
 *
 * This is very similar to nbtinsert.c's _abt_pgaddtup(), but this variant
 * raises an error directly.
 *
 * Note that our nbtsort.c caller does not know yet if the page will be
 * rightmost.  Offset ABTP_FIRSTKEY is always assumed to be the first data key
 * by caller.  Page that turns out to be the rightmost on its level is fixed by
 * calling _abt_slideleft().
 *
 * newfirstdataitem == true if it is the first pivot tuple on the page and will
 * cause all keys in the tuple being truncated.
 */
static void
_abt_sortaddtup(Page page,
			   Size itemsize,
			   IndexTuple itup,
			   OffsetNumber itup_off,
			   bool newfirstdataitem,
			   ABTCachedAggInfo agg_info)
{
	IndexTuple truncated;

	if (newfirstdataitem)
	{
		/* We still need to save the aggregation in the pivot tuple. */
		itemsize = MAXALIGN(sizeof(IndexTupleData)) +
			agg_info->extra_space_when_tid_missing;
		
		truncated = palloc0(itemsize);
		memcpy(truncated, itup, sizeof(IndexTupleData));
		truncated->t_info = itemsize;
		ABTreeTupleSetNAtts(truncated, 0, false);
		memcpy(ABTreePivotTupleGetAggPtr(truncated, agg_info),
			   ABTreePivotTupleGetAggPtr(itup, agg_info),
			   agg_info->agg_typlen);

		itup = truncated;
	}

	if (PageAddItem(page, (Item) itup, itemsize, itup_off,
					false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add item to the index page");

	if (newfirstdataitem)
	{
		pfree(truncated);
	}
}

/*----------
 * Add an item to a disk page from the sort output (or add a posting list
 * item formed from the sort output).
 *
 * We must be careful to observe the page layout conventions of nbtsearch.c:
 * - rightmost pages start data items at ABTP_HIKEY instead of at ABTP_FIRSTKEY.
 * - on non-leaf pages, the key portion of the first item need not be
 *	 stored, we should store only the link.
 *
 * A leaf page being built looks like:
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp0 linp1 linp2 ...           |
 * +-----------+----+---------------------------------+
 * | ... linpN |									  |
 * +-----------+--------------------------------------+
 * |	 ^ last										  |
 * |												  |
 * +-------------+------------------------------------+
 * |			 | itemN ...                          |
 * +-------------+------------------+-----------------+
 * |		  ... item3 item2 item1 | "special space" |
 * +--------------------------------+-----------------+
 *
 * Contrast this with the diagram in bufpage.h; note the mismatch
 * between linps and items.  This is because we reserve linp0 as a
 * placeholder for the pointer to the "high key" item; when we have
 * filled up the page, we will set linp0 to point to itemN and clear
 * linpN.  On the other hand, if we find this is the last (rightmost)
 * page, we leave the items alone and slide the linp array over.  If
 * the high key is to be truncated, offset 1 is deleted, and we insert
 * the truncated high key at offset 1.
 *
 * 'last' pointer indicates the last offset added to the page.
 *
 * 'truncextra' is the size of the posting list in itup, if any.  This
 * information is stashed for the next call here, when we may benefit
 * from considering the impact of truncating away the posting list on
 * the page before deciding to finish the page off.  Posting lists are
 * often relatively large, so it is worth going to the trouble of
 * accounting for the saving from truncating away the posting list of
 * the tuple that becomes the high key (that may be the only way to
 * get close to target free space on the page).  Note that this is
 * only used for the soft fillfactor-wise limit, not the critical hard
 * limit.
 *----------
 */
static void
_abt_buildadd(ABTWriteState *wstate,
			  ABTPageState *state, IndexTuple itup,
			  Size truncextra,
			  ABTCachedAggInfo agg_info)
{
	Page		npage;
	BlockNumber nblkno;
	OffsetNumber last_off;
	Size		last_truncextra;
	Size		pgspc;
	Size		itupsz;
	bool		isleaf;

	/*
	 * This is a handy place to check for cancel interrupts during the btree
	 * load phase of index creation.
	 */
	CHECK_FOR_INTERRUPTS();

	npage = state->abtps_page;
	nblkno = state->abtps_blkno;
	last_off = state->abtps_lastoff;
	last_truncextra = state->abtps_lastextra;
	state->abtps_lastextra = truncextra;

	pgspc = PageGetFreeSpace(npage);
	itupsz = IndexTupleSize(itup);
	itupsz = MAXALIGN(itupsz);
	/* Leaf case has slightly different rules due to suffix truncation */
	isleaf = (state->abtps_level == 0);

	/*
	 * Check whether the new item can fit on a btree page on current level at
	 * all.
	 *
	 * Every newly built index will treat heap TID as part of the keyspace,
	 * which imposes the requirement that new high keys must occasionally have
	 * a heap TID appended within _abt_truncate().  That may leave a new pivot
	 * tuple one or two MAXALIGN() quantums larger than the original
	 * firstright tuple it's derived from.  v4 deals with the problem by
	 * decreasing the limit on the size of tuples inserted on the leaf level
	 * by the same small amount.  Enforce the new v4+ limit on the leaf level,
	 * and the old limit on internal levels, since pivot tuples may need to
	 * make use of the reserved space.  This should never fail on internal
	 * pages.
	 *
	 * At this point, the aggregation value may have already be appended but
	 * the actual derived pivot tuple may be larger than that. So we'll need to
	 * extra conservative about the tuple size here. A safe upper bound of the
	 * size of the original index tuple is: MAXALIGN(itupsz - agg_typlen).
	 * This might be one MAXALIGN larger than needed if agg_typlen is shorter
	 * than MAXALIGN and happens to be placed at a MAXALIGN'd boundry.
	 *
	 */
	if (unlikely((isleaf ?
			(itupsz - agg_info->extra_space_for_nonpivot_tuple) :
			itupsz) > ABTMaxItemSize(npage, agg_info)))
		_abt_check_third_page(wstate->index, wstate->heap, npage, itup,
							  agg_info);

	/*
	 * Check to see if current page will fit new item, with space left over to
	 * append a heap TID during suffix truncation when page is a leaf page.
	 *
	 * It is guaranteed that we can fit at least 2 non-pivot tuples plus a
	 * high key with heap TID when finishing off a leaf page, since we rely on
	 * _abt_check_third_page() rejecting oversized non-pivot tuples.  On
	 * internal pages we can always fit 3 pivot tuples with larger internal
	 * page tuple limit (includes page high key).
	 *
	 * Most of the time, a page is only "full" in the sense that the soft
	 * fillfactor-wise limit has been exceeded.  However, we must always leave
	 * at least two items plus a high key on each page before starting a new
	 * page.  Disregard fillfactor and insert on "full" current page if we
	 * don't have the minimum number of items yet.  (Note that we deliberately
	 * assume that suffix truncation neither enlarges nor shrinks new high key
	 * when applying soft limit, except when last tuple has a posting list.)
	 */
	Assert(last_truncextra == 0 || isleaf);
	if (pgspc < itupsz + (isleaf ? MAXALIGN(sizeof(ItemPointerData)) : 0) ||
		(pgspc + last_truncextra < state->abtps_full && last_off > ABTP_FIRSTKEY))
	{
		/*
		 * Finish off the page and write it out.
		 */
		Page		opage = npage;
		BlockNumber oblkno = nblkno;
		ItemId		ii;
		ItemId		hii;
		IndexTuple	oitup;
		IndexTuple	oitup_copy;

		/* Create new page of same level */
		npage = _abt_blnewpage(state->abtps_level);

		/* and assign it a page position */
		nblkno = wstate->btws_pages_alloced++;

		/*
		 * We copy the last item on the page into the new page, and then
		 * rearrange the old page so that the 'last item' becomes its high key
		 * rather than a true data item.  There had better be at least two
		 * items on the page already, else the page would be empty of useful
		 * data.
		 */
		Assert(last_off > ABTP_FIRSTKEY);
		ii = PageGetItemId(opage, last_off);
		oitup = (IndexTuple) PageGetItem(opage, ii);
		_abt_sortaddtup(npage, ItemIdGetLength(ii), oitup, ABTP_FIRSTKEY,
					   !isleaf, agg_info);

		/*
		 * Move 'last' into the high key position on opage.  _abt_blnewpage()
		 * allocated empty space for a line pointer when opage was first
		 * created, so this is a matter of rearranging already-allocated space
		 * on page, and initializing high key line pointer. (Actually, leaf
		 * pages must also swap oitup with a truncated version of oitup, which
		 * is sometimes larger than oitup, though never by more than the space
		 * needed to append a heap TID.)
		 */
		hii = PageGetItemId(opage, ABTP_HIKEY);
		*hii = *ii;
		ItemIdSetUnused(ii);	/* redundant */
		((PageHeader) opage)->pd_lower -= sizeof(ItemIdData);

		if (isleaf)
		{
			IndexTuple	lastleft;
			IndexTuple	truncated;

			/*
			 * Truncate away any unneeded attributes from high key on leaf
			 * level.  This is only done at the leaf level because downlinks
			 * in internal pages are either negative infinity items, or get
			 * their contents from copying from one level down.  See also:
			 * _abt_split().
			 *
			 * We don't try to bias our choice of split point to make it more
			 * likely that _abt_truncate() can truncate away more attributes,
			 * whereas the split point used within _abt_split() is chosen much
			 * more delicately.  Even still, the lastleft and firstright
			 * tuples passed to _abt_truncate() here are at least not fully
			 * equal to each other when deduplication is used, unless there is
			 * a large group of duplicates (also, unique index builds usually
			 * have few or no spool2 duplicates).  When the split point is
			 * between two unequal tuples, _abt_truncate() will avoid including
			 * a heap TID in the new high key, which is the most important
			 * benefit of suffix truncation.
			 *
			 * Overwrite the old item with new truncated high key directly.
			 * oitup is already located at the physical beginning of tuple
			 * space, so this should directly reuse the existing tuple space.
			 */
			ii = PageGetItemId(opage, OffsetNumberPrev(last_off));
			lastleft = (IndexTuple) PageGetItem(opage, ii);

			Assert(IndexTupleSize(oitup) > last_truncextra);
			truncated = _abt_truncate(wstate->index, lastleft, oitup,
									 wstate->inskey, agg_info);
			if (!PageIndexTupleOverwrite(opage, ABTP_HIKEY, (Item) truncated,
										 IndexTupleSize(truncated)))
				elog(ERROR, "failed to add high key to the index page");
			pfree(truncated);

			/* oitup should continue to point to the page's high key */
			hii = PageGetItemId(opage, ABTP_HIKEY);
			oitup = (IndexTuple) PageGetItem(opage, hii);
		}
		else
		{
			IndexTuple	truncated;

			/* Save a copy of oitup here to set as low key a bit later. */
			oitup_copy = CopyIndexTuple(oitup);

			/* Truncate agg in pivot tuple as well. */
			truncated = _abt_pivot_tuple_purge_agg_space(oitup, agg_info);
			if (!PageIndexTupleOverwrite(opage, ABTP_HIKEY, (Item) truncated,
										IndexTupleSize(truncated)))
				elog(ERROR, "failed to add high key to the index page");
			pfree(truncated);

			hii = PageGetItemId(opage, ABTP_HIKEY);
			oitup = (IndexTuple) PageGetItem(opage, hii);
		}

		/*
		 * Link the old page into its parent, using its low key.  If we don't
		 * have a parent, we have to create one; this adds a new btree level.
		 */
		if (state->abtps_next == NULL)
			state->abtps_next = _abt_pagestate(wstate, state->abtps_level + 1,
											   agg_info);

		Assert((ABTreeTupleGetNAtts(state->abtps_lowkey, wstate->index) <=
				IndexRelationGetNumberOfKeyAttributes(wstate->index) &&
				ABTreeTupleGetNAtts(state->abtps_lowkey, wstate->index) > 0) ||
			   ABT_P_LEFTMOST((ABTPageOpaque) PageGetSpecialPointer(opage)));
		Assert(ABTreeTupleGetNAtts(state->abtps_lowkey, wstate->index) == 0 ||
			   !ABT_P_LEFTMOST((ABTPageOpaque) PageGetSpecialPointer(opage)));
		ABTreeTupleSetDownLink(state->abtps_lowkey, oblkno);
		/* set low key's aggregation value */
		_abt_set_pivot_tuple_aggregation_value(
			state->abtps_lowkey, state->abtps_agg, agg_info);
		if (!agg_info->agg_byval)
		{
			pfree(DatumGetPointer(state->abtps_agg));
		}
		_abt_buildadd(wstate, state->abtps_next, state->abtps_lowkey, 0,
					  agg_info);
		pfree(state->abtps_lowkey);

		/*
		 * Save a copy of the high key from the old page.  It is also the low
		 * key for the new page.
		 */
		if (isleaf)
		{
			/* 
			 * The truncated tuple does not have aggregation space reserved.
			 * So we need to add that here.
			 *
			 * Note: we can't use the original non-pivot version even if it
			 * has space for aggregation, as their offsets may not be the same.
			 */
			state->abtps_lowkey = _abt_pivot_tuple_reserve_agg_space(
				oitup, agg_info);
		}
		else
		{
			/* The version we saved a bit earlier works as the new low key. */
			state->abtps_lowkey = oitup_copy;
		}

		/*
		 * Set the sibling links for both pages.
		 */
		{
			ABTPageOpaque oopaque = (ABTPageOpaque) PageGetSpecialPointer(opage);
			ABTPageOpaque nopaque = (ABTPageOpaque) PageGetSpecialPointer(npage);

			oopaque->abtpo_next = nblkno;
			nopaque->abtpo_prev = oblkno;
			nopaque->abtpo_next = ABTP_NONE;	/* redundant */
		}

		/*
		 * Write out the old page.  We never need to touch it again, so we can
		 * free the opage workspace too.
		 */
		_abt_blwritepage(wstate, opage, oblkno);

		/*
		 * Reset last_off to point to new page
		 */
		last_off = ABTP_FIRSTKEY;
	}

	/*
	 * By here, either original page is still the current page, or a new page
	 * was created that became the current page.  Either way, the current page
	 * definitely has space for new item.
	 *
	 * If the new item is the first for its page, it must also be the first
	 * item on its entire level.  On later same-level pages, a low key for a
	 * page will be copied from the prior page in the code above.  Generate a
	 * minus infinity low key here instead.
	 */
	if (last_off == ABTP_HIKEY)
	{
		Size itup_size = MAXALIGN(sizeof(IndexTupleData)) +
			agg_info->extra_space_when_tid_missing;
		Assert(MAXALIGN(itup_size) == itup_size);
		Assert(state->abtps_lowkey == NULL);
		state->abtps_lowkey = palloc0(itup_size);
		state->abtps_lowkey->t_info = itup_size;
		ABTreeTupleSetNAtts(state->abtps_lowkey, 0, false);
	}

	/* Now add aggregation to the leaf node if needed */
	if (isleaf)
	{
		uint16			i;
		uint16			n;
		TransactionId	*xmin_ptr;

		if (agg_info->leaf_has_agg)
		{
			if (ABTreeTupleIsPosting(itup))
			{
				/* 
				 * TODO map_fn should take the heap tuple as input, and
				 * the value may not be the same in the same posting list
				 */
				Datum agg_val =
					(agg_info->agg_support.abtas_map.fn)(itup);
				_abt_set_posting_tuple_aggregation_values(
					itup,
					agg_val,
					agg_info);
				if (!agg_info->agg_byval)
				{
					pfree(DatumGetPointer(agg_val));
				}
			}
			else
			{
				Datum agg_val =
					(agg_info->agg_support.abtas_map.fn)(itup);
				_abt_set_nonpivot_tuple_aggregation_value(
					itup,
					agg_val,
					agg_info);
			}
		}

		/* 
		 * Also store a dummy FrozenTransactionId into the xmin field.
		 * This is ok regardless what the xmin actually is in the original
		 * table as no one can insert into this index yet and when they can,
		 * these are older than any of those transactions, meaning we need
		 * to return all these index tuples whether they are actually dead
		 * or not.
		 */
		n = ABTreeTupleIsPosting(itup) ? ABTreeTupleGetNPosting(itup) : 1;
		xmin_ptr = ABTreeTupleGetXminPtr(itup);
		for (i = 0; i < n; ++i)
			*xmin_ptr++ = FrozenTransactionId;
	}
	else
	{
		/* set the initial last update id */
		*ABTreeTupleGetLastUpdateIdPtr(itup) = ABT_MIN_LAST_UPDATE_ID;
	}

	/* 
	 * And add the last agg value to the page state's. 
	 * abtps_page needs to be updated here so that
	 * _abt_accum_last_tuple_on_page() can find it. 
	 */
	state->abtps_page = npage;
	_abt_accum_last_tuple_on_page(state, last_off, agg_info);

	/*
	 * Add the new item into the current page.
	 */
	last_off = OffsetNumberNext(last_off);
	_abt_sortaddtup(npage, itupsz, itup, last_off,
				   !isleaf && last_off == ABTP_FIRSTKEY,
				   agg_info);

	state->abtps_blkno = nblkno;
	state->abtps_lastoff = last_off;
}

/*
 * Finalize pending posting list tuple, and add it to the index.  Final tuple
 * is based on saved base tuple, and saved list of heap TIDs.
 *
 * This is almost like _abt_dedup_finish_pending(), but it adds a new tuple
 * using _abt_buildadd().
 */
static void
_abt_sort_dedup_finish_pending(ABTWriteState *wstate, ABTPageState *state,
							   ABTDedupState dstate,
							   ABTCachedAggInfo agg_info)
{
	Assert(dstate->nitems > 0);

	if (dstate->nitems == 1)
		_abt_buildadd(wstate, state, dstate->base, 0, agg_info);
	else
	{
		IndexTuple	postingtuple;
		Size		truncextra;

		/* form a tuple with a posting list with aggregation array following */
		postingtuple = _abt_form_posting(dstate->base,
										dstate->htids,
										dstate->nhtids,
										agg_info);
		/* Calculate posting list, aggregation array and xmin array overhead */
		truncextra = IndexTupleSize(postingtuple) -
			ABTreeTupleGetPostingOffset(postingtuple);

		_abt_buildadd(wstate, state, postingtuple, truncextra, agg_info);
		pfree(postingtuple);
	}

	dstate->nmaxitems = 0;
	dstate->nhtids = 0;
	dstate->nitems = 0;
	dstate->phystupsize = 0;
}

/*
 * Finish writing out the completed btree.
 */
static void
_abt_uppershutdown(ABTWriteState *wstate,
				   ABTPageState *state,
				   ABTCachedAggInfo agg_info)
{
	ABTPageState *s;
	BlockNumber rootblkno = ABTP_NONE;
	uint32		rootlevel = 0;
	Page		metapage;

	/*
	 * Each iteration of this loop completes one more level of the tree.
	 */
	for (s = state; s != NULL; s = s->abtps_next)
	{
		BlockNumber blkno;
		ABTPageOpaque opaque;

		blkno = s->abtps_blkno;
		opaque = (ABTPageOpaque) PageGetSpecialPointer(s->abtps_page);

		/* 
		 * Need to accumulate the aggregation value of the last tuple added to
		 * on this page to the page state.
		 */
		_abt_accum_last_tuple_on_page(s, s->abtps_lastoff, agg_info);

		/*
		 * We have to link the last page on this level to somewhere.
		 *
		 * If we're at the top, it's the root, so attach it to the metapage.
		 * Otherwise, add an entry for it to its parent using its low key.
		 * This may cause the last page of the parent level to split, but
		 * that's not a problem -- we haven't gotten to it yet.
		 */
		if (s->abtps_next == NULL)
		{
			opaque->abtpo_flags |= ABTP_ROOT;
			rootblkno = blkno;
			rootlevel = s->abtps_level;
		}
		else
		{
			Assert((ABTreeTupleGetNAtts(s->abtps_lowkey, wstate->index) <=
					IndexRelationGetNumberOfKeyAttributes(wstate->index) &&
					ABTreeTupleGetNAtts(s->abtps_lowkey, wstate->index) > 0) ||
				   ABT_P_LEFTMOST(opaque));
			Assert(ABTreeTupleGetNAtts(s->abtps_lowkey, wstate->index) == 0 ||
				   !ABT_P_LEFTMOST(opaque));
			ABTreeTupleSetDownLink(s->abtps_lowkey, blkno);
			/* set the low key's aggregation value here */	
			_abt_set_pivot_tuple_aggregation_value(
				s->abtps_lowkey, s->abtps_agg, agg_info);
			if (!agg_info->agg_byval)
			{
				pfree(DatumGetPointer(s->abtps_agg));
			}
			_abt_buildadd(wstate, s->abtps_next, s->abtps_lowkey, 0,
						  agg_info);
			pfree(s->abtps_lowkey);
			s->abtps_lowkey = NULL;
		}

		/*
		 * This is the rightmost page, so the ItemId array needs to be slid
		 * back one slot.  Then we can dump out the page.
		 */
		_abt_slideleft(s->abtps_page);
		_abt_blwritepage(wstate, s->abtps_page, s->abtps_blkno);
		s->abtps_page = NULL;	/* writepage freed the workspace */
	}

	/*
	 * As the last step in the process, construct the metapage and make it
	 * point to the new root (unless we had no data at all, in which case it's
	 * set to point to "ABTP_NONE").  This changes the index to the "valid" state
	 * by filling in a valid magic number in the metapage.
	 */
	metapage = (Page) palloc(BLCKSZ);
	_abt_initmetapage(&agg_info->agg_support,
					  metapage, rootblkno, rootlevel,
					  wstate->inskey->allequalimage);
	_abt_blwritepage(wstate, metapage, ABTREE_METAPAGE);
}

/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
static void
_abt_load(ABTBuildState *buildstate, ABTWriteState *wstate)
{
	ABTSpool	*btspool = buildstate->spool;
	ABTSpool	*btspool2 = buildstate->spool2;
	ABTPageState *state = NULL;
	bool		merge = (btspool2 != NULL);
	IndexTuple	itup,
				itup2 = NULL;
	bool		load1;
	TupleDesc	tupdes = RelationGetDescr(wstate->index);
	int			i,
				keysz = IndexRelationGetNumberOfKeyAttributes(wstate->index);
	SortSupport sortKeys;
	int64		tuples_done = 0;
	bool		deduplicate;
	ABTCachedAggInfo	agg_info = buildstate->agg_info;

	deduplicate = wstate->inskey->allequalimage && !btspool->isunique &&
		ABTGetDeduplicateItems(wstate->index);

	if (merge)
	{
		/*
		 * Another ABTSpool for dead tuples exists. Now we have to merge
		 * btspool and btspool2.
		 */

		/* the preparation of merge */
		itup = tuplesort_getindextuple(btspool->sortstate, true);
		itup2 = tuplesort_getindextuple(btspool2->sortstate, true);

		/* Prepare SortSupport data for each column */
		sortKeys = (SortSupport) palloc0(keysz * sizeof(SortSupportData));

		for (i = 0; i < keysz; i++)
		{
			SortSupport sortKey = sortKeys + i;
			ScanKey		scanKey = wstate->inskey->scankeys + i;
			int16		strategy;

			sortKey->ssup_cxt = CurrentMemoryContext;
			sortKey->ssup_collation = scanKey->sk_collation;
			sortKey->ssup_nulls_first =
				(scanKey->sk_flags & SK_ABT_NULLS_FIRST) != 0;
			sortKey->ssup_attno = scanKey->sk_attno;
			/* Abbreviation is not supported here */
			sortKey->abbreviate = false;

			AssertState(sortKey->ssup_attno != 0);

			strategy = (scanKey->sk_flags & SK_ABT_DESC) != 0 ?
				BTGreaterStrategyNumber : BTLessStrategyNumber;

			PrepareSortSupportFromIndexRel(wstate->index, strategy, sortKey);
		}

		for (;;)
		{
			load1 = true;		/* load ABTSpool next ? */
			if (itup2 == NULL)
			{
				if (itup == NULL)
					break;
			}
			else if (itup != NULL)
			{
				int32		compare = 0;

				for (i = 1; i <= keysz; i++)
				{
					SortSupport entry;
					Datum		attrDatum1,
								attrDatum2;
					bool		isNull1,
								isNull2;

					entry = sortKeys + i - 1;
					attrDatum1 = index_getattr(itup, i, tupdes, &isNull1);
					attrDatum2 = index_getattr(itup2, i, tupdes, &isNull2);

					compare = ApplySortComparator(attrDatum1, isNull1,
												  attrDatum2, isNull2,
												  entry);
					if (compare > 0)
					{
						load1 = false;
						break;
					}
					else if (compare < 0)
						break;
				}

				/*
				 * If key values are equal, we sort on ItemPointer.  This is
				 * required for btree indexes, since heap TID is treated as an
				 * implicit last key attribute in order to ensure that all
				 * keys in the index are physically unique.
				 */
				if (compare == 0)
				{
					compare = ItemPointerCompare(&itup->t_tid, &itup2->t_tid);
					Assert(compare != 0);
					if (compare > 0)
						load1 = false;
				}
			}
			else
				load1 = false;

			/* When we see first tuple, create first index page */
			if (state == NULL)
				state = _abt_pagestate(wstate, 0, agg_info);

			if (load1)
			{
				_abt_buildadd(wstate, state, itup, 0, agg_info);
				itup = tuplesort_getindextuple(btspool->sortstate, true);
			}
			else
			{
				_abt_buildadd(wstate, state, itup2, 0, agg_info);
				itup2 = tuplesort_getindextuple(btspool2->sortstate, true);
			}

			/* Report progress */
			pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
										 ++tuples_done);
		}
		pfree(sortKeys);
	}
	else if (deduplicate)
	{
		/* merge is unnecessary, deduplicate into posting lists */
		ABTDedupState dstate;

		dstate = (ABTDedupState) palloc(sizeof(ABTDedupStateData));
		dstate->deduplicate = true; /* unused */
		dstate->save_extra = false; /* we'll initialize them by ourselves */
		dstate->nmaxitems = 0;	/* unused */
		dstate->maxpostingsize = 0; /* set later */
		/* Metadata about base tuple of current pending posting list */
		dstate->base = NULL;
		dstate->baseoff = InvalidOffsetNumber;	/* unused */
		dstate->basetupsize = 0;
		/* Metadata about current pending posting list TIDs */
		dstate->htids = NULL;
		dstate->aggs = NULL;
		dstate->xmins = NULL;
		dstate->nhtids = 0;
		dstate->nitems = 0;
		dstate->phystupsize = 0;	/* unused */
		dstate->nintervals = 0; /* unused */

		while ((itup = tuplesort_getindextuple(btspool->sortstate,
											   true)) != NULL)
		{
			/* When we see first tuple, create first index page */
			if (state == NULL)
			{
				state = _abt_pagestate(wstate, 0, agg_info);

				/*
				 * Limit size of posting list tuples to 1/10 space we want to
				 * leave behind on the page, plus space for final item's line
				 * pointer.  This is equal to the space that we'd like to
				 * leave behind on each leaf page when fillfactor is 90,
				 * allowing us to get close to fillfactor% space utilization
				 * when there happen to be a great many duplicates.  (This
				 * makes higher leaf fillfactor settings ineffective when
				 * building indexes that have many duplicates, but packing
				 * leaf pages full with few very large tuples doesn't seem
				 * like a useful goal.)
				 */
				/* 
				 * TODO posting list also needs to include the weights if
				 * map is not constant. 
				 */
				dstate->maxpostingsize = MAXALIGN_DOWN((BLCKSZ * 10 / 100)) -
					sizeof(ItemIdData);
				Assert(dstate->maxpostingsize <= 
						ABTMaxItemSize(state->abtps_page, agg_info) &&
					   dstate->maxpostingsize <= INDEX_SIZE_MASK);
				dstate->htids = palloc(dstate->maxpostingsize);

				/* start new pending posting list with itup copy */
				_abt_dedup_start_pending(dstate, CopyIndexTuple(itup),
										InvalidOffsetNumber, agg_info);
			}
			else if (dstate->nhtids > 0 && /* make sure base is not oversized */
					 _abt_keep_natts_fast(wstate->index, dstate->base,
										  itup) > keysz &&
					 _abt_dedup_save_htid(dstate, itup, agg_info))
			{
				/*
				 * Tuple is equal to base tuple of pending posting list.  Heap
				 * TID from itup has been saved in state.
				 */
			}
			else
			{
				/*
				 * Tuple is not equal to pending posting list tuple, or
				 * _abt_dedup_save_htid() opted to not merge current item into
				 * pending posting list.
				 */
				_abt_sort_dedup_finish_pending(wstate, state, dstate,
											   agg_info);
				pfree(dstate->base);

				/* start new pending posting list with itup copy */
				_abt_dedup_start_pending(dstate, CopyIndexTuple(itup),
										InvalidOffsetNumber, agg_info);
			}

			/* Report progress */
			pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
										 ++tuples_done);
		}

		if (state)
		{
			/*
			 * Handle the last item (there must be a last item when the
			 * tuplesort returned one or more tuples)
			 */
			_abt_sort_dedup_finish_pending(wstate, state, dstate, agg_info);
			pfree(dstate->base);
			pfree(dstate->htids);
		}

		pfree(dstate);
	}
	else
	{
		/* merging and deduplication are both unnecessary */
		while ((itup = tuplesort_getindextuple(btspool->sortstate,
											   true)) != NULL)
		{
			/* When we see first tuple, create first index page */
			if (state == NULL)
				state = _abt_pagestate(wstate, 0, agg_info);

			_abt_buildadd(wstate, state, itup, 0, agg_info);

			/* Report progress */
			pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
										 ++tuples_done);
		}
	}

	/* Close down final pages and write the metapage */
	_abt_uppershutdown(wstate, state, agg_info);

	/*
	 * When we WAL-logged index pages, we must nonetheless fsync index files.
	 * Since we're building outside shared buffers, a CHECKPOINT occurring
	 * during the build has no way to flush the previously written data to
	 * disk (indeed it won't know the index even exists).  A crash later on
	 * would replay WAL from the checkpoint, therefore it wouldn't replay our
	 * earlier WAL entries. If we do not fsync those pages here, they might
	 * still not be on disk when the crash occurs.
	 */
	if (wstate->btws_use_wal)
	{
		RelationOpenSmgr(wstate->index);
		smgrimmedsync(wstate->index->rd_smgr, MAIN_FORKNUM);
	}
}

/*
 * Create parallel context, and launch workers for leader.
 *
 * buildstate argument should be initialized (with the exception of the
 * tuplesort state in spools, which may later be created based on shared
 * state initially set up here).
 *
 * isconcurrent indicates if operation is CREATE INDEX CONCURRENTLY.
 *
 * request is the target number of parallel worker processes to launch.
 *
 * Sets buildstate's ABTLeader, which caller must use to shut down parallel
 * mode by passing it to _abt_end_parallel() at the very end of its index
 * build.  If not even a single worker process can be launched, this is
 * never set, and caller should proceed with a serial index build.
 */
static void
_abt_begin_parallel(ABTBuildState *buildstate, bool isconcurrent, int request)
{
	ParallelContext *pcxt;
	int			scantuplesortstates;
	Snapshot	snapshot;
	Size		estbtshared;
	Size		estsort;
	ABTShared   *btshared;
	Sharedsort *sharedsort;
	Sharedsort *sharedsort2;
	ABTSpool    *btspool = buildstate->spool;
	ABTLeader   *btleader = (ABTLeader *) palloc0(sizeof(ABTLeader));
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	bool		leaderparticipates = true;
	int			querylen;

#ifdef DISABLE_LEADER_PARTICIPATION
	leaderparticipates = false;
#endif

	/*
	 * Enter parallel mode, and create context for parallel build of btree
	 * index
	 */
	EnterParallelMode();
	Assert(request > 0);
	pcxt = CreateParallelContext("postgres", "_abt_parallel_build_main",
								 request);

	scantuplesortstates = leaderparticipates ? request + 1 : request;

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples).  In a
	 * concurrent build, we take a regular MVCC snapshot and index whatever's
	 * live according to that.
	 */
	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/*
	 * Estimate size for our own PARALLEL_KEY_ABTREE_SHARED workspace, and
	 * PARALLEL_KEY_TUPLESORT tuplesort workspace
	 */
	estbtshared = _abt_parallel_estimate_shared(btspool->heap, snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, estbtshared);
	estsort = tuplesort_estimate_shared(scantuplesortstates);
	shm_toc_estimate_chunk(&pcxt->estimator, estsort);

	/*
	 * Unique case requires a second spool, and so we may have to account for
	 * another shared workspace for that -- PARALLEL_KEY_TUPLESORT_SPOOL2
	 */
	if (!btspool->isunique)
		shm_toc_estimate_keys(&pcxt->estimator, 2);
	else
	{
		shm_toc_estimate_chunk(&pcxt->estimator, estsort);
		shm_toc_estimate_keys(&pcxt->estimator, 3);
	}

	/*
	 * Estimate space for WalUsage and BufferUsage -- PARALLEL_KEY_WAL_USAGE
	 * and PARALLEL_KEY_BUFFER_USAGE.
	 *
	 * If there are no extensions loaded that care, we could skip this.  We
	 * have no way of knowing whether anyone's looking at pgWalUsage or
	 * pgBufferUsage, so do it unconditionally.
	 */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Finally, estimate PARALLEL_KEY_QUERY_TEXT space */
	if (debug_query_string)
	{
		querylen = strlen(debug_query_string);
		shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	else
		querylen = 0;			/* keep compiler quiet */

	/* Everyone's had a chance to ask for space, so now create the DSM */
	InitializeParallelDSM(pcxt);

	/* If no DSM segment was available, back out (do serial build) */
	if (pcxt->seg == NULL)
	{
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return;
	}

	/* Store shared build state, for which we reserved space */
	btshared = (ABTShared *) shm_toc_allocate(pcxt->toc, estbtshared);
	/* Initialize immutable state */
	btshared->heaprelid = RelationGetRelid(btspool->heap);
	btshared->indexrelid = RelationGetRelid(btspool->index);
	btshared->isunique = btspool->isunique;
	btshared->isconcurrent = isconcurrent;
	btshared->scantuplesortstates = scantuplesortstates;
	ConditionVariableInit(&btshared->workersdonecv);
	SpinLockInit(&btshared->mutex);
	/* Initialize mutable state */
	btshared->nparticipantsdone = 0;
	btshared->reltuples = 0.0;
	btshared->havedead = false;
	btshared->indtuples = 0.0;
	btshared->brokenhotchain = false;
	table_parallelscan_initialize(btspool->heap,
								  ParallelTableScanFromABTShared(btshared),
								  snapshot);

	/*
	 * Store shared tuplesort-private state, for which we reserved space.
	 * Then, initialize opaque state using tuplesort routine.
	 */
	sharedsort = (Sharedsort *) shm_toc_allocate(pcxt->toc, estsort);
	tuplesort_initialize_shared(sharedsort, scantuplesortstates,
								pcxt->seg);

	shm_toc_insert(pcxt->toc, PARALLEL_KEY_ABTREE_SHARED, btshared);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT, sharedsort);

	/* Unique case requires a second spool, and associated shared state */
	if (!btspool->isunique)
		sharedsort2 = NULL;
	else
	{
		/*
		 * Store additional shared tuplesort-private state, for which we
		 * reserved space.  Then, initialize opaque state using tuplesort
		 * routine.
		 */
		sharedsort2 = (Sharedsort *) shm_toc_allocate(pcxt->toc, estsort);
		tuplesort_initialize_shared(sharedsort2, scantuplesortstates,
									pcxt->seg);

		shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT_SPOOL2, sharedsort2);
	}

	/* Store query string for workers */
	if (debug_query_string)
	{
		char	   *sharedquery;

		sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
		memcpy(sharedquery, debug_query_string, querylen + 1);
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, sharedquery);
	}

	/*
	 * Allocate space for each worker's WalUsage and BufferUsage; no need to
	 * initialize.
	 */
	walusage = shm_toc_allocate(pcxt->toc,
								mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_WAL_USAGE, walusage);
	bufferusage = shm_toc_allocate(pcxt->toc,
								   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage);

	/* Launch workers, saving status for leader/caller */
	LaunchParallelWorkers(pcxt);
	btleader->pcxt = pcxt;
	btleader->nparticipanttuplesorts = pcxt->nworkers_launched;
	if (leaderparticipates)
		btleader->nparticipanttuplesorts++;
	btleader->btshared = btshared;
	btleader->sharedsort = sharedsort;
	btleader->sharedsort2 = sharedsort2;
	btleader->snapshot = snapshot;
	btleader->walusage = walusage;
	btleader->bufferusage = bufferusage;

	/* If no workers were successfully launched, back out (do serial build) */
	if (pcxt->nworkers_launched == 0)
	{
		_abt_end_parallel(btleader);
		return;
	}

	/* Save leader state now that it's clear build will be parallel */
	buildstate->btleader = btleader;

	/* Join heap scan ourselves */
	if (leaderparticipates)
		_abt_leader_participate_as_worker(buildstate);

	/*
	 * Caller needs to wait for all launched workers when we return.  Make
	 * sure that the failure-to-start case will not hang forever.
	 */
	WaitForParallelWorkersToAttach(pcxt);
}

/*
 * Shut down workers, destroy parallel context, and end parallel mode.
 */
static void
_abt_end_parallel(ABTLeader *btleader)
{
	int			i;

	/* Shutdown worker processes */
	WaitForParallelWorkersToFinish(btleader->pcxt);

	/*
	 * Next, accumulate WAL usage.  (This must wait for the workers to finish,
	 * or we might get incomplete data.)
	 */
	for (i = 0; i < btleader->pcxt->nworkers_launched; i++)
		InstrAccumParallelQuery(&btleader->bufferusage[i], &btleader->walusage[i]);

	/* Free last reference to MVCC snapshot, if one was used */
	if (IsMVCCSnapshot(btleader->snapshot))
		UnregisterSnapshot(btleader->snapshot);
	DestroyParallelContext(btleader->pcxt);
	ExitParallelMode();
}

/*
 * Returns size of shared memory required to store state for a parallel
 * btree index build based on the snapshot its parallel scan will use.
 */
static Size
_abt_parallel_estimate_shared(Relation heap, Snapshot snapshot)
{
	/* c.f. shm_toc_allocate as to why BUFFERALIGN is used */
	return add_size(BUFFERALIGN(sizeof(ABTShared)),
					table_parallelscan_estimate(heap, snapshot));
}

/*
 * Within leader, wait for end of heap scan.
 *
 * When called, parallel heap scan started by _abt_begin_parallel() will
 * already be underway within worker processes (when leader participates
 * as a worker, we should end up here just as workers are finishing).
 *
 * Fills in fields needed for ambuild statistics, and lets caller set
 * field indicating that some worker encountered a broken HOT chain.
 *
 * Returns the total number of heap tuples scanned.
 */
static double
_abt_parallel_heapscan(ABTBuildState *buildstate, bool *brokenhotchain)
{
	ABTShared   *btshared = buildstate->btleader->btshared;
	int			nparticipanttuplesorts;
	double		reltuples;

	nparticipanttuplesorts = buildstate->btleader->nparticipanttuplesorts;
	for (;;)
	{
		SpinLockAcquire(&btshared->mutex);
		if (btshared->nparticipantsdone == nparticipanttuplesorts)
		{
			buildstate->havedead = btshared->havedead;
			buildstate->indtuples = btshared->indtuples;
			*brokenhotchain = btshared->brokenhotchain;
			reltuples = btshared->reltuples;
			SpinLockRelease(&btshared->mutex);
			break;
		}
		SpinLockRelease(&btshared->mutex);

		ConditionVariableSleep(&btshared->workersdonecv,
							   WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}

	ConditionVariableCancelSleep();

	return reltuples;
}

/*
 * Within leader, participate as a parallel worker.
 */
static void
_abt_leader_participate_as_worker(ABTBuildState *buildstate)
{
	ABTLeader   *btleader = buildstate->btleader;
	ABTSpool    *leaderworker;
	ABTSpool    *leaderworker2;
	int			sortmem;

	/* Allocate memory and initialize private spool */
	leaderworker = (ABTSpool *) palloc0(sizeof(ABTSpool));
	leaderworker->heap = buildstate->spool->heap;
	leaderworker->index = buildstate->spool->index;
	leaderworker->isunique = buildstate->spool->isunique;

	/* Initialize second spool, if required */
	if (!btleader->btshared->isunique)
		leaderworker2 = NULL;
	else
	{
		/* Allocate memory for worker's own private secondary spool */
		leaderworker2 = (ABTSpool *) palloc0(sizeof(ABTSpool));

		/* Initialize worker's own secondary spool */
		leaderworker2->heap = leaderworker->heap;
		leaderworker2->index = leaderworker->index;
		leaderworker2->isunique = false;
	}

	/*
	 * Might as well use reliable figure when doling out maintenance_work_mem
	 * (when requested number of workers were not launched, this will be
	 * somewhat higher than it is for other workers).
	 */
	sortmem = maintenance_work_mem / btleader->nparticipanttuplesorts;

	/* Perform work common to all participants */
	_abt_parallel_scan_and_sort(leaderworker, leaderworker2, btleader->btshared,
							   btleader->sharedsort, btleader->sharedsort2,
							   sortmem, true);

#ifdef ABTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("ABTREE BUILD (Leader Partial Spool) STATISTICS");
		ResetUsage();
	}
#endif							/* ABTREE_BUILD_STATS */
}

/*
 * Perform work within a launched parallel process.
 */
void
_abt_parallel_build_main(dsm_segment *seg, shm_toc *toc)
{
	char	   *sharedquery;
	ABTSpool    *btspool;
	ABTSpool    *btspool2;
	ABTShared   *btshared;
	Sharedsort *sharedsort;
	Sharedsort *sharedsort2;
	Relation	heapRel;
	Relation	indexRel;
	LOCKMODE	heapLockmode;
	LOCKMODE	indexLockmode;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	int			sortmem;

#ifdef ABTREE_BUILD_STATS
	if (log_btree_build_stats)
		ResetUsage();
#endif							/* ABTREE_BUILD_STATS */

	/* Set debug_query_string for individual workers first */
	sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true);
	debug_query_string = sharedquery;

	/* Report the query string from leader */
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	/* Look up nbtree shared state */
	btshared = shm_toc_lookup(toc, PARALLEL_KEY_ABTREE_SHARED, false);

	/* Open relations using lock modes known to be obtained by index.c */
	if (!btshared->isconcurrent)
	{
		heapLockmode = ShareLock;
		indexLockmode = AccessExclusiveLock;
	}
	else
	{
		heapLockmode = ShareUpdateExclusiveLock;
		indexLockmode = RowExclusiveLock;
	}

	/* Open relations within worker */
	heapRel = table_open(btshared->heaprelid, heapLockmode);
	indexRel = index_open(btshared->indexrelid, indexLockmode);

	/* Initialize worker's own spool */
	btspool = (ABTSpool *) palloc0(sizeof(ABTSpool));
	btspool->heap = heapRel;
	btspool->index = indexRel;
	btspool->isunique = btshared->isunique;

	/* Look up shared state private to tuplesort.c */
	sharedsort = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT, false);
	tuplesort_attach_shared(sharedsort, seg);
	if (!btshared->isunique)
	{
		btspool2 = NULL;
		sharedsort2 = NULL;
	}
	else
	{
		/* Allocate memory for worker's own private secondary spool */
		btspool2 = (ABTSpool *) palloc0(sizeof(ABTSpool));

		/* Initialize worker's own secondary spool */
		btspool2->heap = btspool->heap;
		btspool2->index = btspool->index;
		btspool2->isunique = false;
		/* Look up shared state private to tuplesort.c */
		sharedsort2 = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT_SPOOL2, false);
		tuplesort_attach_shared(sharedsort2, seg);
	}

	/* Prepare to track buffer usage during parallel execution */
	InstrStartParallelQuery();

	/* Perform sorting of spool, and possibly a spool2 */
	sortmem = maintenance_work_mem / btshared->scantuplesortstates;
	_abt_parallel_scan_and_sort(btspool, btspool2, btshared, sharedsort,
							   sharedsort2, sortmem, false);

	/* Report WAL/buffer usage during parallel execution */
	bufferusage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false);
	walusage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false);
	InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber],
						  &walusage[ParallelWorkerNumber]);

#ifdef ABTREE_BUILD_STATS
	if (log_btree_build_stats)
	{
		ShowUsage("ABTREE BUILD (Worker Partial Spool) STATISTICS");
		ResetUsage();
	}
#endif							/* ABTREE_BUILD_STATS */

	index_close(indexRel, indexLockmode);
	table_close(heapRel, heapLockmode);
}

/*
 * Perform a worker's portion of a parallel sort.
 *
 * This generates a tuplesort for passed btspool, and a second tuplesort
 * state if a second btspool is need (i.e. for unique index builds).  All
 * other spool fields should already be set when this is called.
 *
 * sortmem is the amount of working memory to use within each worker,
 * expressed in KBs.
 *
 * When this returns, workers are done, and need only release resources.
 */
static void
_abt_parallel_scan_and_sort(ABTSpool *btspool, ABTSpool *btspool2,
						   ABTShared *btshared, Sharedsort *sharedsort,
						   Sharedsort *sharedsort2, int sortmem, bool progress)
{
	SortCoordinate coordinate;
	ABTBuildState buildstate;
	TableScanDesc scan;
	double		reltuples;
	IndexInfo  *indexInfo;

	/* Initialize local tuplesort coordination state */
	coordinate = palloc0(sizeof(SortCoordinateData));
	coordinate->isWorker = true;
	coordinate->nParticipants = -1;
	coordinate->sharedsort = sharedsort;

	/* Begin "partial" tuplesort */
	btspool->sortstate = tuplesort_begin_index_btree(btspool->heap,
													 btspool->index,
													 btspool->isunique,
													 sortmem, coordinate,
													 false);

	/*
	 * Just as with serial case, there may be a second spool.  If so, a
	 * second, dedicated spool2 partial tuplesort is required.
	 */
	if (btspool2)
	{
		SortCoordinate coordinate2;

		/*
		 * We expect that the second one (for dead tuples) won't get very
		 * full, so we give it only work_mem (unless sortmem is less for
		 * worker).  Worker processes are generally permitted to allocate
		 * work_mem independently.
		 */
		coordinate2 = palloc0(sizeof(SortCoordinateData));
		coordinate2->isWorker = true;
		coordinate2->nParticipants = -1;
		coordinate2->sharedsort = sharedsort2;
		btspool2->sortstate =
			tuplesort_begin_index_btree(btspool->heap, btspool->index, false,
										Min(sortmem, work_mem), coordinate2,
										false);
	}

	/* Fill in buildstate for _abt_build_callback() */
	buildstate.isunique = btshared->isunique;
	buildstate.havedead = false;
	buildstate.heap = btspool->heap;
	buildstate.spool = btspool;
	buildstate.spool2 = btspool2;
	buildstate.indtuples = 0;
	buildstate.btleader = NULL;

	/* Join parallel scan */
	indexInfo = BuildIndexInfo(btspool->index);
	indexInfo->ii_Concurrent = btshared->isconcurrent;
	scan = table_beginscan_parallel(btspool->heap,
									ParallelTableScanFromABTShared(btshared));
	reltuples = table_index_build_scan(btspool->heap, btspool->index, indexInfo,
									   true, progress, _abt_build_callback,
									   (void *) &buildstate, scan);

	/*
	 * Execute this worker's part of the sort.
	 *
	 * Unlike leader and serial cases, we cannot avoid calling
	 * tuplesort_performsort() for spool2 if it ends up containing no dead
	 * tuples (this is disallowed for workers by tuplesort).
	 */
	tuplesort_performsort(btspool->sortstate);
	if (btspool2)
		tuplesort_performsort(btspool2->sortstate);

	/*
	 * Done.  Record ambuild statistics, and whether we encountered a broken
	 * HOT chain.
	 */
	SpinLockAcquire(&btshared->mutex);
	btshared->nparticipantsdone++;
	btshared->reltuples += reltuples;
	if (buildstate.havedead)
		btshared->havedead = true;
	btshared->indtuples += buildstate.indtuples;
	if (indexInfo->ii_BrokenHotChain)
		btshared->brokenhotchain = true;
	SpinLockRelease(&btshared->mutex);

	/* Notify leader */
	ConditionVariableSignal(&btshared->workersdonecv);

	/* We can end tuplesorts immediately */
	tuplesort_end(btspool->sortstate);
	if (btspool2)
		tuplesort_end(btspool2->sortstate);
}

static void
_abt_accum_last_tuple_on_page(
	ABTPageState *state,
	OffsetNumber last_off,
	ABTCachedAggInfo agg_info)
{
	ItemId		ii;
	IndexTuple	itup;
	
	/* There's no last tuple. */
	if (last_off == ABTP_HIKEY) return ;

	ii = PageGetItemId(state->abtps_page, last_off);
	itup = (IndexTuple) PageGetItem(state->abtps_page, ii);

	_abt_accum_index_tuple_value(agg_info, &state->abtps_agg, itup,
		/* is_leaf = */state->abtps_level == 0, 
		/* is_first = */last_off == ABTP_FIRSTKEY);
}

