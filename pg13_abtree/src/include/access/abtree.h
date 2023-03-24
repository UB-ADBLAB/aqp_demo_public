/*------------------------------------------------------------------------
 *
 * abtree.h
 *      header file for an aggregate btree implementation based on
 *      postgres btree
 *
 *      Copied and adapted from src/include/access/nbtree.h.
 *
 * src/include/access/abtree.h
 *
 *------------------------------------------------------------------------
 */

#ifndef ABTREE_H
#define ABTREE_H

#include "access/amapi.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "access/xlogreader.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_index.h"
#include "lib/stringinfo.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/shm_toc.h"

extern int abtree_version_store_initial_mem;
extern int abtree_version_store_num_buckets;
extern int abtree_version_store_garbage_collector_delay;


/* Stats for collecting abtree metrics (for demo) */
extern bool abtree_enable_metrics_collection;
extern int abtree_metrics_reporter_delay;
extern int abtree_metrics_reporter_port;
extern pg_atomic_uint64 *abtree_ninserts;
extern pg_atomic_uint64 *abtree_nsamples_accepted;
extern pg_atomic_uint64 *abtree_nsamples_rejected;

static inline void
_abt_inc_ninserts(void)
{
    uint64 x = pg_atomic_read_u64(&abtree_ninserts[MyBackendId - 1]);
    pg_atomic_write_u64(&abtree_ninserts[MyBackendId - 1], x + 1);
}

static inline void
_abt_inc_nsamples_accepted(void)
{
    uint64 x = pg_atomic_read_u64(&abtree_nsamples_accepted[MyBackendId - 1]);
    pg_atomic_write_u64(&abtree_nsamples_accepted[MyBackendId - 1], x + 1);
}

static inline void
_abt_inc_nsamples_rejected(void)
{
    uint64 x = pg_atomic_read_u64(&abtree_nsamples_rejected[MyBackendId - 1]);
    pg_atomic_write_u64(&abtree_nsamples_rejected[MyBackendId - 1], x + 1);
}

extern void ABTreeRegisterMetricsReporter(void);
extern void ABTreeMetricsReporterMain(Datum main_arg);

/* There's room for a 16-bit vacuum cycle ID in BTPageOpaqueData */
typedef uint16 ABTCycleId;

/* 16-bit last update id for both page and pivot tuples */
typedef uint16 ABTLastUpdateId;
#define ABT_INVALID_LAST_UPDATE_ID 0
#define ABT_MIN_LAST_UPDATE_ID 1
#define ABTAdvanceLastUpdateId(last_update_id) \
	((last_update_id + 1 == 0) ? ABT_MIN_LAST_UPDATE_ID : (last_update_id + 1))

/*
 *	BTPageOpaqueData -- At the end of every page, we store a pointer
 *	to both siblings in the tree.  This is used to do forward/backward
 *	index scans.  The next-page link is also critical for recovery when
 *	a search has navigated to the wrong page due to concurrent page splits
 *	or deletions; see src/backend/access/nbtree/README for more info.
 *
 *	In addition, we store the page's btree level (counting upwards from
 *	zero at a leaf page) as well as some flag bits indicating the page type
 *	and status.  If the page is deleted, we replace the level with the
 *	next-transaction-ID value indicating when it is safe to reclaim the page.
 *
 *	We also store a "vacuum cycle ID".  When a page is split while VACUUM is
 *	processing the index, a nonzero value associated with the VACUUM run is
 *	stored into both halves of the split page.  (If VACUUM is not running,
 *	both pages receive zero cycleids.)	This allows VACUUM to detect whether
 *	a page was split since it started, with a small probability of false match
 *	if the page was last split some exact multiple of MAX_BT_CYCLE_ID VACUUMs
 *	ago.  Also, during a split, the BTP_SPLIT_END flag is cleared in the left
 *	(original) page, and set in the right page, but only if the next page
 *	to its right has a different cycleid.
 *
 *	NOTE: the BTP_LEAF flag bit is redundant since level==0 could be tested
 *	instead.
 *
 *	We now additionally store a 64-bit last update id in the page opaque to
 *	detect any insertion or deletion to a non-leaf page while we are updating
 *	the aggregates along an insertion path. It is incremented every time
 *	someone does inserts or deletes a tuple into the page (in exclusive lock
 *	mode). Readers are reading it in shared lock mode. We don't maintain this
 *	ID for any of the leaf pages.
 */

typedef struct ABTPageOpaqueData
{
	BlockNumber abtpo_prev;		/* left sibling, or P_NONE if leftmost */
	BlockNumber abtpo_next;		/* right sibling, or P_NONE if rightmost */

	/* 8 bytes */
	union
	{
		uint32		level;		/* tree level --- zero for leaf pages */
		TransactionId xact;		/* next transaction ID, if deleted */
	}			abtpo;
	uint16		abtpo_flags;	/* flag bits, see below */
	ABTCycleId	abtpo_cycleid;	/* vacuum cycle ID of latest split */

	/* 16 bytes */
	ABTLastUpdateId abtpo_last_update_id; /* last tuple insertion/deletion id */
	char abtpo_unused[6]; /* wasting 6 bytes here */
	/* 24 bytes */
} ABTPageOpaqueData;

typedef ABTPageOpaqueData *ABTPageOpaque;

/* Bits defined in abtpo_flags */
#define ABTP_LEAF		(1 << 0)	/* leaf page, i.e. not internal page */
#define ABTP_ROOT		(1 << 1)	/* root page (has no parent) */
#define ABTP_DELETED	(1 << 2)	/* page has been deleted from tree */
#define ABTP_META		(1 << 3)	/* meta-page */
#define ABTP_HALF_DEAD	(1 << 4)	/* empty, but still in tree */
#define ABTP_SPLIT_END	(1 << 5)	/* rightmost page of split group */
#define ABTP_HAS_GARBAGE (1 << 6)	/* page has LP_DEAD tuples */
#define ABTP_INCOMPLETE_SPLIT (1 << 7)	/* right sibling's downlink's missing */
#define ABTP_HAD_GARBAGE_REMOVED (1 << 8)	/* page had LP_DEAD tuples removed 
											 * since last vacuum
											 */

/*
 * The max allowed value of a cycle ID is a bit less than 64K.  This is
 * for convenience of pg_filedump and similar utilities: we want to use
 * the last 2 bytes of special space as an index type indicator, and
 * restricting cycle ID lets btree use that space for vacuum cycle IDs
 * while still allowing index type to be identified.
 */
#define MAX_ABT_CYCLE_ID	0xFF7F

/* TODO the map fn should take the heap tuple as input */
typedef Datum (*abt_agg_map_fn)(IndexTuple);

typedef struct ABTAggSupportData
{
	uint64				abtas_flags;
	Oid					abtas_agg_type_oid;
	RegProcedure		abtas_int2_to_agg_type_fn_oid;
	RegProcedure		abtas_agg_type_to_int2_fn_oid;
	RegProcedure		abtas_double_to_agg_type_fn_oid;
	RegProcedure		abtas_agg_type_to_double_fn_oid;
	RegProcedure		abtas_agg_type_btcmp_fn_oid;
	union {
		Datum			val;
		Size			off;
		abt_agg_map_fn	fn;
	}					abtas_map;
	union {
		RegProcedure	oid;
	}					abtas_add;	/* agg_type + agg_type */
	union {
		RegProcedure	oid;
	}					abtas_mul;	/* agg_type * int2 if available.
									 * Or agg_type * agg_type if former is
									 * not available, in which case,
									 * we set ABTAS_MUL_REQ_CAST_2ND_ARG.
									 */
	union {
		RegProcedure	oid;
	}					abtas_sub;	/* agg_type - agg_type */
	
	/* may have data at the end */
} ABTAggSupportData;

typedef ABTAggSupportData *ABTAggSupport;


#define ABTAS_MAX_SIZE					(BLCKSZ - \
										 MAXALIGN(SizeOfPageHeaderData) - \
										 MAXALIGN(sizeof(ABTMetaPageData)) - \
										 MAXALIGN(sizeof(ABTPageOpaqueData)))
#define ABTAS_SIZE_MASK					UINT64CONST(0x7FFF)
#define ABTAS_DEFINE_FLAG(x)			UINT64CONST(x) << 15
#define ABTAS_UNUSED_1					ABTAS_DEFINE_FLAG(0x0001)
#define ABTAS_UNUSED_2					ABTAS_DEFINE_FLAG(0x0002)
#define ABTAS_MAP_IS_CONST				ABTAS_DEFINE_FLAG(0x0004)
#define ABTAS_MAP_IS_C_FUNC				ABTAS_DEFINE_FLAG(0x0008)
#define ABTAS_ADD_IS_REGPROC			ABTAS_DEFINE_FLAG(0x0010)
#define ABTAS_RESERVED_1				ABTAS_DEFINE_FLAG(0x0020)
#define ABTAS_MUL_IS_REGPROC			ABTAS_DEFINE_FLAG(0x0040)
#define ABTAS_RESERVED_2				ABTAS_DEFINE_FLAG(0x0080)
#define ABTAS_MUL_REQ_CAST_2ND_ARG		ABTAS_DEFINE_FLAG(0x0100)
#define ABTAS_AGG_TYPE_IS_BYVAL			ABTAS_DEFINE_FLAG(0x0200)
#define ABTAS_SUB_IS_REGPROC			ABTAS_DEFINE_FLAG(0x0400)

#define ABTASGetStructSize(as)	(Size)((as)->abtas_flags & ABTAS_SIZE_MASK)

#define ABTASGetExtraData(as, off)	PointerGetDatum(((Pointer) (as)) + off)

typedef struct ABTCachedAggInfoData
{
	bool					agg_byval : 1;
	bool					leaf_has_agg : 1;
	bool					int2_to_agg_type_fn_initialized : 1;
	bool					agg_type_to_int2_fn_initialized : 1;
	bool					double_to_agg_type_fn_initialized : 1;
	bool					agg_type_to_double_fn_initialized : 1;
	bool					agg_is_floating_point : 1;
	char					agg_align;
	int16					agg_typlen;
	int16					agg_stride;

	Datum					agg_map_val;

	/* Must be copied before calling if this struct is stored in rd_amcache. */
	FmgrInfo				int2_to_agg_type_fn;
	FmgrInfo				agg_type_to_int2_fn;
	FmgrInfo				double_to_agg_type_fn;
	FmgrInfo				agg_type_to_double_fn;
	FmgrInfo				agg_type_btcmp_fn;
	FmgrInfo				agg_add_fn;
	FmgrInfo				agg_mul_fn;
	FmgrInfo				agg_sub_fn;
	
	Size					extra_space_for_nonpivot_tuple;
	Size					extra_space_when_tid_missing;
	Size					extra_space_when_tid_present;

	ABTAggSupportData		agg_support;
} ABTCachedAggInfoData;

typedef ABTCachedAggInfoData *ABTCachedAggInfo;

#define ABTGetCachedAggInfoSize(as) \
	(offsetof(ABTCachedAggInfoData, agg_support) + ABTASGetStructSize(as))


/*
 * The Meta page is always the first page in the btree index.
 * Its primary purpose is to point to the location of the btree root page.
 * We also point to the "fast" root, which is the current effective root;
 * see README for discussion.
 */

typedef struct ABTMetaPageData
{
	uint32		abtm_magic;		/* should contain ABTREE_MAGIC */
	uint32		abtm_version;	/* nbtree version (always <= BTREE_VERSION) */
	BlockNumber abtm_root;		/* current root location */
	uint32		abtm_level;		/* tree level of the root page */
	BlockNumber abtm_fastroot;	/* current "fast" root location */
	uint32		abtm_fastlevel;	/* tree level of the "fast" root page */
	/* remaining fields only valid when btm_version >= BTREE_NOVAC_VERSION */
	TransactionId abtm_oldest_abtpo_xact; /* oldest abtpo_xact among all deleted
										 * pages */
	float8		abtm_last_cleanup_num_heap_tuples;	/* number of heap tuples
													 * during last cleanup */
	bool		abtm_allequalimage;	/* are all columns "equalimage"? */
} ABTMetaPageData;

#define ABTPageGetMeta(p) \
	((ABTMetaPageData *) PageGetContents(p))
#define ABTMetaPageGetAggSupport(metad) \
	((ABTAggSupport) ((Pointer) metad + MAXALIGN(sizeof(ABTMetaPageData))))

/*
 * The current Btree version is 4.  That's what you'll get when you create
 * a new index.
 *
 * Btree version 3 was used in PostgreSQL v11.  It is mostly the same as
 * version 4, but heap TIDs were not part of the keyspace.  Index tuples
 * with duplicate keys could be stored in any order.  We continue to
 * support reading and writing Btree versions 2 and 3, so that they don't
 * need to be immediately re-indexed at pg_upgrade.  In order to get the
 * new heapkeyspace semantics, however, a REINDEX is needed.
 *
 * Deduplication is safe to use when the btm_allequalimage field is set to
 * true.  It's safe to read the btm_allequalimage field on version 3, but
 * only version 4 indexes make use of deduplication.  Even version 4
 * indexes created on PostgreSQL v12 will need a REINDEX to make use of
 * deduplication, though, since there is no other way to set
 * btm_allequalimage to true (pg_upgrade hasn't been taught to set the
 * metapage field).
 *
 * Btree version 2 is mostly the same as version 3.  There are two new
 * fields in the metapage that were introduced in version 3.  A version 2
 * metapage will be automatically upgraded to version 3 on the first
 * insert to it.  INCLUDE indexes cannot use version 2.
 */
#define ABTREE_METAPAGE	0		/* first page is meta */
#define ABTREE_MAGIC	0x853162	/* magic number in metapage */
#define ABTREE_VERSION	4		/* current version number */
#define ABTREE_MIN_VERSION	4	/* minimum supported version */
#define ABTREE_NOVAC_VERSION	3	/* version with all meta fields set */

/*
 * Maximum size of a btree index entry, including its tuple header.
 *
 * We actually need to be able to fit three items on every page,
 * so restrict any one item to 1/3 the per-page available space.
 *
 * There are rare cases where _bt_truncate() will need to enlarge
 * a heap index tuple to make space for a tiebreaker heap TID
 * attribute, which we account for here.
 *
 * Note: I changed how the maximum item size is calculated. This is very
 * conservative and should yield a smaller size than BTMaxItemSize(). But
 * given how we align things, it should be able to redirecting the check of
 * any non-pivot tuple that is close to the maximum to _abt_check_third_page.
 *
 */
#define ABTMaxItemSize(page, agg_info) \
	MAXALIGN_DOWN((PageGetPageSize(page) - \
				   MAXALIGN(SizeOfPageHeaderData) - \
				   MAXALIGN(3*sizeof(ItemIdData)) - \
				   3 * agg_info->extra_space_when_tid_present - \
				   MAXALIGN(sizeof(ABTPageOpaqueData))) / 3)
#define ABTMaxItemSizeNoAgg(page, agg_info) \
	MAXALIGN_DOWN((PageGetPageSize(page) - \
				   MAXALIGN(SizeOfPageHeaderData) - \
				   MAXALIGN(3*sizeof(ItemIdData)) - \
				   MAXALIGN(3 * sizeof(ItemPointerData)) - \
				   MAXALIGN(sizeof(ABTPageOpaqueData))) / 3)
#define ABTMaxItemSizeNoHeapTid(page) \
	MAXALIGN_DOWN((PageGetPageSize(page) - \
				   MAXALIGN(SizeOfPageHeaderData) - \
				   3 * MAXALIGN(sizeof(ItemIdData)) - \
				   MAXALIGN(sizeof(ABTPageOpaqueData))) / 3)

/*
 * MaxTIDsPerBTreePage is an upper bound on the number of heap TIDs tuples
 * that may be stored on a btree leaf page.  It is used to size the
 * per-page temporary buffers used by index scans.
 *
 * Note: we don't bother considering per-tuple overheads here to keep
 * things simple (value is based on how many elements a single array of
 * heap TIDs must have to fill the space between the page header and
 * special area).  The value is slightly higher (i.e. more conservative)
 * than necessary as a result, which is considered acceptable.
 */
#define MaxTIDsPerABTreePage \
	(int) ((BLCKSZ - SizeOfPageHeaderData - sizeof(ABTPageOpaqueData)) / \
		   sizeof(ItemPointerData))

/*
 * The leaf-page fillfactor defaults to 90% but is user-adjustable.
 * For pages above the leaf level, we use a fixed 70% fillfactor.
 * The fillfactor is applied during index build and when splitting
 * a rightmost page; when splitting non-rightmost pages we try to
 * divide the data equally.  When splitting a page that's entirely
 * filled with a single value (duplicates), the effective leaf-page
 * fillfactor is 96%, regardless of whether the page is a rightmost
 * page.
 */
#define ABTREE_MIN_FILLFACTOR		10
#define ABTREE_DEFAULT_FILLFACTOR	90
#define ABTREE_NONLEAF_FILLFACTOR	70
#define ABTREE_SINGLEVAL_FILLFACTOR	96

/*
 *	In general, the btree code tries to localize its knowledge about
 *	page layout to a couple of routines.  However, we need a special
 *	value to indicate "no page number" in those places where we expect
 *	page numbers.  We can use zero for this because we never need to
 *	make a pointer to the metadata page.
 */

#define ABTP_NONE			0

/*
 * Macros to test whether a page is leftmost or rightmost on its tree level,
 * as well as other state info kept in the opaque data.
 */
#define ABT_P_LEFTMOST(opaque)		((opaque)->abtpo_prev == ABTP_NONE)
#define ABT_P_RIGHTMOST(opaque)		((opaque)->abtpo_next == ABTP_NONE)
#define ABT_P_ISLEAF(opaque)		(((opaque)->abtpo_flags & ABTP_LEAF) != 0)
#define ABT_P_ISROOT(opaque)		(((opaque)->abtpo_flags & ABTP_ROOT) != 0)
#define ABT_P_ISDELETED(opaque)	\
	(((opaque)->abtpo_flags & ABTP_DELETED) != 0)
#define ABT_P_ISMETA(opaque)		(((opaque)->abtpo_flags & ABTP_META) != 0)
#define ABT_P_ISHALFDEAD(opaque)	\
	(((opaque)->abtpo_flags & ABTP_HALF_DEAD) != 0)
#define ABT_P_IGNORE(opaque)	\
	(((opaque)->abtpo_flags & (ABTP_DELETED|ABTP_HALF_DEAD)) != 0)
#define ABT_P_HAS_GARBAGE(opaque)	\
	(((opaque)->abtpo_flags & ABTP_HAS_GARBAGE) != 0)
#define ABT_P_INCOMPLETE_SPLIT(opaque)	\
	(((opaque)->abtpo_flags & ABTP_INCOMPLETE_SPLIT) != 0)
#define ABT_P_HAD_GARBAGE_REMOVED(opaque) \
	(((opaque)->abtpo_flags & ABTP_HAD_GARBAGE_REMOVED) != 0)

/*
 *	Lehman and Yao's algorithm requires a ``high key'' on every non-rightmost
 *	page.  The high key is not a tuple that is used to visit the heap.  It is
 *	a pivot tuple (see "Notes on B-Tree tuple format" below for definition).
 *	The high key on a page is required to be greater than or equal to any
 *	other key that appears on the page.  If we find ourselves trying to
 *	insert a key that is strictly > high key, we know we need to move right
 *	(this should only happen if the page was split since we examined the
 *	parent page).
 *
 *	Our insertion algorithm guarantees that we can use the initial least key
 *	on our right sibling as the high key.  Once a page is created, its high
 *	key changes only if the page is split.
 *
 *	On a non-rightmost page, the high key lives in item 1 and data items
 *	start in item 2.  Rightmost pages have no high key, so we store data
 *	items beginning in item 1.
 */

#define ABTP_HIKEY				((OffsetNumber) 1)
#define ABTP_FIRSTKEY			((OffsetNumber) 2)
#define ABT_P_FIRSTDATAKEY(opaque)	\
	(ABT_P_RIGHTMOST(opaque) ? ABTP_HIKEY : ABTP_FIRSTKEY)

inline static Datum
ABTreeComputeProductOfAggAndInt2(ABTCachedAggInfo agg_info,
								 Datum agg,
								 int16 n)
{
	Datum	n_arg;
	Datum	result;
	bool	need_free;

	if (agg_info->int2_to_agg_type_fn_initialized)
	{
		/* need to cast n to agg type first. */
		n_arg = FunctionCall1(&agg_info->int2_to_agg_type_fn,
								   Int16GetDatum(n));
		need_free = !agg_info->agg_byval;
	}
	else
	{
		/* Same type as INT2 or binary coercible. */
		n_arg = Int16GetDatum(n);
		need_free = false;
	}
	
	result = FunctionCall2(&agg_info->agg_mul_fn, agg, n_arg);

	if (need_free)
	{
		pfree(DatumGetPointer(n_arg));
	}

	return result;
}


/*
 * Notes on B-Tree tuple format, and key and non-key attributes:
 *
 * INCLUDE B-Tree indexes have non-key attributes.  These are extra
 * attributes that may be returned by index-only scans, but do not influence
 * the order of items in the index (formally, non-key attributes are not
 * considered to be part of the key space).  Non-key attributes are only
 * present in leaf index tuples whose item pointers actually point to heap
 * tuples (non-pivot tuples).  _bt_check_natts() enforces the rules
 * described here.
 *
 * Non-pivot tuple format (plain/non-posting variant):
 *
 *  t_tid | t_info | key values | INCLUDE columns, if any | [agg value] | xmin 
 *
 * t_tid points to the heap TID, which is a tiebreaker key column as of
 * BTREE_VERSION 4.
 * 
 * agg_value may be present if the map function provided by the agg_support
 * function is not constant. If present, it is guaranteed to be properly
 * aligned.
 *
 * Non-pivot tuples complement pivot tuples, which only have key columns.
 * The sole purpose of pivot tuples is to represent how the key space is
 * separated.  In general, any B-Tree index that has more than one level
 * (i.e. any index that does not just consist of a metapage and a single
 * leaf root page) must have some number of pivot tuples, since pivot
 * tuples are used for traversing the tree.  Suffix truncation can omit
 * trailing key columns when a new pivot is formed, which makes minus
 * infinity their logical value.  Since BTREE_VERSION 4 indexes treat heap
 * TID as a trailing key column that ensures that all index tuples are
 * physically unique, it is necessary to represent heap TID as a trailing
 * key column in pivot tuples, though very often this can be truncated
 * away, just like any other key column. (Actually, the heap TID is
 * omitted rather than truncated, since its representation is different to
 * the non-pivot representation.)
 *
 * In aggregate btrees, we need to store an additional agg value in the
 * pivot tuple, which appears after all key values but before the heap TID.
 * We don't need a flag for agg value for now because it's present in all
 * pivot tuples. Later, if we were to allow it to be a varlen item for
 * larger values or compression, we may need to encode the length somewhere.
 * Nevertheless, agg value's end can be determined as either size of the itup
 * if ABT_PIVOT_HEAP_TID_ATTR bit is not set, or the size of the itup less
 * 4 if the bit is set.
 *
 * Pivot tuple format:
 *
 *  t_tid | t_info | key values | agg value (at most 8 bytes) | last_update_id | [heap TID]
 *
 * We store the number of columns present inside pivot tuples by abusing
 * their t_tid offset field, since pivot tuples never need to store a real
 * offset (pivot tuples generally store a downlink in t_tid, though).  The
 * offset field only stores the number of columns/attributes when the
 * INDEX_ALT_TID_MASK bit is set, which doesn't count the trailing heap
 * TID column sometimes stored in pivot tuples -- that's represented by
 * the presence of BT_PIVOT_HEAP_TID_ATTR.  The INDEX_ALT_TID_MASK bit in
 * t_info is always set on BTREE_VERSION 4 pivot tuples, since
 * BTreeTupleIsPivot() must work reliably on heapkeyspace versions.
 *
 * last_update_id in the pivot tuple guards against concurrent split/vacuum
 * from undoing any aggregate value update when we do a second descend down the
 * tree to fix it. TODO we only need to set last_update_id in pivot tuples of
 * level 2 or above.
 *
 * In version 2 or version 3 (!heapkeyspace) indexes, INDEX_ALT_TID_MASK
 * might not be set in pivot tuples.  BTreeTupleIsPivot() won't work
 * reliably as a result.  The number of columns stored is implicitly the
 * same as the number of columns in the index, just like any non-pivot
 * tuple. (The number of columns stored should not vary, since suffix
 * truncation of key columns is unsafe within any !heapkeyspace index.)
 *
 * The 12 least significant bits from t_tid's offset number are used to
 * represent the number of key columns within a pivot tuple.  This leaves 4
 * status bits (ABT_STATUS_OFFSET_MASK bits), which are shared by all tuples
 * that have the INDEX_ALT_TID_MASK bit set (set in t_info) to store basic
 * tuple metadata.  BTreeTupleIsPivot() and BTreeTupleIsPosting() use the
 * ABT_STATUS_OFFSET_MASK bits.
 *
 * Sometimes non-pivot tuples also use a representation that repurposes
 * t_tid to store metadata rather than a TID.  PostgreSQL v13 introduced a
 * new non-pivot tuple format to support deduplication: posting list
 * tuples.  Deduplication merges together multiple equal non-pivot tuples
 * into a logically equivalent, space efficient representation.  A posting
 * list is an array of ItemPointerData elements.  Non-pivot tuples are
 * merged together to form posting list tuples lazily, at the point where
 * we'd otherwise have to split a leaf page.
 *
 * Posting tuple format (alternative non-pivot tuple representation):
 *
 *  t_tid | t_info | key values | posting list (TID array) | [agg array] | xmin array
 *
 * Posting list tuples are recognized as such by having the
 * INDEX_ALT_TID_MASK status bit set in t_info and the ABT_IS_POSTING status
 * bit set in t_tid's offset number.  These flags redefine the content of
 * the posting tuple's t_tid to store the location of the posting list
 * (instead of a block number), as well as the total number of heap TIDs
 * present in the tuple (instead of a real offset number).
 *
 * The 12 least significant bits from t_tid's offset number are used to
 * represent the number of heap TIDs present in the tuple, leaving 4 status
 * bits (the ABT_STATUS_OFFSET_MASK bits).  Like any non-pivot tuple, the
 * number of columns stored is always implicitly the total number in the
 * index (in practice there can never be non-key columns stored, since
 * deduplication is not supported with INCLUDE indexes).
 */
#define ABT_INDEX_ALT_TID_MASK		INDEX_AM_RESERVED_BIT

/* Item pointer offset bit masks */
#define ABT_OFFSET_MASK				0x0FFF
#define ABT_STATUS_OFFSET_MASK		0xF000
/* ABT_STATUS_OFFSET_MASK status bits */
#define ABT_PIVOT_HEAP_TID_ATTR		0x1000
#define ABT_IS_POSTING				0x2000
/* 
 * Unused status bits in the offsets, which may be used later for alias
 * method optimization.
 */
#define ABT_UNUSED_OFFSET_STATUS_BIT1 0x4000
#define ABT_UNUSED_OFFSET_STATUS_BIT2 0x8000


/*
 * Note: BTreeTupleIsPivot() can have false negatives (but not false
 * positives) when used with !heapkeyspace indexes
 */
static inline bool
ABTreeTupleIsPivot(IndexTuple itup)
{
	if ((itup->t_info & ABT_INDEX_ALT_TID_MASK) == 0)
		return false;
	/* absence of ABT_IS_POSTING in offset number indicates pivot tuple */
	if ((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) & ABT_IS_POSTING) != 0)
		return false;

	return true;
}

static inline bool
ABTreeTupleIsPosting(IndexTuple itup)
{
	if ((itup->t_info & ABT_INDEX_ALT_TID_MASK) == 0)
		return false;
	/* presence of ABT_IS_POSTING in offset number indicates posting tuple */
	if ((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) & ABT_IS_POSTING) == 0)
		return false;

	return true;
}

static inline void
ABTreeTupleSetPosting(IndexTuple itup, uint16 nhtids, int postingoffset)
{
	Assert(nhtids > 1);
	Assert((nhtids & ABT_STATUS_OFFSET_MASK) == 0);
	Assert((size_t) postingoffset == MAXALIGN(postingoffset));
	Assert(postingoffset < INDEX_SIZE_MASK);
	Assert(!ABTreeTupleIsPivot(itup));

	itup->t_info |= ABT_INDEX_ALT_TID_MASK;
	ItemPointerSetOffsetNumber(&itup->t_tid, (nhtids | ABT_IS_POSTING));
	ItemPointerSetBlockNumber(&itup->t_tid, postingoffset);
}

static inline uint16
ABTreeTupleGetNPosting(IndexTuple posting)
{
	OffsetNumber existing;

	Assert(ABTreeTupleIsPosting(posting));

	existing = ItemPointerGetOffsetNumberNoCheck(&posting->t_tid);
	return (existing & ABT_OFFSET_MASK);
}

static inline uint32
ABTreeTupleGetPostingOffset(IndexTuple posting)
{
	Assert(ABTreeTupleIsPosting(posting));

	return ItemPointerGetBlockNumberNoCheck(&posting->t_tid);
}

static inline ItemPointer
ABTreeTupleGetPosting(IndexTuple posting)
{
	return (ItemPointer) ((char *) posting +
						  ABTreeTupleGetPostingOffset(posting));
}

static inline ItemPointer
ABTreeTupleGetPostingN(IndexTuple posting, int n)
{
	return ABTreeTupleGetPosting(posting) + n;
}

/*
 * Get/set downlink block number in pivot tuple.
 *
 * Note: Cannot assert that tuple is a pivot tuple.  If we did so then
 * !heapkeyspace indexes would exhibit false positive assertion failures.
 */
static inline BlockNumber
ABTreeTupleGetDownLink(IndexTuple pivot)
{
	return ItemPointerGetBlockNumberNoCheck(&pivot->t_tid);
}

static inline void
ABTreeTupleSetDownLink(IndexTuple pivot, BlockNumber blkno)
{
	ItemPointerSetBlockNumber(&pivot->t_tid, blkno);
}

/*
 * Get number of attributes within tuple.
 *
 * Note that this does not include an implicit tiebreaker heap TID
 * attribute, if any.  Note also that the number of key attributes must be
 * explicitly represented in all heapkeyspace pivot tuples.
 *
 * Note: This is defined as a macro rather than an inline function to
 * avoid including rel.h.
 */
#define ABTreeTupleGetNAtts(itup, rel)	\
	( \
		(ABTreeTupleIsPivot(itup)) ? \
		( \
			ItemPointerGetOffsetNumberNoCheck(&(itup)->t_tid) & ABT_OFFSET_MASK \
		) \
		: \
		IndexRelationGetNumberOfAttributes(rel) \
	)

/*
 * Set number of key attributes in tuple.
 *
 * The heap TID tiebreaker attribute bit may also be set here, indicating that
 * a heap TID value will be stored at the end of the tuple (i.e. using the
 * special pivot tuple representation).
 */
static inline void
ABTreeTupleSetNAtts(IndexTuple itup, uint16 nkeyatts, bool heaptid)
{
	Assert(nkeyatts <= INDEX_MAX_KEYS);
	Assert((nkeyatts & ABT_STATUS_OFFSET_MASK) == 0);
	Assert(!heaptid || nkeyatts > 0);
	Assert(!ABTreeTupleIsPivot(itup) || nkeyatts == 0);

	itup->t_info |= ABT_INDEX_ALT_TID_MASK;

	if (heaptid)
		nkeyatts |= ABT_PIVOT_HEAP_TID_ATTR;

	/* ABT_IS_POSTING bit is deliberately unset here */
	ItemPointerSetOffsetNumber(&itup->t_tid, nkeyatts);
	Assert(ABTreeTupleIsPivot(itup));
}

/*
 * Get/set leaf page's "top parent" link from its high key.  Used during page
 * deletion.
 *
 * Note: Cannot assert that tuple is a pivot tuple.  If we did so then
 * !heapkeyspace indexes would exhibit false positive assertion failures.
 */
static inline BlockNumber
ABTreeTupleGetTopParent(IndexTuple leafhikey)
{
	return ItemPointerGetBlockNumberNoCheck(&leafhikey->t_tid);
}

static inline void
ABTreeTupleSetTopParent(IndexTuple leafhikey, BlockNumber blkno)
{
	ItemPointerSetBlockNumber(&leafhikey->t_tid, blkno);
	ABTreeTupleSetNAtts(leafhikey, 0, false);
}

/*
 * Get tiebreaker heap TID attribute, if any.
 *
 * This returns the first/lowest heap TID in the case of a posting list tuple.
 */
static inline ItemPointer
ABTreeTupleGetHeapTID(IndexTuple itup)
{
	if (ABTreeTupleIsPivot(itup))
	{
		/* Pivot tuple heap TID representation? */
		if ((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) &
			 ABT_PIVOT_HEAP_TID_ATTR) != 0)
			return (ItemPointer) ((char *) itup + IndexTupleSize(itup) -
								  sizeof(ItemPointerData));

		/* Heap TID attribute was truncated */
		return NULL;
	}
	else if (ABTreeTupleIsPosting(itup))
		return ABTreeTupleGetPosting(itup);

	return &itup->t_tid;
}

/*
 * Get maximum heap TID attribute, which could be the only TID in the case of
 * a non-pivot tuple that does not have a posting list tuple.
 *
 * Works with non-pivot tuples only.
 */
static inline ItemPointer
ABTreeTupleGetMaxHeapTID(IndexTuple itup)
{
	Assert(!ABTreeTupleIsPivot(itup));

	if (ABTreeTupleIsPosting(itup))
	{
		uint16		nposting = ABTreeTupleGetNPosting(itup);

		return ABTreeTupleGetPostingN(itup, nposting - 1);
	}

	return &itup->t_tid;
}

#define ABTPivotTupleHasHeapTID(itup) \
	((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) \
	 & ABT_PIVOT_HEAP_TID_ATTR) != 0)

/* Get the aggregation value's pointer for a non-pivot and non-posting tuple. */
#define ABTreeNonPivotTupleGetAggPtr(itup, agg_info) \
	((Pointer) (itup) + IndexTupleSize(itup) - \
		agg_info->extra_space_for_nonpivot_tuple)
/* Get the aggregation value's pointer for a posting (non-pivot) tuple. */
#define ABTreePostingTupleGetAggPtr(itup, agg_info) \
	((Pointer) (itup) + MAXALIGN(ABTreeTupleGetPostingOffset(itup) + \
		ABTreeTupleGetNPosting(itup) * sizeof(ItemPointerData)))
/* Get the aggregation value's pointer for a pivot tuple. */
#define ABTreePivotTupleGetAggPtr(itup, agg_info) \
	(ABTPivotTupleHasHeapTID(itup) ? \
	 ((Pointer) (itup) + IndexTupleSize(itup) - \
		agg_info->extra_space_when_tid_present) : \
	 ((Pointer) (itup) + IndexTupleSize(itup) - \
		agg_info->extra_space_when_tid_missing))

inline static Pointer
ABTreeTupleGetAggPtr(IndexTuple itup, ABTCachedAggInfo agg_info)
{
	if ((itup->t_info & ABT_INDEX_ALT_TID_MASK) == 0)
	{
		/* non-pivot and non-posting tuple */
		return ABTreeNonPivotTupleGetAggPtr(itup, agg_info);
	}
	if ((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) & ABT_IS_POSTING) != 0)
	{
		/* non-pivot posting tuple */
		return ABTreePostingTupleGetAggPtr(itup, agg_info);
	}

	/* pivot tuple */
	return ABTreePivotTupleGetAggPtr(itup, agg_info);
}

#define ABTreeNonPivotTupleGetXminPtr(itup) \
	((TransactionId *)((Pointer)(itup) + IndexTupleSize(itup) - \
		sizeof(TransactionId)))

#define ABTreePostingTupleGetXminPtr(itup) \
	((TransactionId *)((Pointer)(itup) + IndexTupleSize(itup) - \
		sizeof(TransactionId) * ABTreeTupleGetNPosting(itup)))

static inline TransactionId *
ABTreeTupleGetXminPtr(IndexTuple itup)
{
	Assert(!ABTreeTupleIsPivot(itup));
	if (ABTreeTupleIsPosting(itup))
		return ABTreePostingTupleGetXminPtr(itup);
	return ABTreeNonPivotTupleGetXminPtr(itup);
}

static inline ABTLastUpdateId *
ABTreeTupleGetLastUpdateIdPtr(IndexTuple itup)
{
	Assert(ABTreeTupleIsPivot(itup));
	
	if (ABTPivotTupleHasHeapTID(itup))
	{
		/* with heap tid */
		return (ABTLastUpdateId *)((Pointer) itup + IndexTupleSize(itup) -
			sizeof(ABTLastUpdateId) - SHORTALIGN(sizeof(ItemPointerData)));
	}
	
	/* no heap tid */
	return (ABTLastUpdateId *)((Pointer)itup + IndexTupleSize(itup) -
								sizeof(ABTLastUpdateId));
}


/*
 *	Operator strategy numbers for B-tree have been moved to access/stratnum.h,
 *	because many places need to use them in ScanKeyInit() calls.
 *
 *	Note: aggregate B-Tree uses the same strategy number names as nbtree. Hence,
 *	there's no ABTxxxStrategyNumber in access/stratnum.h.
 *
 *	The strategy numbers are chosen so that we can commute them by
 *	subtraction, thus:
 */
#define ABTCommuteStrategyNumber(strat)	(BTMaxStrategyNumber + 1 - (strat))

/*
 *	When a new operator class is declared, we require that the user
 *	supply us with an amproc procedure (BTORDER_PROC) for determining
 *	whether, for two keys a and b, a < b, a = b, or a > b.  This routine
 *	must return < 0, 0, > 0, respectively, in these three cases.
 *
 *	To facilitate accelerated sorting, an operator class may choose to
 *	offer a second procedure (BTSORTSUPPORT_PROC).  For full details, see
 *	src/include/utils/sortsupport.h.
 *
 *	To support window frames defined by "RANGE offset PRECEDING/FOLLOWING",
 *	an operator class may choose to offer a third amproc procedure
 *	(BTINRANGE_PROC), independently of whether it offers sortsupport.
 *	For full details, see doc/src/sgml/btree.sgml.
 *
 *	To facilitate B-Tree deduplication, an operator class may choose to
 *	offer a forth amproc procedure (BTEQUALIMAGE_PROC).  For full details,
 *	see doc/src/sgml/btree.sgml.
 */

#define ABTORDER_PROC		1
#define ABTSORTSUPPORT_PROC	2
#define ABTINRANGE_PROC		3
#define ABTEQUALIMAGE_PROC	4
#define ABTOPTIONS_PROC		5
#define ABTNProcs			5

/*
 *	We need to be able to tell the difference between read and write
 *	requests for pages, in order to do locking correctly.
 */

#define ABT_READ			BUFFER_LOCK_SHARE
#define ABT_WRITE		BUFFER_LOCK_EXCLUSIVE

/*
 * ABTStackData -- As we descend a tree, we push the location of pivot
 * tuples whose downlink we are about to follow onto a private stack.  If
 * we split a leaf, we use this stack to walk back up the tree and insert
 * data into its parent page at the correct location.  We also have to
 * recursively insert into the grandparent page if and when the parent page
 * splits.  Our private stack can become stale due to concurrent page
 * splits and page deletions, but it should never give us an irredeemably
 * bad picture.
 */
typedef struct ABTStackData
{
	BlockNumber			abts_blkno;
	OffsetNumber		abts_offset;
	ABTLastUpdateId		abts_last_update_id;
	struct ABTStackData	*abts_parent;

	/* These are not set up until in _abt_fix_path_aggregation_datum(). */
	uint32				abts_level;
	struct ABTStackData	*abts_child;
	ABTLastUpdateId		abts_tuple_last_update_id;
} ABTStackData;

typedef ABTStackData *ABTStack;

/*
 * BTScanInsertData is the btree-private state needed to find an initial
 * position for an indexscan, or to insert new tuples -- an "insertion
 * scankey" (not to be confused with a search scankey).  It's used to descend
 * a B-Tree using _bt_search.
 *
 * heapkeyspace indicates if we expect all keys in the index to be physically
 * unique because heap TID is used as a tiebreaker attribute, and if index may
 * have truncated key attributes in pivot tuples.  This is actually a property
 * of the index relation itself (not an indexscan).  heapkeyspace indexes are
 * indexes whose version is >= version 4.  It's convenient to keep this close
 * by, rather than accessing the metapage repeatedly.
 *
 * allequalimage is set to indicate that deduplication is safe for the index.
 * This is also a property of the index relation rather than an indexscan.
 *
 * anynullkeys indicates if any of the keys had NULL value when scankey was
 * built from index tuple (note that already-truncated tuple key attributes
 * set NULL as a placeholder key value, which also affects value of
 * anynullkeys).  This is a convenience for unique index non-pivot tuple
 * insertion, which usually temporarily unsets scantid, but shouldn't iff
 * anynullkeys is true.  Value generally matches non-pivot tuple's HasNulls
 * bit, but may not when inserting into an INCLUDE index (tuple header value
 * is affected by the NULL-ness of both key and non-key attributes).
 *
 * When nextkey is false (the usual case), _bt_search and _bt_binsrch will
 * locate the first item >= scankey.  When nextkey is true, they will locate
 * the first item > scan key.
 *
 * pivotsearch is set to true by callers that want to re-find a leaf page
 * using a scankey built from a leaf page's high key.  Most callers set this
 * to false.
 *
 * scantid is the heap TID that is used as a final tiebreaker attribute.  It
 * is set to NULL when index scan doesn't need to find a position for a
 * specific physical tuple.  Must be set when inserting new tuples into
 * heapkeyspace indexes, since every tuple in the tree unambiguously belongs
 * in one exact position (it's never set with !heapkeyspace indexes, though).
 * Despite the representational difference, nbtree search code considers
 * scantid to be just another insertion scankey attribute.
 *
 * scankeys is an array of scan key entries for attributes that are compared
 * before scantid (user-visible attributes).  keysz is the size of the array.
 * During insertion, there must be a scan key for every attribute, but when
 * starting a regular index scan some can be omitted.  The array is used as a
 * flexible array member, though it's sized in a way that makes it possible to
 * use stack allocations.  See nbtree/README for full details.
 */
typedef struct ABTScanInsertData
{
	bool		heapkeyspace;
	bool		allequalimage;
	bool		anynullkeys;
	bool		nextkey;
	bool		pivotsearch;
	ItemPointer scantid;		/* tiebreaker for scankeys */
	int			keysz;			/* Size of scankeys array */
	ScanKeyData scankeys[INDEX_MAX_KEYS];	/* Must appear last */
} ABTScanInsertData;

typedef ABTScanInsertData *ABTScanInsert;

/*
 * ABTInsertStateData is a working area used during insertion.
 *
 * This is filled in after descending the tree to the first leaf page the new
 * tuple might belong on.  Tracks the current position while performing
 * uniqueness check, before we have determined which exact page to insert
 * to.
 *
 * (This should be private to abtinsert.c, but it's also used by
 * _bt_binsrch_insert)
 */
typedef struct ABTInsertStateData
{
	IndexTuple	itup;			/* Item we're inserting */
	Size		itemsz;			/* Size of itup -- should be MAXALIGN()'d */
	ABTScanInsert itup_key;		/* Insertion scankey */

	/* Buffer containing leaf page we're likely to insert itup on */
	Buffer		buf;

	/*
	 * Cache of bounds within the current buffer.  Only used for insertions
	 * where _bt_check_unique is called.  See _bt_binsrch_insert and
	 * _bt_findinsertloc for details.
	 */
	bool		bounds_valid;
	OffsetNumber low;
	OffsetNumber stricthigh;

	/*
	 * if _bt_binsrch_insert found the location inside existing posting list,
	 * save the position inside the list.  -1 sentinel value indicates overlap
	 * with an existing posting list tuple that has its LP_DEAD bit set.
	 */
	int			postingoff;
} ABTInsertStateData;

typedef ABTInsertStateData *ABTInsertState;

/*
 * State used to representing an individual pending tuple during
 * deduplication.
 */
typedef struct ABTDedupInterval
{
	OffsetNumber baseoff;
	uint16		nitems;
} ABTDedupInterval;

/*
 * ABTDedupStateData is a working area used during deduplication.
 *
 * The status info fields track the state of a whole-page deduplication pass.
 * State about the current pending posting list is also tracked.
 *
 * A pending posting list is comprised of a contiguous group of equal items
 * from the page, starting from page offset number 'baseoff'.  This is the
 * offset number of the "base" tuple for new posting list.  'nitems' is the
 * current total number of existing items from the page that will be merged to
 * make a new posting list tuple, including the base tuple item.  (Existing
 * items may themselves be posting list tuples, or regular non-pivot tuples.)
 *
 * The total size of the existing tuples to be freed when pending posting list
 * is processed gets tracked by 'phystupsize'.  This information allows
 * deduplication to calculate the space saving for each new posting list
 * tuple, and for the entire pass over the page as a whole.
 */
typedef struct ABTDedupStateData
{
	/* Deduplication status info for entire pass over page */
	bool		deduplicate;	/* Still deduplicating page? */
	bool		save_extra;		/* Need to save the aggregation values & xmin?*/
	int			nmaxitems;		/* Number of max-sized tuples so far */
	Size		maxpostingsize; /* Limit on size of final tuple */

	/* Metadata about base tuple of current pending posting list */
	IndexTuple	base;			/* Use to form new posting list */
	OffsetNumber baseoff;		/* page offset of base */
	Size		basetupsize;	/* base size without original posting list */

	/* Other metadata about pending posting list */
	ItemPointer htids;			/* Heap TIDs in pending posting list */
	Pointer		aggs;			/* Aggregation values in the list */
	TransactionId *xmins;		/* Xmin values in the list */
	int			nhtids;			/* Number of heap TIDs in htids array */
	int			nitems;			/* Number of existing tuples/line pointers */
	Size		phystupsize;	/* Includes line pointer overhead */

	/*
	 * Array of tuples to go on new version of the page.  Contains one entry
	 * for each group of consecutive items.  Note that existing tuples that
	 * will not become posting list tuples do not appear in the array (they
	 * are implicitly unchanged by deduplication pass).
	 */
	int			nintervals;		/* current number of intervals in array */
	ABTDedupInterval intervals[MaxIndexTuplesPerPage];
} ABTDedupStateData;

typedef ABTDedupStateData *ABTDedupState;

/* 
 * An upper bound of the number of TIDs that can be included in a posting list.
 * Be very conservative about this value because the posting lists during
 * btree bulk loading can be larger for some reason...
 */
#define ABTDedupMaxNumberOfTIDs(state)	\
	(state->maxpostingsize / sizeof(ItemPointerData)) + 1

/*
 * BTVacuumPostingData is state that represents how to VACUUM a posting list
 * tuple when some (though not all) of its TIDs are to be deleted.
 *
 * Convention is that itup field is the original posting list tuple on input,
 * and palloc()'d final tuple used to overwrite existing tuple on output.
 */
typedef struct ABTVacuumPostingData
{
	/* Tuple that will be/was updated */
	IndexTuple	itup;
	OffsetNumber updatedoffset;

	/* State needed to describe final itup in WAL */
	uint16		ndeletedtids;
	uint16		deletetids[FLEXIBLE_ARRAY_MEMBER];
} ABTVacuumPostingData;

typedef ABTVacuumPostingData *ABTVacuumPosting;

/*
 * BTScanOpaqueData is the btree-private state needed for an indexscan.
 * This consists of preprocessed scan keys (see _bt_preprocess_keys() for
 * details of the preprocessing), information about the current location
 * of the scan, and information about the marked location, if any.  (We use
 * BTScanPosData to represent the data needed for each of current and marked
 * locations.)	In addition we can remember some known-killed index entries
 * that must be marked before we can move off the current page.
 *
 * Index scans work a page at a time: we pin and read-lock the page, identify
 * all the matching items on the page and save them in BTScanPosData, then
 * release the read-lock while returning the items to the caller for
 * processing.  This approach minimizes lock/unlock traffic.  Note that we
 * keep the pin on the index page until the caller is done with all the items
 * (this is needed for VACUUM synchronization, see nbtree/README).  When we
 * are ready to step to the next page, if the caller has told us any of the
 * items were killed, we re-lock the page to mark them killed, then unlock.
 * Finally we drop the pin and step to the next page in the appropriate
 * direction.
 *
 * If we are doing an index-only scan, we save the entire IndexTuple for each
 * matched item, otherwise only its heap TID and offset.  The IndexTuples go
 * into a separate workspace array; each BTScanPosItem stores its tuple's
 * offset within that array.  Posting list tuples store a "base" tuple once,
 * allowing the same key to be returned for each TID in the posting list
 * tuple.
 */

typedef struct ABTScanPosItem	/* what we remember about each match */
{
	ItemPointerData heapTid;	/* TID of referenced heap item */
	OffsetNumber indexOffset;	/* index item's location within page */
	LocationIndex tupleOffset;	/* IndexTuple's offset in workspace, if any */
} ABTScanPosItem;

typedef struct ABTScanPosData
{
	Buffer		buf;			/* if valid, the buffer is pinned */

	XLogRecPtr	lsn;			/* pos in the WAL stream when page was read */
	BlockNumber currPage;		/* page referenced by items array */
	BlockNumber nextPage;		/* page's right link when we scanned it */

	/*
	 * moreLeft and moreRight track whether we think there may be matching
	 * index entries to the left and right of the current page, respectively.
	 * We can clear the appropriate one of these flags when _bt_checkkeys()
	 * returns continuescan = false.
	 */
	bool		moreLeft;
	bool		moreRight;

	/*
	 * If we are doing an index-only scan, nextTupleOffset is the first free
	 * location in the associated tuple storage workspace.
	 */
	int			nextTupleOffset;

	/*
	 * The items array is always ordered in index order (ie, increasing
	 * indexoffset).  When scanning backwards it is convenient to fill the
	 * array back-to-front, so we start at the last slot and fill downwards.
	 * Hence we need both a first-valid-entry and a last-valid-entry counter.
	 * itemIndex is a cursor showing which entry was last returned to caller.
	 */
	int			firstItem;		/* first valid index in items[] */
	int			lastItem;		/* last valid index in items[] */
	int			itemIndex;		/* current index in items[] */

	ABTScanPosItem items[MaxTIDsPerABTreePage];	/* MUST BE LAST */
} ABTScanPosData;

typedef ABTScanPosData *ABTScanPos;

#define ABTScanPosIsPinned(scanpos) \
( \
	AssertMacro(BlockNumberIsValid((scanpos).currPage) || \
				!BufferIsValid((scanpos).buf)), \
	BufferIsValid((scanpos).buf) \
)
#define ABTScanPosUnpin(scanpos) \
	do { \
		ReleaseBuffer((scanpos).buf); \
		(scanpos).buf = InvalidBuffer; \
	} while (0)
#define ABTScanPosUnpinIfPinned(scanpos) \
	do { \
		if (ABTScanPosIsPinned(scanpos)) \
			ABTScanPosUnpin(scanpos); \
	} while (0)

#define ABTScanPosIsValid(scanpos) \
( \
	AssertMacro(BlockNumberIsValid((scanpos).currPage) || \
				!BufferIsValid((scanpos).buf)), \
	BlockNumberIsValid((scanpos).currPage) \
)
#define ABTScanPosInvalidate(scanpos) \
	do { \
		(scanpos).currPage = InvalidBlockNumber; \
		(scanpos).nextPage = InvalidBlockNumber; \
		(scanpos).buf = InvalidBuffer; \
		(scanpos).lsn = InvalidXLogRecPtr; \
		(scanpos).nextTupleOffset = 0; \
	} while (0)

/* We need one of these for each equality-type SK_SEARCHARRAY scan key */
typedef struct ABTArrayKeyInfo
{
	int			scan_key;		/* index of associated key in arrayKeyData */
	int			cur_elem;		/* index of current element in elem_values */
	int			mark_elem;		/* index of marked element in elem_values */
	int			num_elems;		/* number of elems in current array value */
	Datum	   *elem_values;	/* array of num_elems Datums */
} ABTArrayKeyInfo;

typedef struct ABTSnapshotData
{
	TransactionId	xmin;
	TransactionId	xmax;
	uint32			xcnt;

	/* variable length array, xcnt in size (xcnt may be 0) */
	TransactionId	xip[1];
} ABTSnapshotData;

typedef ABTSnapshotData *ABTSnapshot;

typedef struct ABTScanOpaqueData
{
	/* these fields are set by _abt_preprocess_keys(): */
	bool		qual_ok;		/* false if qual can never be satisfied */
	bool		preprocessed;	/* whether we have called _abt_preprocess_keys*/
	int			numberOfKeys;	/* number of preprocessed scan keys */
	ScanKey		keyData;		/* array of preprocessed scan keys */

	/* workspace for SK_SEARCHARRAY support */
	ScanKey		arrayKeyData;	/* modified copy of scan->keyData */
	int			numArrayKeys;	/* number of equality-type array keys (-1 if
								 * there are any unsatisfiable array keys) */
	int			arrayKeyCount;	/* count indicating number of array scan keys
								 * processed */
	ABTArrayKeyInfo *arrayKeys;	/* info about each equality-type array key */
	MemoryContext arrayContext; /* scan-lifespan context for array data */

	/* info about killed items if any (killedItems is NULL if never used) */
	int		   *killedItems;	/* currPos.items indexes of killed items */
	int			numKilled;		/* number of currently stored items */

	/*
	 * If we are doing an index-only scan, these are the tuple storage
	 * workspaces for the currPos and markPos respectively.  Each is of size
	 * BLCKSZ, so it can hold as much as a full page's worth of tuples.
	 */
	char	   *currTuples;		/* tuple storage for currPos */
	char	   *markTuples;		/* tuple storage for markPos */

	/*
	 * If the marked position is on the same page as current position, we
	 * don't use markPos, but just keep the marked itemIndex in markItemIndex
	 * (all the rest of currPos is valid for the mark position). Hence, to
	 * determine if there is a mark, first look at markItemIndex, then at
	 * markPos.
	 */
	int			markItemIndex;	/* itemIndex, or -1 if not valid */

	/* Set up by _abt_build_start_inskey(). NULL if not set. */
	ABTScanInsert	inskey;
	bool			goback;

	ABTCachedAggInfo agg_info; /* a local copy of the cached agg info */
	ABTScanInsert	inskey_low;
	ABTScanInsert	inskey_high;
	ABTScanInsert	inskey_pivot;
#ifdef USE_ASSERT_CHECKING
	int				max_num_items;	/* only for debug */
#endif
	MemoryContext	prefix_sums_context;

	/* Stores the prefix sums of aggregation values in the root page. */
	/*Datum			*prefix_sums; */
	uint64			*prefix_sums;

	/* 
	 * Stores the offsets of the items that corresponding to the prefix sums.
	 *
	 * The lower 16-bits stores the offset in the page. If that tuple is a
	 * posting tuple, then we store the item's posting list offset in the upper
	 * 16-bits. See the macros below for details.
	 */
	uint32			*item_indexes;
    
    /* 
     * The reciprocal of the probability if the tree/matching range is
     * non-empty. Set to 0.0 if the tree/matching range is empty.
     */
    double          inv_prob;
	
	bool			snapshot_prepared;
	ABTSnapshot		snapshot;

	/* Sampling related fields. */

	/* keep these last in struct for efficiency */
	ABTScanPosData currPos;		/* current position data */
	ABTScanPosData markPos;		/* marked position, if any */
} ABTScanOpaqueData;

typedef ABTScanOpaqueData *ABTScanOpaque;

#define ABT_ITEMINDEX_OFFSET_MASK	0xffffu
#define ABT_ITEMINDEX_POSTING_OFF_MASK	0xffff0000u
#define ABT_ITEMINDEX_POSTING_OFF_SHIFT 16
#define ABT_ITEMINDEX_GET_OFFSET(idx) ((idx) & ABT_ITEMINDEX_OFFSET_MASK)
#define ABT_ITEMINDEX_GET_POSTING_OFF(idx) \
	(((idx) & ABT_ITEMINDEX_POSTING_OFF_MASK) >> \
		ABT_ITEMINDEX_POSTING_OFF_SHIFT)
#define ABT_OFFSET_GET_ITEMINDEX(offset) \
	(((uint32) (offset)) & ABT_ITEMINDEX_OFFSET_MASK)
#define ABT_OFFSET_AND_POSTING_OFF_GET_ITEMINDEX(offset, posting_off) \
	(ABT_OFFSET_GET_ITEMINDEX(offset) | \
	 ((((uint32) (posting_off)) & ABT_ITEMINDEX_POSTING_OFF_MASK) << \
	  ABT_ITEMINDEX_POSTING_OFF_SHIFT))

/*
 * We use some private sk_flags bits in preprocessed scan keys.  We're allowed
 * to use bits 16-31 (see skey.h).  The uppermost bits are copied from the
 * index's indoption[] array entry for the index attribute.
 */
#define SK_ABT_REQFWD	0x00010000	/* required to continue forward scan */
#define SK_ABT_REQBKWD	0x00020000	/* required to continue backward scan */
#define SK_ABT_INDOPTION_SHIFT  24	/* must clear the above bits */
#define SK_ABT_DESC			(INDOPTION_DESC << SK_ABT_INDOPTION_SHIFT)
#define SK_ABT_NULLS_FIRST	(INDOPTION_NULLS_FIRST << SK_ABT_INDOPTION_SHIFT)

typedef struct ABTOptions
{
	int32		varlena_header_;	/* varlena header (do not touch directly!) */
	int			fillfactor;		/* page fill factor in percent (0..100) */
	/* fraction of newly inserted tuples prior to trigger index cleanup */
	float8		vacuum_cleanup_index_scale_factor;
	bool		deduplicate_items;	/* Try to deduplicate items? */
	
	struct {
		Oid		typeOid;
		int32	typemod;
	}			aggregation_type;
	RegProcedure	agg_support_fn;
} ABTOptions;

#define ABTGetFillFactor(relation) \
	(AssertMacro(relation->rd_rel->relkind == RELKIND_INDEX && \
				 relation->rd_rel->relam == ABTREE_AM_OID), \
	 (relation)->rd_options ? \
	 ((ABTOptions *) (relation)->rd_options)->fillfactor : \
	 ABTREE_DEFAULT_FILLFACTOR)
#define ABTGetTargetPageFreeSpace(relation) \
	(BLCKSZ * (100 - ABTGetFillFactor(relation)) / 100)
#define ABTGetDeduplicateItems(relation) \
	(AssertMacro(relation->rd_rel->relkind == RELKIND_INDEX && \
				 relation->rd_rel->relam == ABTREE_AM_OID), \
	((relation)->rd_options ? \
	 ((ABTOptions *) (relation)->rd_options)->deduplicate_items : true))

/*
 * Constant definition for progress reporting.  Phase numbers must match
 * btbuildphasename.
 */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 (see progress.h) */
#define PROGRESS_ABTREE_PHASE_INDEXBUILD_TABLESCAN		2
#define PROGRESS_ABTREE_PHASE_PERFORMSORT_1				3
#define PROGRESS_ABTREE_PHASE_PERFORMSORT_2				4
#define PROGRESS_ABTREE_PHASE_LEAF_LOAD					5

typedef struct ABTAggVersion
{
	/* Next in the chain. Either in free list or in hash table. */
	struct ABTAggVersion	*next;
} ABTAggVersion;

/*
 * external entry points for btree, in abtree.c
 */
extern void abtbuildempty(Relation index);
extern bool abtinsert(Relation rel, Datum *values, bool *isnull,
					 ItemPointer ht_ctid, Relation heapRel,
					 IndexUniqueCheck checkUnique,
					 struct IndexInfo *indexInfo);
extern IndexScanDesc abtbeginscan(Relation rel, int nkeys, int norderbys);
extern Size abtestimateparallelscan(void);
extern void abtinitparallelscan(void *target);
extern bool abtgettuple(IndexScanDesc scan, ScanDirection dir);
extern bool abtsampletuple(IndexScanDesc scan, double random_number);
extern int64 abtgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void abtrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
					 ScanKey orderbys, int norderbys);
extern void abtparallelrescan(IndexScanDesc scan);
extern void abtendscan(IndexScanDesc scan);
extern void abtmarkpos(IndexScanDesc scan);
extern void abtrestrpos(IndexScanDesc scan);
extern IndexBulkDeleteResult *abtbulkdelete(IndexVacuumInfo *info,
										   IndexBulkDeleteResult *stats,
										   IndexBulkDeleteCallback callback,
										   void *callback_state);
extern IndexBulkDeleteResult *abtvacuumcleanup(IndexVacuumInfo *info,
											  IndexBulkDeleteResult *stats);
extern bool abtcanreturn(Relation index, int attno);

/*
 * prototypes for internal functions in abtree.c
 */
extern bool _abt_parallel_seize(IndexScanDesc scan, BlockNumber *pageno);
extern void _abt_parallel_release(IndexScanDesc scan, BlockNumber scan_page);
extern void _abt_parallel_done(IndexScanDesc scan);
extern void _abt_parallel_advance_array_keys(IndexScanDesc scan);

/*
 * prototypes for functions in abtdedup.c
 */
extern void _abt_dedup_one_page(Relation rel, Buffer buf, Relation heapRel,
							   IndexTuple newitem, Size newitemsz,
							   bool checkingunique,
							   ABTCachedAggInfo agg_info);
extern void _abt_dedup_start_pending(ABTDedupState state, IndexTuple base,
									OffsetNumber baseoff,
									ABTCachedAggInfo agg_info);
extern bool _abt_dedup_save_htid(ABTDedupState state, IndexTuple itup,
								 ABTCachedAggInfo agg_info);
extern Size _abt_dedup_finish_pending(Page newpage, ABTDedupState state,
									  ABTCachedAggInfo agg_info);
extern IndexTuple _abt_form_posting(IndexTuple base, ItemPointer htids,
								   int nhtids, ABTCachedAggInfo agg_info);
extern void _abt_update_posting(ABTVacuumPosting vacposting,
								ABTCachedAggInfo agg_info);
extern IndexTuple _abt_swap_posting(IndexTuple newitem, IndexTuple oposting,
								   int postingoff, ABTCachedAggInfo agg_info);

/*
 * prototypes for functions in abtinsert.c
 */
extern bool _abt_doinsert(Relation rel, IndexTuple itup,
						 IndexUniqueCheck checkUnique, Relation heapRel,
						 ABTCachedAggInfo agg_info);
extern void _abt_finish_split(Relation rel, Buffer lbuf, ABTStack stack,
							  ABTCachedAggInfo agg_info);
extern Buffer _abt_getstackbuf(Relation rel, ABTStack stack, BlockNumber child,
							   ABTCachedAggInfo agg_info);

/*
 * prototypes for functions in abtsplitloc.c
 */
extern OffsetNumber _abt_findsplitloc(Relation rel, Page origpage,
									 OffsetNumber newitemoff, Size newitemsz,
									 IndexTuple newitem,
									 bool *newitemonleft,
									 ABTCachedAggInfo agg_info);

/*
 * prototypes for functions in abtpage.c
 */
extern void _abt_initmetapage(ABTAggSupport options, 
							  Page page, BlockNumber rootbknum, uint32 level,
							  bool allequalimage);
extern void _abt_update_meta_cleanup_info(Relation rel,
										 TransactionId oldestABtpoXact,
										 float8 numHeapTuples);
extern void _abt_upgrademetapage(Page page);
extern Buffer _abt_getroot(Relation rel, int access, bool reject_non_fastroot,
						   ABTCachedAggInfo agg_info);
extern Buffer _abt_gettrueroot(Relation rel);
extern int	_abt_getrootheight(Relation rel);
extern void _abt_metaversion(Relation rel, bool *heapkeyspace,
							bool *allequalimage);
extern void _abt_checkpage(Relation rel, Buffer buf);
extern Buffer _abt_getbuf(Relation rel, BlockNumber blkno, int access);
extern Buffer _abt_relandgetbuf(Relation rel, Buffer obuf,
							   BlockNumber blkno, int access);
extern void _abt_relbuf(Relation rel, Buffer buf);
extern void _abt_pageinit(Page page, Size size);
extern bool _abt_page_recyclable(Page page);
extern void _abt_delitems_vacuum(Relation rel, Buffer buf,
								OffsetNumber *deletable, int ndeletable,
								ABTVacuumPosting *updatable, int nupdatable,
								ABTCachedAggInfo agg_info);
extern void _abt_delitems_delete(Relation rel, Buffer buf,
								OffsetNumber *deletable, int ndeletable,
								Relation heapRel);
extern uint32 _abt_pagedel(Relation rel, Buffer leafbuf,
						  TransactionId *oldestABtpoXact,
						  ABTCachedAggInfo agg_info);
extern void _abt_decrement_aggs_for_vacuum(Relation rel, Buffer leafbuf,
                                           Datum deleted_sum,
                                           ABTCachedAggInfo agg_info);

/*
 * prototypes for functions in abtsearch.c
 */
extern ABTStack _abt_search_extended(
        Relation rel, ABTScanInsert key, Buffer *bufP,
	    int access, Snapshot snapshot,
	    ABTCachedAggInfo agg_info, uint32 level);
static inline ABTStack
_abt_search(Relation rel, ABTScanInsert key, Buffer *bufP,
		    int access, Snapshot snapshot,
			ABTCachedAggInfo agg_info)
{
    return _abt_search_extended(rel, key, bufP, access, snapshot, agg_info, 0);
}
extern OffsetNumber _abt_binsrch(Relation rel, ABTScanInsert key, Buffer buf);
extern int _abt_binsrch_posting(ABTScanInsert key, Page page,
								OffsetNumber offnum);
extern Buffer _abt_moveright(Relation rel, ABTScanInsert key, Buffer buf,
							bool forupdate, ABTStack stack, int access,
							Snapshot snapshot, ABTCachedAggInfo agg_info);
extern OffsetNumber _abt_binsrch_insert(Relation rel,
							ABTInsertState insertstate);
extern int32 _abt_compare(Relation rel, ABTScanInsert key, Page page,
						OffsetNumber offnum);
extern bool _abt_first(IndexScanDesc scan, ScanDirection dir);
extern bool _abt_next(IndexScanDesc scan, ScanDirection dir);
extern Buffer _abt_get_endpoint(Relation rel, uint32 level, bool rightmost,
							   Snapshot snapshot);
extern Buffer _abt_get_endpoint_stack(Relation rel, uint32 level,
                                      bool rightmost,
							          Snapshot snapshot, ABTStack *p_stack);
extern bool _abt_sample(IndexScanDesc scan, double random_number);
extern void _abt_build_start_inskey(IndexScanDesc scan, ScanDirection dir);

/*
 * prototypes for functions in abtutils.c
 */
#define _abt_mkscankey(rel, itup) _abt_mkscankey_extended(rel, itup, NULL)
extern ABTScanInsert _abt_mkscankey_extended(Relation rel, IndexTuple itup,
											 ABTScanInsert key);
extern void _abt_freestack(Relation rel, ABTStack stack);
extern void _abt_preprocess_array_keys(IndexScanDesc scan);
extern void _abt_start_array_keys(IndexScanDesc scan, ScanDirection dir);
extern bool _abt_advance_array_keys(IndexScanDesc scan, ScanDirection dir);
extern void _abt_mark_array_keys(IndexScanDesc scan);
extern void _abt_restore_array_keys(IndexScanDesc scan);
extern void _abt_preprocess_keys(IndexScanDesc scan);
extern bool _abt_checkkeys(IndexScanDesc scan, IndexTuple tuple,
						  int tupnatts, ScanDirection dir, bool *continuescan);
extern void _abt_killitems(IndexScanDesc scan);
extern ABTCycleId _abt_vacuum_cycleid(Relation rel);
extern ABTCycleId _abt_start_vacuum(Relation rel);
extern void _abt_end_vacuum(Relation rel);
extern void _abt_end_vacuum_callback(int code, Datum arg);
extern Size ABTreeShmemSize(void);
extern void ABTreeShmemInit(void);
extern bytea *abtoptions(Datum reloptions, bool validate);
extern void abt_get_arg_info_for_combine_fn(void *options,
											int *nargs,
											Oid *argtypes);
extern bool abtproperty(Oid index_oid, int attno,
					   IndexAMProperty prop, const char *propname,
					   bool *res, bool *isnull);
extern char *abtbuildphasename(int64 phasenum);
extern IndexTuple _abt_truncate(Relation rel, IndexTuple lastleft,
							   IndexTuple firstright, ABTScanInsert itup_key,
							   ABTCachedAggInfo agg_info);
extern int	_abt_keep_natts_fast(Relation rel, IndexTuple lastleft,
								IndexTuple firstright);
extern bool _abt_check_natts(Relation rel, bool heapkeyspace, Page page,
							OffsetNumber offnum);
extern void _abt_check_third_page(Relation rel, Relation heap,
								 Page page,
								 IndexTuple newtup,
								 ABTCachedAggInfo agg_info);
extern bool _abt_allequalimage(Relation rel, bool debugmessage);
extern IndexTuple _abt_pivot_tuple_purge_agg_space(IndexTuple itup,
												   ABTCachedAggInfo agg_info);
extern IndexTuple _abt_pivot_tuple_reserve_agg_space(IndexTuple itup,
												ABTCachedAggInfo agg_info);
extern void _abt_set_pivot_tuple_aggregation_value(IndexTuple itup,
												Datum agg_val,
												ABTCachedAggInfo agg_info);
extern void _abt_set_nonpivot_tuple_aggregation_value(IndexTuple itup,
												Datum agg_val,
												ABTCachedAggInfo agg_info);
extern void _abt_set_posting_tuple_aggregation_values(IndexTuple itup,
												Datum agg_val,
												ABTCachedAggInfo agg_info);
extern ABTCachedAggInfo _abt_get_cached_agg_info(Relation rel,
												  bool missing_ok);
extern ABTCachedAggInfo	_abt_copy_cached_agg_info(Relation rel,
												  MemoryContext mcxt);
extern ABTMetaPageData *_abt_get_cached_metapage(Relation rel);
extern void _abt_save_metapage(Relation rel, ABTMetaPageData *metad,
							   ABTAggSupport agg_support);
extern void _abt_clear_cached_metapage(Relation rel);
extern void _abt_clear_cache(Relation rel);
extern void _abt_fill_agg_info(ABTCachedAggInfo agg_info,
							   ABTAggSupport agg_support);
extern void _abt_compute_tuple_offsets(ABTCachedAggInfo agg_info);
extern Datum _abt_accum_value_nofree(ABTCachedAggInfo agg_info,
									 Datum acc, Datum value_to_add,
									 bool is_first);
extern void _abt_accum_value(ABTCachedAggInfo agg_info,
							 Datum *acc, Datum value_to_add, bool is_first);
extern void _abt_exclude_value(ABTCachedAggInfo agg_info,
							   Datum *acc, Datum value_to_subtract);
extern void _abt_accum_index_tuple_value(ABTCachedAggInfo agg_info,
										 Datum *agg,
										 IndexTuple itup,
										 bool is_leaf,
										 bool is_first);
extern void _abt_exclude_index_tuple_value(ABTCachedAggInfo agg_info,
										   Datum *agg,
										   IndexTuple itup,
										   bool is_leaf);
extern Datum _abt_accum_page_value(ABTCachedAggInfo agg_info, Buffer buf);
extern ABTStack _abt_inverse_stack(ABTStack stack);
extern ABTStack _abt_stack_setup_child_link(ABTStack stack);

static inline uint64
_abt_atomic_read_agg_ptr(Pointer agg_ptr, ABTCachedAggInfo agg_info)
{
	if (agg_info->agg_typlen == 8)
	{
		return pg_atomic_read_u64((pg_atomic_uint64 *) agg_ptr);
	}
	else if (agg_info->agg_typlen == 4)
	{
		return pg_atomic_read_u32((pg_atomic_uint32 *) agg_ptr);
	}

	return (uint16) pg_atomic_read_u32((pg_atomic_uint32 *) agg_ptr);
}

/*
 * prototypes for functions in abtvalidate.c
 */
extern bool abtvalidate(Oid opclassoid);

/*
 * prototypes for functions in abtsort.c
 */
extern IndexBuildResult *abtbuild(Relation heap, Relation index,
								 struct IndexInfo *indexInfo);
extern void _abt_parallel_build_main(dsm_segment *seg, shm_toc *toc);

/*
 * prototypes for functions in abtversionstore.c
 */
extern Size ABTreeVersionStoreShmemSize(void);
extern void ABTreeVersionStoreShmemInit(void);
extern void ABTreeVersionStoreSetup(void);
extern Datum _abt_compute_page_aggregate_and_version_chain(Relation rel,
													Buffer buf,
													uint64 *opaque_p_chain,
													ABTCachedAggInfo agg_info);
extern void _abt_install_version_chain(Relation rel, BlockNumber blkno,
									   uint64 opaque_chain);
extern void _abt_add_to_version(Relation rel, BlockNumber blkno,
								TransactionId xmin, Datum dval,
								ABTCachedAggInfo agg_info);
extern ABTSnapshot _abt_prepare_snapshot_for_sampling(Snapshot snapshot);
extern bool _abt_xid_in_snapshot(TransactionId xid, ABTSnapshot snapshot);
extern uint64 _abt_read_agg(Relation rel, IndexTuple itup, ABTSnapshot snapshot,
							ABTCachedAggInfo agg_info);
extern void ABTreeRegisterGarbageCollector(void);
extern void ABTreeGarbageCollectorMain(Datum main_arg);


#endif  /* ABTREE_H */
