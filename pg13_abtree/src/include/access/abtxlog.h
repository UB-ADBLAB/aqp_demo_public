/*-------------------------------------------------------------------------
 *
 * nbtxlog.h
 *	  header file for aggregate b-tree xlog routines
 *
 *	  Copied and adapted from src/include/access/nbtxlog.h
 *
 * src/include/access/nbtxlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ABTXLOG_H
#define ABTXLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/off.h"

/*
 * XLOG records for btree operations
 *
 * XLOG allows to store some information in high 4 bits of log
 * record xl_info field
 */
#define XLOG_ABTREE_INSERT_LEAF	0x00	/* add index tuple without split */
#define XLOG_ABTREE_INSERT_UPPER 0x10	/* same, on a non-leaf page */
#define XLOG_ABTREE_INSERT_META	0x20	/* same, plus update metapage */
#define XLOG_ABTREE_SPLIT_L		0x30	/* add index tuple with split */
#define XLOG_ABTREE_SPLIT_R		0x40	/* as above, new item on right */
#define XLOG_ABTREE_INSERT_POST	0x50	/* add index tuple with posting split */
#define XLOG_ABTREE_DEDUP		0x60	/* deduplicate tuples for a page */
#define XLOG_ABTREE_DELETE		0x70	/* delete leaf index tuples for a page
										 * */
#define XLOG_ABTREE_UNLINK_PAGE	0x80	/* delete a half-dead page */
#define XLOG_ABTREE_UNLINK_PAGE_META 0x90	/* same, and update metapage */
#define XLOG_ABTREE_NEWROOT		0xA0	/* new root page */
#define XLOG_ABTREE_MARK_PAGE_HALFDEAD 0xB0	/* mark a leaf as half-dead */
#define XLOG_ABTREE_VACUUM		0xC0	/* delete entries on a page during
			    						 * vacuum */
#define XLOG_ABTREE_REUSE_PAGE	0xD0	/* old page is about to be reused from
			    						 * FSM */
#define XLOG_ABTREE_META_CLEANUP	0xE0	/* update cleanup-related data in
										 * the metapage */
#define XLOG_ABTREE_AGG_OP		0xF0	/* aggregation related op, need to
										   consult the struct header to
										   find out what it is. */

/* Forward declaration to avoid including abtree.h */
struct ABTCachedAggInfoData;

typedef struct xl_abtree_minimal_agg_info
{
	int16		agg_typlen;
	int16		agg_stride;
} xl_abtree_minimal_agg_info;

#define SizeOfABtreeMinimalAggInfo \
	(offsetof(xl_abtree_minimal_agg_info, agg_typlen) \
	 + sizeof(int16))

/*
 * All that we need to regenerate the meta-data page
 *
 * We also need ABTAggSupportData at the end.
 *
 */
typedef struct xl_abtree_metadata
{
	uint32		version;
	BlockNumber root;
	uint32		level;
	BlockNumber fastroot;
	uint32		fastlevel;
	TransactionId oldest_abtpo_xact;
	float8		last_cleanup_num_heap_tuples;
	bool		allequalimage;
} xl_abtree_metadata;

/*
 * This is what we need to know about simple (without split) insert.
 *
 * This data record is used for INSERT_LEAF, INSERT_UPPER, INSERT_META, and
 * INSERT_POST.  Note that INSERT_META and INSERT_UPPER implies it's not a
 * leaf page, while INSERT_POST and INSERT_LEAF imply that it must be a leaf
 * page.
 *
 * Backup Blk 0: original page
 * Backup Blk 1: child's left sibling, if INSERT_UPPER or INSERT_META
 * Backup Blk 2: xl_abtree_metadata, if INSERT_META
 *
 * Note: The new tuple is actually the "original" new item in the posting
 * list split insert case (i.e. the INSERT_POST case).  A split offset for
 * the posting list is logged before the original new item.  Recovery needs
 * both, since it must do an in-place update of the existing posting list
 * that was split as an extra step.  Also, recovery generates a "final"
 * newitem.  See _bt_swap_posting() for details on posting list splits.
 */
typedef struct xl_abtree_insert
{
	OffsetNumber offnum;

	/* POSTING SPLIT OFFSET FOLLOWS (INSERT_POST case) */
	/* NEW TUPLE ALWAYS FOLLOWS AT THE END */
} xl_abtree_insert;

#define SizeOfABtreeInsert	(offsetof(xl_abtree_insert, offnum) + \
							sizeof(OffsetNumber))

/*
 * On insert with split, we save all the items going into the right sibling
 * so that we can restore it completely from the log record.  This way takes
 * less xlog space than the normal approach, because if we did it standardly,
 * XLogInsert would almost always think the right page is new and store its
 * whole page image.  The left page, however, is handled in the normal
 * incremental-update fashion.
 *
 * Note: XLOG_BTREE_SPLIT_L and XLOG_BTREE_SPLIT_R share this data record.
 * There are two variants to indicate whether the inserted tuple went into the
 * left or right split page (and thus, whether the new item is stored or not).
 * We always log the left page high key because suffix truncation can generate
 * a new leaf high key using user-defined code.  This is also necessary on
 * internal pages, since the firstright item that the left page's high key was
 * based on will have been truncated to zero attributes in the right page (the
 * separator key is unavailable from the right page).
 *
 * Backup Blk 0: original page / new left page
 *
 * The left page's data portion contains the new item, if it's the _L variant.
 * _R variant split records generally do not have a newitem (_R variant leaf
 * page split records that must deal with a posting list split will include an
 * explicit newitem, though it is never used on the right page -- it is
 * actually an orignewitem needed to update existing posting list).  The new
 * high key of the left/original page appears last of all (and must always be
 * present).
 *
 * Page split records that need the REDO routine to deal with a posting list
 * split directly will have an explicit newitem, which is actually an
 * orignewitem (the newitem as it was before the posting list split, not
 * after).  A posting list split always has a newitem that comes immediately
 * after the posting list being split (which would have overlapped with
 * orignewitem prior to split).  Usually REDO must deal with posting list
 * splits with an _L variant page split record, and usually both the new
 * posting list and the final newitem go on the left page (the existing
 * posting list will be inserted instead of the old, and the final newitem
 * will be inserted next to that).  However, _R variant split records will
 * include an orignewitem when the split point for the page happens to have a
 * lastleft tuple that is also the posting list being split (leaving newitem
 * as the page split's firstright tuple).  The existence of this corner case
 * does not change the basic fact about newitem/orignewitem for the REDO
 * routine: it is always state used for the left page alone.  (This is why the
 * record's postingoff field isn't a reliable indicator of whether or not a
 * posting list split occurred during the page split; a non-zero value merely
 * indicates that the REDO routine must reconstruct a new posting list tuple
 * that is needed for the left page.)
 *
 * This posting list split handling is equivalent to the xl_abtree_insert REDO
 * routine's INSERT_POST handling.  While the details are more complicated
 * here, the concept and goals are exactly the same.  See _bt_swap_posting()
 * for details on posting list splits.
 *
 * Backup Blk 1: new right page
 *
 * The right page's data portion contains the right page's tuples in the form
 * used by _bt_restore_page.  This includes the new item, if it's the _R
 * variant.  The right page's tuples also include the right page's high key
 * with either variant (moved from the left/original page during the split),
 * unless the split happened to be of the rightmost page on its level, where
 * there is no high key for new right page.
 *
 * Backup Blk 2: next block (orig page's rightlink), if any
 * Backup Blk 3: child's left sibling, if non-leaf split
 */
typedef struct xl_abtree_split
{
	uint32		level;			/* tree level of page being split */
	OffsetNumber firstrightoff; /* first origpage item on rightpage */
	OffsetNumber newitemoff;	/* new item's offset */
	uint16		postingoff;		/* offset inside orig posting tuple */
} xl_abtree_split;

#define SizeOfABtreeSplit	(offsetof(xl_abtree_split, postingoff) + \
							sizeof(uint16))

/*
 * When page is deduplicated, consecutive groups of tuples with equal keys are
 * merged together into posting list tuples.
 *
 * The WAL record represents a deduplication pass for a leaf page.  An array
 * of BTDedupInterval structs follows.
 */
typedef struct xl_abtree_dedup
{
	uint16		nintervals;

	/* DEDUPLICATION INTERVALS FOLLOW */
} xl_abtree_dedup;

#define SizeOfABtreeDedup 	(offsetof(xl_abtree_dedup, nintervals) + \
							sizeof(uint16))

/*
 * This is what we need to know about delete of individual leaf index tuples.
 * The WAL record can represent deletion of any number of index tuples on a
 * single index page when *not* executed by VACUUM.  Deletion of a subset of
 * the TIDs within a posting list tuple is not supported.
 *
 * Backup Blk 0: index page
 */
typedef struct xl_abtree_delete
{
	TransactionId latestRemovedXid;
	uint32		ndeleted;

	/* DELETED TARGET OFFSET NUMBERS FOLLOW */
} xl_abtree_delete;

#define SizeOfABtreeDelete	(offsetof(xl_abtree_delete, ndeleted) + \
							sizeof(uint32))

/*
 * This is what we need to know about page reuse within btree.  This record
 * only exists to generate a conflict point for Hot Standby.
 *
 * Note that we must include a RelFileNode in the record because we don't
 * actually register the buffer with the record.
 */
typedef struct xl_abtree_reuse_page
{
	RelFileNode node;
	BlockNumber block;
	TransactionId latestRemovedXid;
} xl_abtree_reuse_page;

#define SizeOfABtreeReusePage	(sizeof(xl_abtree_reuse_page))

/*
 * This is what we need to know about which TIDs to remove from an individual
 * posting list tuple during vacuuming.  An array of these may appear at the
 * end of xl_abtree_vacuum records.
 */
typedef struct xl_abtree_update
{
	uint16		ndeletedtids;

	/* POSTING LIST uint16 OFFSETS TO A DELETED TID FOLLOW */
} xl_abtree_update;

#define SizeOfABtreeUpdate	(offsetof(xl_abtree_update, ndeletedtids) + \
							sizeof(uint16))

/*
 * This is what we need to know about a VACUUM of a leaf page.  The WAL record
 * can represent deletion of any number of index tuples on a single index page
 * when executed by VACUUM.  It can also support "updates" of index tuples,
 * which is how deletes of a subset of TIDs contained in an existing posting
 * list tuple are implemented. (Updates are only used when there will be some
 * remaining TIDs once VACUUM finishes; otherwise the posting list tuple can
 * just be deleted).
 *
 * Updated posting list tuples are represented using xl_abtree_update metadata.
 * The REDO routine uses each xl_abtree_update (plus its corresponding original
 * index tuple from the target leaf page) to generate the final updated tuple.
 */
typedef struct xl_abtree_vacuum
{
	uint16		ndeleted;
	uint16		nupdated;

	/* DELETED TARGET OFFSET NUMBERS FOLLOW */
	/* UPDATED TARGET OFFSET NUMBERS FOLLOW */
	/* UPDATED TUPLES METADATA ARRAY FOLLOWS */
} xl_abtree_vacuum;

#define SizeOfABtreeVacuum	(offsetof(xl_abtree_vacuum, nupdated) + \
							sizeof(uint16))

/*
 * This is what we need to know about marking an empty subtree for deletion.
 * The target identifies the tuple removed from the parent page (note that we
 * remove this tuple's downlink and the *following* tuple's key).  Note that
 * the leaf page is empty, so we don't need to store its content --- it is
 * just reinitialized during recovery using the rest of the fields.
 *
 * Backup Blk 0: leaf block
 * Backup Blk 1: top parent
 */
typedef struct xl_abtree_mark_page_halfdead
{
	OffsetNumber poffset;		/* deleted tuple id in parent page */

	/* information needed to recreate the leaf page: */
	BlockNumber leafblk;		/* leaf block ultimately being deleted */
	BlockNumber leftblk;		/* leaf block's left sibling, if any */
	BlockNumber rightblk;		/* leaf block's right sibling */
	BlockNumber topparent;		/* topmost internal page in the subtree */
} xl_abtree_mark_page_halfdead;

#define SizeOfABtreeMarkPageHalfDead \
	(offsetof(xl_abtree_mark_page_halfdead, topparent) + sizeof(BlockNumber))

/*
 * This is what we need to know about deletion of a btree page.  Note we do
 * not store any content for the deleted page --- it is just rewritten as empty
 * during recovery, apart from resetting the btpo.xact.
 *
 * Backup Blk 0: target block being deleted
 * Backup Blk 1: target block's left sibling, if any
 * Backup Blk 2: target block's right sibling
 * Backup Blk 3: leaf block (if different from target)
 * Backup Blk 4: metapage (if rightsib becomes new fast root)
 */
typedef struct xl_abtree_unlink_page
{
	BlockNumber leftsib;		/* target block's left sibling, if any */
	BlockNumber rightsib;		/* target block's right sibling */

	/*
	 * Information needed to recreate the leaf page, when target is an
	 * internal page.
	 */
	BlockNumber leafleftsib;
	BlockNumber leafrightsib;
	BlockNumber topparent;		/* next child down in the subtree */

	TransactionId abtpo_xact;	/* value of abtpo.xact for use in recovery */
	/* xl_abtree_metadata FOLLOWS IF XLOG_BTREE_UNLINK_PAGE_META */
} xl_abtree_unlink_page;

#define SizeOfABtreeUnlinkPage	(offsetof(xl_abtree_unlink_page, abtpo_xact) +\
								sizeof(TransactionId))

/*
 * New root log record.  There are zero tuples if this is to establish an
 * empty root, or two if it is the result of splitting an old root.
 *
 * Note that although this implies rewriting the metadata page, we don't need
 * an xl_abtree_metadata record --- the rootblk and level are sufficient.
 *
 * Backup Blk 0: new root page (2 tuples as payload, if splitting old root)
 * Backup Blk 1: left child (if splitting an old root)
 * Backup Blk 2: metapage
 */
typedef struct xl_abtree_newroot
{
	BlockNumber rootblk;		/* location of new root (redundant with blk 0) */
	uint32		level;			/* its tree level */
} xl_abtree_newroot;

#define SizeOfABtreeNewroot	(offsetof(xl_abtree_newroot, level) + sizeof(uint32))

typedef enum xl_abtree_agg_op_number {
	XLOG_ABTREE_AGG_OP_FETCH_ADD= 0,
	XLOG_ABTREE_AGG_OP_ATOMIC_WRITE,

	XLOG_ABTREE_AGG_OP_NUMBER_OF_OPS	
} xl_abtree_agg_op_number;

/*
 * The common tag struct for all aggregation related redo ops.
 *
 * Currently all agg atomic ops directly use this struct. See
 * backend/access/abtxlog.c for details.
 *
 * Block 0: the page where we do atomic ops on. The data is the
 * right operand and it might be 2/4/8 bytes.
 */
typedef struct xl_abtree_agg_op
{
	uint16		info;
} xl_abtree_agg_op;

#define SizeOfABTreeAggOp \
	(offsetof(xl_abtree_agg_op, info) + sizeof(uint16))

/*
 * prototypes for functions in abtxlog.c
 */
extern void abtree_redo(XLogReaderState *record);
extern void abtree_desc(StringInfo buf, XLogReaderState *record);
extern const char *abtree_identify(uint8 info);
extern void abtree_xlog_startup(void);
extern void abtree_xlog_cleanup(void);
extern void abtree_mask(char *pagedata, BlockNumber blkno);
extern void _abt_prepare_min_agg_info(xl_abtree_minimal_agg_info *min_agg_info,
									  struct ABTCachedAggInfoData *agg_info);
extern void _abt_apply_min_agg_info(xl_abtree_minimal_agg_info *min_agg_info,
									struct ABTCachedAggInfoData *agg_info);
extern void abt_create_agg_op(xl_abtree_agg_op *agg_op,
							  xl_abtree_agg_op_number opno,
							  Size offset);

#endif							/* ABTXLOG_H */
