/*-------------------------------------------------------------------------
 *
 * nbtxlog.c
 *	  WAL replay logic for aggregate btrees.
 *
 *	  Copied and adapted from src/backend/access/nbtree/nbtxlog.c.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/abtree/abtxlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/abtree.h"
#include "access/abtxlog.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/procarray.h"
#include "utils/memutils.h"

static MemoryContext opCtx;		/* working memory for operations */

static void abt_redo_agg_op(XLogReaderState *record);

/*
 * _abt_restore_page -- re-enter all the index tuples on a page
 *
 * The page is freshly init'd, and *from (length len) is a copy of what
 * had been its upper part (pd_upper to pd_special).  We assume that the
 * tuples had been added to the page in item-number order, and therefore
 * the one with highest item number appears first (lowest on the page).
 */
static void
_abt_restore_page(Page page, char *from, int len)
{
	IndexTupleData itupdata;
	Size		itemsz;
	char	   *end = from + len;
	Item		items[MaxIndexTuplesPerPage];
	uint16		itemsizes[MaxIndexTuplesPerPage];
	int			i;
	int			nitems;

	/*
	 * To get the items back in the original order, we add them to the page in
	 * reverse.  To figure out where one tuple ends and another begins, we
	 * have to scan them in forward order first.
	 */
	i = 0;
	while (from < end)
	{
		/*
		 * As we step through the items, 'from' won't always be properly
		 * aligned, so we need to use memcpy().  Further, we use Item (which
		 * is just a char*) here for our items array for the same reason;
		 * wouldn't want the compiler or anyone thinking that an item is
		 * aligned when it isn't.
		 */
		memcpy(&itupdata, from, sizeof(IndexTupleData));
		itemsz = IndexTupleSize(&itupdata);
		itemsz = MAXALIGN(itemsz);

		items[i] = (Item) from;
		itemsizes[i] = itemsz;
		i++;

		from += itemsz;
	}
	nitems = i;

	for (i = nitems - 1; i >= 0; i--)
	{
		if (PageAddItem(page, items[i], itemsizes[i], nitems - i,
						false, false) == InvalidOffsetNumber)
			elog(PANIC, "_abt_restore_page: cannot add item to page");
		from += itemsz;
	}
}

static void
_abt_restore_meta(XLogReaderState *record, uint8 block_id)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		metabuf;
	Page		metapg;
	ABTMetaPageData *md;
	ABTPageOpaque pageop;
	xl_abtree_metadata *xlrec;
	char	   *ptr;
	Size		len;
	char		*agg_support_bytes;
	Size		agg_support_size;
	ABTAggSupport	agg_support_on_page;

	metabuf = XLogInitBufferForRedo(record, block_id);
	ptr = XLogRecGetBlockData(record, block_id, &len);
	agg_support_bytes = ptr + sizeof(xl_abtree_metadata);	
	agg_support_size = len - sizeof(xl_abtree_metadata);
	
	Assert(agg_support_size <= ABTAS_MAX_SIZE);
	Assert(BufferGetBlockNumber(metabuf) == ABTREE_METAPAGE);
	xlrec = (xl_abtree_metadata *) ptr;
	metapg = BufferGetPage(metabuf);

	_abt_pageinit(metapg, BufferGetPageSize(metabuf));

	md = ABTPageGetMeta(metapg);
	md->abtm_magic = ABTREE_MAGIC;
	md->abtm_version = xlrec->version;
	md->abtm_root = xlrec->root;
	md->abtm_level = xlrec->level;
	md->abtm_fastroot = xlrec->fastroot;
	md->abtm_fastlevel = xlrec->fastlevel;
	/* Cannot log ABTREE_MIN_VERSION index metapage without upgrade */
	Assert(md->abtm_version >= ABTREE_NOVAC_VERSION);
	md->abtm_oldest_abtpo_xact = xlrec->oldest_abtpo_xact;
	md->abtm_last_cleanup_num_heap_tuples = xlrec->last_cleanup_num_heap_tuples;
	md->abtm_allequalimage = xlrec->allequalimage;

	agg_support_on_page = ABTMetaPageGetAggSupport(md);
	memcpy(agg_support_on_page, agg_support_bytes, agg_support_size);
	Assert(ABTASGetStructSize(agg_support_on_page) == agg_support_size);

	pageop = (ABTPageOpaque) PageGetSpecialPointer(metapg);
	pageop->abtpo_flags = ABTP_META;

	/*
	 * Set pd_lower just past the end of the metadata.  This is essential,
	 * because without doing so, metadata will be lost if xlog.c compresses
	 * the page.
	 */
	((PageHeader) metapg)->pd_lower =
		((char *) agg_support_on_page + agg_support_size) - (char *) metapg;

	PageSetLSN(metapg, lsn);
	MarkBufferDirty(metabuf);
	UnlockReleaseBuffer(metabuf);
}

/*
 * _abt_clear_incomplete_split -- clear INCOMPLETE_SPLIT flag on a page
 *
 * This is a common subroutine of the redo functions of all the WAL record
 * types that can insert a downlink: insert, split, and newroot.
 */
static void
_abt_clear_incomplete_split(XLogReaderState *record, uint8 block_id)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buf;

	if (XLogReadBufferForRedo(record, block_id, &buf) == BLK_NEEDS_REDO)
	{
		Page		page = (Page) BufferGetPage(buf);
		ABTPageOpaque pageop = (ABTPageOpaque) PageGetSpecialPointer(page);

		Assert(ABT_P_INCOMPLETE_SPLIT(pageop));
		pageop->abtpo_flags &= ~ABTP_INCOMPLETE_SPLIT;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buf);
	}
	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);
}

static void
abtree_xlog_insert(bool isleaf, bool ismeta, bool posting,
				  XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_abtree_insert *xlrec = (xl_abtree_insert *) XLogRecGetData(record);
	Size		data_len = XLogRecGetDataLen(record);
	ABTCachedAggInfoData agg_info_data;
	ABTCachedAggInfo agg_info = &agg_info_data;
	Buffer		buffer;
	Page		page;

	if (posting)
	{
		if (data_len > SizeOfABtreeInsert)
		{
			xl_abtree_minimal_agg_info min_agg_info;

			/*
			 * We're dealing with posting list and we only store the minimal
			 * agg info if the leaf level has stored agg. This must be set
			 * before calling _abt_apply_min_agg_info().
			 */
			agg_info->leaf_has_agg = true;

			Assert(XLogRecGetDataLen(record) == SizeOfABtreeInsert +
					SizeOfABtreeMinimalAggInfo);
			memcpy(&min_agg_info, (Pointer) xlrec + SizeOfABtreeInsert,
				   SizeOfABtreeMinimalAggInfo);
			_abt_apply_min_agg_info(&min_agg_info, agg_info);
		}
		else
		{
			agg_info->leaf_has_agg = false;
		}
	}

	/*
	 * Insertion to an internal page finishes an incomplete split at the child
	 * level.  Clear the incomplete-split flag in the child.  Note: during
	 * normal operation, the child and parent pages are locked at the same
	 * time, so that clearing the flag and inserting the downlink appear
	 * atomic to other backends.  We don't bother with that during replay,
	 * because readers don't care about the incomplete-split flag and there
	 * cannot be updates happening.
	 */
	if (!isleaf)
		_abt_clear_incomplete_split(record, 1);
	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Size		datalen;
		char	   *datapos = XLogRecGetBlockData(record, 0, &datalen);

		page = BufferGetPage(buffer);

		if (!posting)
		{
			/* Simple retail insertion */
			if (PageAddItem(page, (Item) datapos, datalen, xlrec->offnum,
							false, false) == InvalidOffsetNumber)
				elog(PANIC, "failed to add new item");
		}
		else
		{
			ItemId		itemid;
			IndexTuple	oposting,
						newitem,
						nposting;
			uint16		postingoff;

			/*
			 * A posting list split occurred during leaf page insertion.  WAL
			 * record data will start with an offset number representing the
			 * point in an existing posting list that a split occurs at.
			 *
			 * Use _abt_swap_posting() to repeat posting list split steps from
			 * primary.  Note that newitem from WAL record is 'orignewitem',
			 * not the final version of newitem that is actually inserted on
			 * page.
			 */
			postingoff = *((uint16 *) datapos);
			datapos += sizeof(uint16);
			datalen -= sizeof(uint16);

			itemid = PageGetItemId(page, OffsetNumberPrev(xlrec->offnum));
			oposting = (IndexTuple) PageGetItem(page, itemid);

			/* Use mutable, aligned newitem copy in _abt_swap_posting() */
			Assert(isleaf && postingoff > 0);
			newitem = CopyIndexTuple((IndexTuple) datapos);
			nposting = _abt_swap_posting(newitem, oposting, postingoff,
										 agg_info);

			/* Replace existing posting list with post-split version */
			memcpy(oposting, nposting, MAXALIGN(IndexTupleSize(nposting)));

			/* Insert "final" new item (not orignewitem from WAL stream) */
			Assert(IndexTupleSize(newitem) == datalen);
			if (PageAddItem(page, (Item) newitem, datalen, xlrec->offnum,
							false, false) == InvalidOffsetNumber)
				elog(PANIC, "failed to add posting split new item");
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/*
	 * Note: in normal operation, we'd update the metapage while still holding
	 * lock on the page we inserted into.  But during replay it's not
	 * necessary to hold that lock, since no other index updates can be
	 * happening concurrently, and readers will cope fine with following an
	 * obsolete link from the metapage.
	 */
	if (ismeta)
		_abt_restore_meta(record, 2);
}

static void
abtree_xlog_split(bool newitemonleft, XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_abtree_split *xlrec = (xl_abtree_split *) XLogRecGetData(record);
	Size data_len = XLogRecGetDataLen(record);
	ABTCachedAggInfoData	agg_info_data;
	ABTCachedAggInfo	agg_info = &agg_info_data;
	bool		isleaf = (xlrec->level == 0);
	Buffer		lbuf;
	Buffer		rbuf;
	Page		rpage;
	ABTPageOpaque ropaque;
	char	   *datapos;
	Size		datalen;
	BlockNumber leftsib;
	BlockNumber rightsib;
	BlockNumber rnext;
	
	/* 
	 * Set up the a minimal agg_info to required to call _abt_swap_posting() or
	 * copy the agg value for the tuple that points to the split page.
	 */
	if (data_len > SizeOfABtreeSplit)
	{
		xl_abtree_minimal_agg_info	min_agg_info;


		/* 
		 * If it's leaf, we are dealing with a posting list split and in this
		 * case, minimal agg info is appended only if the leaf level has stored
		 * aggregation values. 
		 *
		 * Otherwise, we don't care about whether the leaf level has
		 * aggregation values or not.
		 *
		 * Must be called before _abt_apply_min_agg_info().
		 */
		if (isleaf)
			agg_info->leaf_has_agg = true;
		else
			agg_info->leaf_has_agg = false;

		Assert(SizeOfABtreeSplit + SizeOfABtreeMinimalAggInfo == data_len);
		memcpy(&min_agg_info, ((char *) xlrec) + SizeOfABtreeSplit,
			   SizeOfABtreeMinimalAggInfo);
		_abt_apply_min_agg_info(&min_agg_info, agg_info);
	}
	else
	{
		if (xlrec->postingoff != 0)
		{
			Assert(isleaf);
			agg_info->leaf_has_agg = false;
		}
	}

	XLogRecGetBlockTag(record, 0, NULL, NULL, &leftsib);
	XLogRecGetBlockTag(record, 1, NULL, NULL, &rightsib);
	if (!XLogRecGetBlockTag(record, 2, NULL, NULL, &rnext))
		rnext = ABTP_NONE;

	/*
	 * Clear the incomplete split flag on the left sibling of the child page
	 * this is a downlink for.  (Like in btree_xlog_insert, this can be done
	 * before locking the other pages)
	 */
	if (!isleaf)
		_abt_clear_incomplete_split(record, 3);

	/* Reconstruct right (new) sibling page from scratch */
	rbuf = XLogInitBufferForRedo(record, 1);
	datapos = XLogRecGetBlockData(record, 1, &datalen);
	rpage = (Page) BufferGetPage(rbuf);

	_abt_pageinit(rpage, BufferGetPageSize(rbuf));
	ropaque = (ABTPageOpaque) PageGetSpecialPointer(rpage);

	ropaque->abtpo_prev = leftsib;
	ropaque->abtpo_next = rnext;
	ropaque->abtpo.level = xlrec->level;
	ropaque->abtpo_flags = isleaf ? ABTP_LEAF : 0;
	ropaque->abtpo_cycleid = 0;

	_abt_restore_page(rpage, datapos, datalen);

	PageSetLSN(rpage, lsn);
	MarkBufferDirty(rbuf);

	/* Now reconstruct left (original) sibling page */
	if (XLogReadBufferForRedo(record, 0, &lbuf) == BLK_NEEDS_REDO)
	{
		/*
		 * To retain the same physical order of the tuples that they had, we
		 * initialize a temporary empty page for the left page and add all the
		 * items to that in item number order.  This mirrors how _abt_split()
		 * works.  Retaining the same physical order makes WAL consistency
		 * checking possible.  See also _abt_restore_page(), which does the
		 * same for the right page.
		 */
		Page		lpage = (Page) BufferGetPage(lbuf);
		ABTPageOpaque lopaque = (ABTPageOpaque) PageGetSpecialPointer(lpage);
		OffsetNumber off;
		IndexTuple	newitem = NULL,
					left_hikey = NULL,
					nposting = NULL;
		Size		newitemsz = 0,
					left_hikeysz = 0;
		Page		newlpage;
		OffsetNumber leftoff,
					replacepostingoff = InvalidOffsetNumber,
					previtem_off = InvalidOffsetNumber;
		char		*previtem_newvalue_bytes;

		datapos = XLogRecGetBlockData(record, 0, &datalen);

		if (newitemonleft || xlrec->postingoff != 0)
		{
			newitem = (IndexTuple) datapos;
			newitemsz = MAXALIGN(IndexTupleSize(newitem));
			datapos += newitemsz;
			datalen -= newitemsz;

			if (xlrec->postingoff != 0)
			{
				ItemId		itemid;
				IndexTuple	oposting;

				/* Posting list must be at offset number before new item's */
				replacepostingoff = OffsetNumberPrev(xlrec->newitemoff);

				/* Use mutable, aligned newitem copy in _abt_swap_posting() */
				newitem = CopyIndexTuple(newitem);
				itemid = PageGetItemId(lpage, replacepostingoff);
				oposting = (IndexTuple) PageGetItem(lpage, itemid);
				nposting = _abt_swap_posting(newitem, oposting,
											xlrec->postingoff,
											agg_info);
			}
		}

		/*
		 * Extract left hikey and its size.  We assume that 16-bit alignment
		 * is enough to apply IndexTupleSize (since it's fetching from a
		 * uint16 field).
		 */
		left_hikey = (IndexTuple) datapos;
		left_hikeysz = MAXALIGN(IndexTupleSize(left_hikey));
		datapos += left_hikeysz;
		datalen -= left_hikeysz;

		if (!isleaf)
		{
			previtem_off = OffsetNumberPrev(xlrec->newitemoff);
			if (previtem_off < xlrec->firstrightoff)
			{
				previtem_newvalue_bytes = datapos;
				datapos += agg_info->agg_typlen;
				datalen -= agg_info->agg_typlen;
			}
		}

		Assert(datalen == 0);

		newlpage = PageGetTempPageCopySpecial(lpage);

		/* Set high key */
		leftoff = ABTP_HIKEY;
		if (PageAddItem(newlpage, (Item) left_hikey, left_hikeysz,
						ABTP_HIKEY, false, false) == InvalidOffsetNumber)
			elog(PANIC, "failed to add high key to left page after split");
		leftoff = OffsetNumberNext(leftoff);

		for (off = ABT_P_FIRSTDATAKEY(lopaque); off < xlrec->firstrightoff; off++)
		{
			ItemId		itemid;
			Size		itemsz;
			IndexTuple	item;

			/* Add replacement posting list when required */
			if (off == replacepostingoff)
			{
				Assert(newitemonleft ||
					   xlrec->firstrightoff == xlrec->newitemoff);
				if (PageAddItem(newlpage, (Item) nposting,
								MAXALIGN(IndexTupleSize(nposting)), leftoff,
								false, false) == InvalidOffsetNumber)
					elog(ERROR, "failed to add new posting list item to left page after split");
				leftoff = OffsetNumberNext(leftoff);
				continue;		/* don't insert oposting */
			}

			/* add the new item if it was inserted on left page */
			else if (newitemonleft && off == xlrec->newitemoff)
			{
				if (PageAddItem(newlpage, (Item) newitem, newitemsz, leftoff,
								false, false) == InvalidOffsetNumber)
					elog(ERROR, "failed to add new item to left page after split");
				leftoff = OffsetNumberNext(leftoff);
			}
	

			itemid = PageGetItemId(lpage, off);
			itemsz = ItemIdGetLength(itemid);
			item = (IndexTuple) PageGetItem(lpage, itemid);

			/* Set the agg value of the previtem. */
			if (off == previtem_off)
			{
				memcpy(ABTreeTupleGetAggPtr(item, agg_info),
					   previtem_newvalue_bytes,
					   agg_info->agg_typlen);
			}

			if (PageAddItem(newlpage, (Item) item, itemsz, leftoff,
							false, false) == InvalidOffsetNumber)
				elog(ERROR, "failed to add old item to left page after split");
			leftoff = OffsetNumberNext(leftoff);
		}

		/* cope with possibility that newitem goes at the end */
		if (newitemonleft && off == xlrec->newitemoff)
		{
			if (PageAddItem(newlpage, (Item) newitem, newitemsz, leftoff,
							false, false) == InvalidOffsetNumber)
				elog(ERROR, "failed to add new item to left page after split");
			leftoff = OffsetNumberNext(leftoff);
		}

		PageRestoreTempPage(newlpage, lpage);

		/* Fix opaque fields */
		lopaque->abtpo_flags = ABTP_INCOMPLETE_SPLIT;
		if (isleaf)
			lopaque->abtpo_flags |= ABTP_LEAF;
		lopaque->abtpo_next = rightsib;
		lopaque->abtpo_cycleid = 0;

		PageSetLSN(lpage, lsn);
		MarkBufferDirty(lbuf);
	}

	/*
	 * We no longer need the buffers.  They must be released together, so that
	 * readers cannot observe two inconsistent halves.
	 */
	if (BufferIsValid(lbuf))
		UnlockReleaseBuffer(lbuf);
	UnlockReleaseBuffer(rbuf);

	/*
	 * Fix left-link of the page to the right of the new right sibling.
	 *
	 * Note: in normal operation, we do this while still holding lock on the
	 * two split pages.  However, that's not necessary for correctness in WAL
	 * replay, because no other index update can be in progress, and readers
	 * will cope properly when following an obsolete left-link.
	 */
	if (rnext != ABTP_NONE)
	{
		Buffer		buffer;

		if (XLogReadBufferForRedo(record, 2, &buffer) == BLK_NEEDS_REDO)
		{
			Page		page = (Page) BufferGetPage(buffer);
			ABTPageOpaque pageop = (ABTPageOpaque) PageGetSpecialPointer(page);

			pageop->abtpo_prev = rightsib;

			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

static void
abtree_xlog_dedup(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_abtree_dedup *xlrec = (xl_abtree_dedup *) XLogRecGetData(record);
#ifdef USE_ASSERT_CHECKING
	Size data_len = XLogRecGetDataLen(record);
#endif
	Buffer		buf;
	ABTCachedAggInfoData agg_info_data;
	ABTCachedAggInfo agg_info = &agg_info_data;
	bool		leaf_has_agg;
	xl_abtree_minimal_agg_info	min_agg_info;

	Assert(data_len == SizeOfABtreeDedup + SizeOfABtreeMinimalAggInfo +
					   sizeof(bool));
	memcpy(&min_agg_info, ((char *) xlrec) + SizeOfABtreeDedup,
		   SizeOfABtreeMinimalAggInfo);
	memcpy(&leaf_has_agg, ((char *) xlrec) + SizeOfABtreeDedup +
											 SizeOfABtreeMinimalAggInfo,
		   sizeof(bool));
	/* 
	 * agg_info->leaf_has_agg must be set before calling
	 * _abt_apply_min_agg_info() 
	 */
	agg_info->leaf_has_agg = leaf_has_agg;
	_abt_apply_min_agg_info(&min_agg_info, agg_info);

	if (XLogReadBufferForRedo(record, 0, &buf) == BLK_NEEDS_REDO)
	{
		char	   *ptr = XLogRecGetBlockData(record, 0, NULL);
		Page		page = (Page) BufferGetPage(buf);
		ABTPageOpaque opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		OffsetNumber offnum,
					minoff,
					maxoff;
		ABTDedupState state;
		ABTDedupInterval *intervals;
		Page		newpage;

		state = (ABTDedupState) palloc(sizeof(ABTDedupStateData));
		state->deduplicate = true;	/* unused */
		state->save_extra = true;
		state->nmaxitems = 0;	/* unused */
		/* Conservatively use larger maxpostingsize than primary */
		state->maxpostingsize = ABTMaxItemSize(page, agg_info);
		state->base = NULL;
		state->baseoff = InvalidOffsetNumber;
		state->basetupsize = 0;
		state->htids = palloc(state->maxpostingsize);
		state->aggs = agg_info->leaf_has_agg ? ((Pointer) palloc(
			ABTDedupMaxNumberOfTIDs(state) * agg_info->agg_stride)) :
			NULL;
		state->xmins = (TransactionId *) palloc(
			ABTDedupMaxNumberOfTIDs(state) * sizeof(TransactionId));
		state->nhtids = 0;
		state->nitems = 0;
		state->phystupsize = 0;
		state->nintervals = 0;

		minoff = ABT_P_FIRSTDATAKEY(opaque);
		maxoff = PageGetMaxOffsetNumber(page);
		newpage = PageGetTempPageCopySpecial(page);

		if (!ABT_P_RIGHTMOST(opaque))
		{
			ItemId		itemid = PageGetItemId(page, ABTP_HIKEY);
			Size		itemsz = ItemIdGetLength(itemid);
			IndexTuple	item = (IndexTuple) PageGetItem(page, itemid);

			if (PageAddItem(newpage, (Item) item, itemsz, ABTP_HIKEY,
							false, false) == InvalidOffsetNumber)
				elog(ERROR, "deduplication failed to add highkey");
		}

		intervals = (ABTDedupInterval *) ptr;
		for (offnum = minoff;
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid = PageGetItemId(page, offnum);
			IndexTuple	itup = (IndexTuple) PageGetItem(page, itemid);

			if (offnum == minoff)
				_abt_dedup_start_pending(state, itup, offnum, agg_info);
			else if (state->nintervals < xlrec->nintervals &&
					 state->baseoff == intervals[state->nintervals].baseoff &&
					 state->nitems < intervals[state->nintervals].nitems)
			{
				if (!_abt_dedup_save_htid(state, itup, agg_info))
					elog(ERROR, "deduplication failed to add heap tid to pending posting list");
			}
			else
			{
				_abt_dedup_finish_pending(newpage, state, agg_info);
				_abt_dedup_start_pending(state, itup, offnum, agg_info);
			}
		}

		_abt_dedup_finish_pending(newpage, state, agg_info);
		Assert(state->nintervals == xlrec->nintervals);
		Assert(memcmp(state->intervals, intervals,
					  state->nintervals * sizeof(ABTDedupInterval)) == 0);

		if (ABT_P_HAS_GARBAGE(opaque))
		{
			ABTPageOpaque nopaque = (ABTPageOpaque) PageGetSpecialPointer(newpage);

			nopaque->abtpo_flags &= ~ABTP_HAS_GARBAGE;
		}

		PageRestoreTempPage(newpage, page);
		PageSetLSN(page, lsn);
		MarkBufferDirty(buf);
	}

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);
}

static void
abtree_xlog_vacuum(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_abtree_vacuum *xlrec = (xl_abtree_vacuum *) XLogRecGetData(record);
	Size		data_len = XLogRecGetDataLen(record);
	Buffer		buffer;
	Page		page;
	ABTPageOpaque opaque;
	ABTCachedAggInfoData agg_info_data;
	ABTCachedAggInfo agg_info = NULL;

	if (data_len == SizeOfABtreeVacuum + SizeOfABtreeMinimalAggInfo)
	{
		xl_abtree_minimal_agg_info min_agg_info;
		memcpy(&min_agg_info, ((char *) xlrec) + SizeOfABtreeVacuum,
			   SizeOfABtreeMinimalAggInfo);
		agg_info = &agg_info_data;

		/* 
		 * agg_info->leaf_has_agg must be set before calling
		 * _abt_apply_min_agg_info() 
		 */
		agg_info->leaf_has_agg = true;
		_abt_apply_min_agg_info(&min_agg_info, agg_info);
	}
	else
	{
		Assert(data_len == SizeOfABtreeVacuum);
	}
	
	/* 
	 * We must have stored the minimal agg info if we need to call
	 * _abt_update_posting() later. 
	 */
	Assert(xlrec->nupdated == 0 || agg_info != NULL);

	/*
	 * We need to take a cleanup lock here, just like btvacuumpage(). However,
	 * it isn't necessary to exhaustively get a cleanup lock on every block in
	 * the index during recovery (just getting a cleanup lock on pages with
	 * items to kill suffices).  See nbtree/README for details.
	 */
	if (XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &buffer)
		== BLK_NEEDS_REDO)
	{
		char	   *ptr = XLogRecGetBlockData(record, 0, NULL);

		page = (Page) BufferGetPage(buffer);

		if (xlrec->nupdated > 0)
		{
			OffsetNumber *updatedoffsets;
			xl_abtree_update *updates;

			updatedoffsets = (OffsetNumber *)
				(ptr + xlrec->ndeleted * sizeof(OffsetNumber));
			updates = (xl_abtree_update *) ((char *) updatedoffsets +
										   xlrec->nupdated *
										   sizeof(OffsetNumber));

			for (int i = 0; i < xlrec->nupdated; i++)
			{
				ABTVacuumPosting vacposting;
				IndexTuple	origtuple;
				ItemId		itemid;
				Size		itemsz;

				itemid = PageGetItemId(page, updatedoffsets[i]);
				origtuple = (IndexTuple) PageGetItem(page, itemid);

				vacposting = palloc(offsetof(ABTVacuumPostingData, deletetids) +
									updates->ndeletedtids * sizeof(uint16));
				vacposting->updatedoffset = updatedoffsets[i];
				vacposting->itup = origtuple;
				vacposting->ndeletedtids = updates->ndeletedtids;
				memcpy(vacposting->deletetids,
					   (char *) updates + SizeOfABtreeUpdate,
					   updates->ndeletedtids * sizeof(uint16));

				_abt_update_posting(vacposting, agg_info);

				/* Overwrite updated version of tuple */
				itemsz = MAXALIGN(IndexTupleSize(vacposting->itup));
				if (!PageIndexTupleOverwrite(page, updatedoffsets[i],
											 (Item) vacposting->itup, itemsz))
					elog(PANIC, "failed to update partially dead item");

				pfree(vacposting->itup);
				pfree(vacposting);

				/* advance to next xl_abtree_update from array */
				updates = (xl_abtree_update *)
					((char *) updates + SizeOfABtreeUpdate +
					 updates->ndeletedtids * sizeof(uint16));
			}
		}

		if (xlrec->ndeleted > 0)
			PageIndexMultiDelete(page, (OffsetNumber *) ptr, xlrec->ndeleted);

		/*
		 * Mark the page as not containing any LP_DEAD items --- see comments
		 * in _abt_delitems_vacuum().
		 */
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		opaque->abtpo_flags &= ~ABTP_HAS_GARBAGE;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
abtree_xlog_delete(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_abtree_delete *xlrec = (xl_abtree_delete *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	ABTPageOpaque opaque;

	/*
	 * If we have any conflict processing to do, it must happen before we
	 * update the page
	 */
	if (InHotStandby)
	{
		RelFileNode rnode;

		XLogRecGetBlockTag(record, 0, &rnode, NULL, NULL);

		ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid, rnode);
	}

	/*
	 * We don't need to take a cleanup lock to apply these changes. See
	 * nbtree/README for details.
	 */
	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		char	   *ptr = XLogRecGetBlockData(record, 0, NULL);

		page = (Page) BufferGetPage(buffer);

		PageIndexMultiDelete(page, (OffsetNumber *) ptr, xlrec->ndeleted);

		/* Mark the page as not containing any LP_DEAD items */
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		opaque->abtpo_flags &= ~ABTP_HAS_GARBAGE;
		opaque->abtpo_flags |= ABTP_HAD_GARBAGE_REMOVED;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
abtree_xlog_mark_page_halfdead(uint8 info, XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_abtree_mark_page_halfdead *xlrec =
		(xl_abtree_mark_page_halfdead *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	ABTPageOpaque pageop;
	IndexTupleData trunctuple;

	/*
	 * In normal operation, we would lock all the pages this WAL record
	 * touches before changing any of them.  In WAL replay, it should be okay
	 * to lock just one page at a time, since no concurrent index updates can
	 * be happening, and readers should not care whether they arrive at the
	 * target page or not (since it's surely empty).
	 */

	/* to-be-deleted subtree's parent page */
	if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO)
	{
		OffsetNumber poffset;
		ItemId		itemid;
		IndexTuple	itup;
		OffsetNumber nextoffset;
		BlockNumber rightsib;

		page = (Page) BufferGetPage(buffer);
		pageop = (ABTPageOpaque) PageGetSpecialPointer(page);

		poffset = xlrec->poffset;

		nextoffset = OffsetNumberNext(poffset);
		itemid = PageGetItemId(page, nextoffset);
		itup = (IndexTuple) PageGetItem(page, itemid);
		rightsib = ABTreeTupleGetDownLink(itup);

		itemid = PageGetItemId(page, poffset);
		itup = (IndexTuple) PageGetItem(page, itemid);
		ABTreeTupleSetDownLink(itup, rightsib);
		nextoffset = OffsetNumberNext(poffset);
		PageIndexTupleDelete(page, nextoffset);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* Rewrite the leaf page as a halfdead page */
	buffer = XLogInitBufferForRedo(record, 0);
	page = (Page) BufferGetPage(buffer);

	_abt_pageinit(page, BufferGetPageSize(buffer));
	pageop = (ABTPageOpaque) PageGetSpecialPointer(page);

	pageop->abtpo_prev = xlrec->leftblk;
	pageop->abtpo_next = xlrec->rightblk;
	pageop->abtpo.level = 0;
	pageop->abtpo_flags = ABTP_HALF_DEAD | ABTP_LEAF;
	pageop->abtpo_cycleid = 0;

	/*
	 * Construct a dummy high key item that points to top parent page (value
	 * is InvalidBlockNumber when the top parent page is the leaf page itself)
	 */
	MemSet(&trunctuple, 0, sizeof(IndexTupleData));
	trunctuple.t_info = sizeof(IndexTupleData);
	ABTreeTupleSetTopParent(&trunctuple, xlrec->topparent);

	if (PageAddItem(page, (Item) &trunctuple, sizeof(IndexTupleData), ABTP_HIKEY,
					false, false) == InvalidOffsetNumber)
		elog(ERROR, "could not add dummy high key to half-dead page");

	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}


static void
abtree_xlog_unlink_page(uint8 info, XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_abtree_unlink_page *xlrec =
		(xl_abtree_unlink_page *) XLogRecGetData(record);
	BlockNumber leftsib;
	BlockNumber rightsib;
	Buffer		buffer;
	Page		page;
	ABTPageOpaque pageop;

	leftsib = xlrec->leftsib;
	rightsib = xlrec->rightsib;

	/*
	 * In normal operation, we would lock all the pages this WAL record
	 * touches before changing any of them.  In WAL replay, it should be okay
	 * to lock just one page at a time, since no concurrent index updates can
	 * be happening, and readers should not care whether they arrive at the
	 * target page or not (since it's surely empty).
	 */

	/* Fix left-link of right sibling */
	if (XLogReadBufferForRedo(record, 2, &buffer) == BLK_NEEDS_REDO)
	{
		page = (Page) BufferGetPage(buffer);
		pageop = (ABTPageOpaque) PageGetSpecialPointer(page);
		pageop->abtpo_prev = leftsib;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* Fix right-link of left sibling, if any */
	if (leftsib != ABTP_NONE)
	{
		if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO)
		{
			page = (Page) BufferGetPage(buffer);
			pageop = (ABTPageOpaque) PageGetSpecialPointer(page);
			pageop->abtpo_next = rightsib;

			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}

	/* Rewrite target page as empty deleted page */
	buffer = XLogInitBufferForRedo(record, 0);
	page = (Page) BufferGetPage(buffer);

	_abt_pageinit(page, BufferGetPageSize(buffer));
	pageop = (ABTPageOpaque) PageGetSpecialPointer(page);

	pageop->abtpo_prev = leftsib;
	pageop->abtpo_next = rightsib;
	pageop->abtpo.xact = xlrec->abtpo_xact;
	pageop->abtpo_flags = ABTP_DELETED;
	pageop->abtpo_cycleid = 0;

	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	/*
	 * If we deleted a parent of the targeted leaf page, instead of the leaf
	 * itself, update the leaf to point to the next remaining child in the
	 * to-be-deleted subtree
	 */
	if (XLogRecHasBlockRef(record, 3))
	{
		/*
		 * There is no real data on the page, so we just re-create it from
		 * scratch using the information from the WAL record.
		 */
		IndexTupleData trunctuple;

		buffer = XLogInitBufferForRedo(record, 3);
		page = (Page) BufferGetPage(buffer);

		_abt_pageinit(page, BufferGetPageSize(buffer));
		pageop = (ABTPageOpaque) PageGetSpecialPointer(page);

		pageop->abtpo_flags = ABTP_HALF_DEAD | ABTP_LEAF;
		pageop->abtpo_prev = xlrec->leafleftsib;
		pageop->abtpo_next = xlrec->leafrightsib;
		pageop->abtpo.level = 0;
		pageop->abtpo_cycleid = 0;

		/* Add a dummy hikey item */
		MemSet(&trunctuple, 0, sizeof(IndexTupleData));
		trunctuple.t_info = sizeof(IndexTupleData);
		ABTreeTupleSetTopParent(&trunctuple, xlrec->topparent);

		if (PageAddItem(page, (Item) &trunctuple, sizeof(IndexTupleData), ABTP_HIKEY,
						false, false) == InvalidOffsetNumber)
			elog(ERROR, "could not add dummy high key to half-dead page");

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
		UnlockReleaseBuffer(buffer);
	}

	/* Update metapage if needed */
	if (info == XLOG_ABTREE_UNLINK_PAGE_META)
		_abt_restore_meta(record, 4);
}

static void
abtree_xlog_newroot(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_abtree_newroot *xlrec = (xl_abtree_newroot *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	ABTPageOpaque pageop;
	char	   *ptr;
	Size		len;

	buffer = XLogInitBufferForRedo(record, 0);
	page = (Page) BufferGetPage(buffer);

	_abt_pageinit(page, BufferGetPageSize(buffer));
	pageop = (ABTPageOpaque) PageGetSpecialPointer(page);

	pageop->abtpo_flags = ABTP_ROOT;
	pageop->abtpo_prev = pageop->abtpo_next = ABTP_NONE;
	pageop->abtpo.level = xlrec->level;
	if (xlrec->level == 0)
		pageop->abtpo_flags |= ABTP_LEAF;
	pageop->abtpo_cycleid = 0;

	if (xlrec->level > 0)
	{
		ptr = XLogRecGetBlockData(record, 0, &len);
		_abt_restore_page(page, ptr, len);

		/* Clear the incomplete-split flag in left child */
		_abt_clear_incomplete_split(record, 1);
	}

	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	_abt_restore_meta(record, 2);
}

static void
abtree_xlog_reuse_page(XLogReaderState *record)
{
	xl_abtree_reuse_page *xlrec =
		(xl_abtree_reuse_page *) XLogRecGetData(record);

	/*
	 * Btree reuse_page records exist to provide a conflict point when we
	 * reuse pages in the index via the FSM.  That's all they do though.
	 *
	 * latestRemovedXid was the page's abtpo.xact.  The abtpo.xact <
	 * RecentGlobalXmin test in _abt_page_recyclable() conceptually mirrors the
	 * pgxact->xmin > limitXmin test in GetConflictingVirtualXIDs().
	 * Consequently, one XID value achieves the same exclusion effect on
	 * master and standby.
	 */
	if (InHotStandby)
	{
		ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid,
											xlrec->node);
	}
}

void
abtree_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	MemoryContext oldCtx;

	oldCtx = MemoryContextSwitchTo(opCtx);
	switch (info)
	{
		case XLOG_ABTREE_INSERT_LEAF:
			abtree_xlog_insert(true, false, false, record);
			break;
		case XLOG_ABTREE_INSERT_UPPER:
			abtree_xlog_insert(false, false, false, record);
			break;
		case XLOG_ABTREE_INSERT_META:
			abtree_xlog_insert(false, true, false, record);
			break;
		case XLOG_ABTREE_SPLIT_L:
			abtree_xlog_split(true, record);
			break;
		case XLOG_ABTREE_SPLIT_R:
			abtree_xlog_split(false, record);
			break;
		case XLOG_ABTREE_INSERT_POST:
			abtree_xlog_insert(true, false, true, record);
			break;
		case XLOG_ABTREE_DEDUP:
			abtree_xlog_dedup(record);
			break;
		case XLOG_ABTREE_VACUUM:
			abtree_xlog_vacuum(record);
			break;
		case XLOG_ABTREE_DELETE:
			abtree_xlog_delete(record);
			break;
		case XLOG_ABTREE_MARK_PAGE_HALFDEAD:
			abtree_xlog_mark_page_halfdead(info, record);
			break;
		case XLOG_ABTREE_UNLINK_PAGE:
		case XLOG_ABTREE_UNLINK_PAGE_META:
			abtree_xlog_unlink_page(info, record);
			break;
		case XLOG_ABTREE_NEWROOT:
			abtree_xlog_newroot(record);
			break;
		case XLOG_ABTREE_REUSE_PAGE:
			abtree_xlog_reuse_page(record);
			break;
		case XLOG_ABTREE_META_CLEANUP:
			_abt_restore_meta(record, 0);
			break;
		case XLOG_ABTREE_AGG_OP:
			abt_redo_agg_op(record);
			break;
		default:
			elog(PANIC, "btree_redo: unknown op code %u", info);
	}
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(opCtx);
}

void
abtree_xlog_startup(void)
{
	opCtx = AllocSetContextCreate(CurrentMemoryContext,
								  "Btree recovery temporary context",
								  ALLOCSET_DEFAULT_SIZES);
}

void
abtree_xlog_cleanup(void)
{
	MemoryContextDelete(opCtx);
	opCtx = NULL;
}

/*
 * Mask a btree page before performing consistency checks on it.
 */
void
abtree_mask(char *pagedata, BlockNumber blkno)
{
	Page		page = (Page) pagedata;
	ABTPageOpaque maskopaq;

	mask_page_lsn_and_checksum(page);

	mask_page_hint_bits(page);
	mask_unused_space(page);

	maskopaq = (ABTPageOpaque) PageGetSpecialPointer(page);

	if (ABT_P_ISDELETED(maskopaq))
	{
		/*
		 * Mask page content on a DELETED page since it will be re-initialized
		 * during replay. See btree_xlog_unlink_page() for details.
		 */
		mask_page_content(page);
	}
	else if (ABT_P_ISLEAF(maskopaq))
	{
		/*
		 * In btree leaf pages, it is possible to modify the LP_FLAGS without
		 * emitting any WAL record. Hence, mask the line pointer flags. See
		 * _abt_killitems(), _abt_check_unique() for details.
		 */
		mask_lp_flags(page);
	}

	/*
	 * ABTP_HAS_GARBAGE is just an un-logged hint bit. So, mask it. See
	 * _abt_killitems(), _abt_check_unique() for details.
	 */
	maskopaq->abtpo_flags &= ~ABTP_HAS_GARBAGE;

	/*
	 * During replay of a btree page split, we don't set the ABTP_SPLIT_END
	 * flag of the right sibling and initialize the cycle_id to 0 for the same
	 * page. See btree_xlog_split() for details.
	 */
	maskopaq->abtpo_flags &= ~ABTP_SPLIT_END;
	maskopaq->abtpo_cycleid = 0;
}

/*
 * The caller should set agg_info->leaf_has_agg before calling this function.
 */
void
_abt_prepare_min_agg_info(xl_abtree_minimal_agg_info *min_agg_info,
						  ABTCachedAggInfo agg_info)
{
	min_agg_info->agg_typlen = agg_info->agg_typlen;
	min_agg_info->agg_stride = agg_info->agg_stride;
}

void
_abt_apply_min_agg_info(xl_abtree_minimal_agg_info *min_agg_info,
						ABTCachedAggInfo agg_info)
{
	agg_info->agg_typlen = min_agg_info->agg_typlen;
	agg_info->agg_stride = min_agg_info->agg_stride;
	_abt_compute_tuple_offsets(agg_info);
}


/* 
 * Currently we only allow up to 8 different agg redo ops, which are stored in
 * the top 3 bits of info. The bottom 13 bits are used for storing the offsets
 * shifted 2 bits () to the right, because all ops are at least 4-byte aligned.
 * Because BLKSZ is capped at 32768 (see include/pg_config.h and
 * include/storage/itemid.h), we can store 4-byte aligned pointers in 13 bits.
 */
#define XLOG_ABTREE_AGG_OP_NUMBER_SHIFT 13
#define XLOG_ABTREE_AGG_OP_MAX_NUMBER_OF_OPS \
	(1 << (sizeof(uint16) * 8 - XLOG_ABTREE_AGG_OP_NUMBER_SHIFT))
#define XLOG_ABTREE_AGG_OP_OFFSET_MASK 0x1fff
StaticAssertDecl(XLOG_ABTREE_AGG_OP_NUMBER_OF_OPS <=
				 XLOG_ABTREE_AGG_OP_MAX_NUMBER_OF_OPS,
				 "Number of xlog abtree ops exceed the maximum");
#define ABTGetXLogAggOpNumber(agg_op) \
	((agg_op)->info >> XLOG_ABTREE_AGG_OP_NUMBER_SHIFT)
#define ABTGetXLogAggOpOffset(agg_op) \
	(((agg_op)->info & XLOG_ABTREE_AGG_OP_OFFSET_MASK) << 2)

/* agg_op->info needs to initialized to 0 before calling setters */
#define ABTInitXlogAggOp(agg_op) ((agg_op)->info = 0)
#define ABTSetXLogAggOpNumber(agg_op, opno) \
	AssertMacro((opno) >= 0 && (opno) < XLOG_ABTREE_AGG_OP_NUMBER_OF_OPS), \
	((agg_op)->info |= ((opno) << XLOG_ABTREE_AGG_OP_NUMBER_SHIFT))
#define ABTSetXLogAggOpOffset(agg_op, offset) \
	AssertMacro(((offset) >= 0) && ((offset) & 0x3) == 0 && \
			((offset) < ((Size) 1 << (XLOG_ABTREE_AGG_OP_NUMBER_SHIFT + 2)))), \
	(((agg_op)->info) |= (((offset) >> 2) & XLOG_ABTREE_AGG_OP_OFFSET_MASK))

void
abt_create_agg_op(xl_abtree_agg_op *agg_op,
				  xl_abtree_agg_op_number opno,
				  Size offset)
{
	ABTInitXlogAggOp(agg_op);
	ABTSetXLogAggOpNumber(agg_op, opno);
	ABTSetXLogAggOpOffset(agg_op, offset);
}

static void
abt_redo_agg_op(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_abtree_agg_op *xlrec = (xl_abtree_agg_op *) XLogRecGetData(record);
	Buffer		buf;

	if (XLogReadBufferForRedo(record, 0, &buf) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buf);
		Size		datalen;
		char		*new_value = XLogRecGetBlockData(record, 0, &datalen);
		Size		offset;

		Assert(datalen == 2 || datalen == 4 || datalen == 8);
		offset = ABTGetXLogAggOpOffset(xlrec);
		
		switch (ABTGetXLogAggOpNumber(xlrec))
		{
			case XLOG_ABTREE_AGG_OP_FETCH_ADD:
				if (datalen == 8)
				{
					uint64 *lop;
					int64 rop;

					lop = (uint64 *)((char *) page + offset);
					memcpy(&rop, new_value, datalen);
					Assert(TYPEALIGN(8, (uintptr_t) lop) == (uintptr_t) lop);
					*lop += rop;
				}
				else if (datalen == 4)
				{
					uint32 *lop;
					int32 rop;

					lop = (uint32 *)((char *) page + offset);
					memcpy(&rop, new_value, datalen);
					Assert(TYPEALIGN(4, (uintptr_t) lop) == (uintptr_t) lop);
					*lop += rop;
				}
				else if (datalen == 2)
				{
					uint32 *lop;
					int16	rop;

					lop = (uint32 *)((char *) page + offset);
					memcpy(&rop, new_value, datalen);
					Assert(TYPEALIGN(4, (uintptr_t) lop) == (uintptr_t) lop);
					*lop += rop;
				}
				else
				{
					elog(ERROR, "unexpected datalen");
				}
				break;	
			case XLOG_ABTREE_AGG_OP_ATOMIC_WRITE:
				{
					char *lop;
					lop = (char *) page + offset;
					Assert(TYPEALIGN(datalen, lop));
					memcpy(lop, new_value, datalen);
				}
				break;
			default:
				elog(PANIC, "abt_redo_agg_op: unknown agg op number %u",
					 ABTGetXLogAggOpNumber(xlrec));
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buf);
	}
	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);
}


