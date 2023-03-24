/*-------------------------------------------------------------------------
 *
 * abtsearch.c
 *	  Search code for aggregate btrees.
 *
 *	  Copied and adapted from src/backend/access/nbtree/nbtsearch.c.
 *
 * IDENTIFICATION
 *	  src/backend/access/abtree/abtsearch.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "access/abtree.h"
#include "access/relscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"


static void _abt_drop_lock_and_maybe_pin(IndexScanDesc scan, ABTScanPos sp);
static bool _abt_readpage(IndexScanDesc scan, ScanDirection dir,
						 OffsetNumber offnum);
static void _abt_saveitem(ABTScanOpaque so, int itemIndex,
						 OffsetNumber offnum, IndexTuple itup);
static int	_abt_setuppostingitems(ABTScanOpaque so, int itemIndex,
								  OffsetNumber offnum, ItemPointer heapTid,
								  IndexTuple itup);
static inline void _abt_savepostingitem(ABTScanOpaque so, int itemIndex,
									   OffsetNumber offnum,
									   ItemPointer heapTid, int tupleOffset);
static bool _abt_steppage(IndexScanDesc scan, ScanDirection dir);
static bool _abt_readnextpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir);
static bool _abt_parallel_readpage(IndexScanDesc scan, BlockNumber blkno,
								  ScanDirection dir);
static Buffer _abt_walk_left(Relation rel, Buffer buf, Snapshot snapshot);
static bool _abt_endpoint(IndexScanDesc scan, ScanDirection dir);
static inline void _abt_initialize_more_data(ABTScanOpaque so, ScanDirection dir);
static Buffer _abt_moveright_to_next_live_page(Relation rel, Buffer buf);


/*
 *	_abt_drop_lock_and_maybe_pin()
 *
 * Unlock the buffer; and if it is safe to release the pin, do that, too.  It
 * is safe if the scan is using an MVCC snapshot and the index is WAL-logged.
 * This will prevent vacuum from stalling in a blocked state trying to read a
 * page when a cursor is sitting on it -- at least in many important cases.
 *
 * Set the buffer to invalid if the pin is released, since the buffer may be
 * re-used.  If we need to go back to this block (for example, to apply
 * LP_DEAD hints) we must get a fresh reference to the buffer.  Hopefully it
 * will remain in shared memory for as long as it takes to scan the index
 * buffer page.
 */
static void
_abt_drop_lock_and_maybe_pin(IndexScanDesc scan, ABTScanPos sp)
{
	LockBuffer(sp->buf, BUFFER_LOCK_UNLOCK);

	if (IsMVCCSnapshot(scan->xs_snapshot) &&
		RelationNeedsWAL(scan->indexRelation) &&
		!scan->xs_want_itup)
	{
		ReleaseBuffer(sp->buf);
		sp->buf = InvalidBuffer;
	}
}

/*
 *	_abt_search_extended() -- Search the tree for a particular scankey, at a
 *	certain level. _abt_search() now uses this function to find the leaf page
 *	with the given scankey.
 *
 * The passed scankey is an insertion-type scankey (see nbtree/README),
 * but it can omit the rightmost column(s) of the index.
 *
 * Return value is a stack of parent-page pointers.  *bufP is set to the
 * address of the leaf-page buffer, which is locked and pinned.  No locks
 * are held on the parent pages, if xlock_path = false or access == ABT_READ.
 * Otherwise, the parent pages are write locked.
 *
 * If the snapshot parameter is not NULL, "old snapshot" checking will take
 * place during the descent through the tree.  This is not needed when
 * positioning for an insert or delete, so NULL is used for those cases.
 *
 * The returned buffer is locked according to access parameter.  Additionally,
 * access = ABT_WRITE will allow an empty root page to be created and returned.
 * When access = ABT_READ, an empty index will result in *bufP being set to
 * InvalidBuffer.  Also, in ABT_WRITE mode, any incomplete splits encountered
 * during the search will be finished.
 */
ABTStack
_abt_search_extended(Relation rel, ABTScanInsert key, Buffer *bufP, int access,
		             Snapshot snapshot, ABTCachedAggInfo agg_info, uint32 level)
{
	ABTStack		stack_in = NULL;
	int			page_access = ABT_READ;

	/* Get the root page to start with */
	*bufP = _abt_getroot(rel, access, /* reject_non_fastroot = */false, NULL); 
	/* If index is empty and access = ABT_READ, no root page is created. */
	if (!BufferIsValid(*bufP))
		return (ABTStack) NULL;

	/* Loop iterates once per level descended in the tree */
	for (;;)
	{
		Page		page;
		ABTPageOpaque opaque;
		OffsetNumber offnum;
		ItemId		itemid;
		IndexTuple	itup;
		BlockNumber blkno;
		BlockNumber par_blkno;
		ABTStack	new_stack;

		/*
		 * Race -- the page we just grabbed may have split since we read its
		 * pointer in the parent (or metapage).  If it has, we may need to
		 * move right to its new sibling.  Do that.
		 *
		 * In write-mode, allow _abt_moveright to finish any incomplete splits
		 * along the way.  Strictly speaking, we'd only need to finish an
		 * incomplete split on the leaf page we're about to insert to, not on
		 * any of the upper levels (they are taken care of in _abt_getstackbuf,
		 * if the leaf page is split and we insert to the parent page).  But
		 * this is a good opportunity to finish splits of internal pages too.
		 */
		*bufP = _abt_moveright(rel, key, *bufP, (access == ABT_WRITE), stack_in,
							  page_access, snapshot, agg_info);

		/* if this is a page at the requested level, we're done */
		page = BufferGetPage(*bufP);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		if (opaque->abtpo.level == level)
			break;
        if (opaque->abtpo.level < level)
        {
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg_internal("btree level %u not found in index \"%s\"",
									 level, RelationGetRelationName(rel))));
        }

		/*
		 * Find the appropriate item on the internal page, and get the child
		 * page that it points to.
		 */
		offnum = _abt_binsrch(rel, key, *bufP);
		itemid = PageGetItemId(page, offnum);
		itup = (IndexTuple) PageGetItem(page, itemid);
		Assert(ABTreeTupleIsPivot(itup) || !key->heapkeyspace);
		blkno = ABTreeTupleGetDownLink(itup);
		par_blkno = BufferGetBlockNumber(*bufP);

		/*
		 * We need to save the location of the pivot tuple we chose in the
		 * parent page on a stack.  If we need to split a page, we'll use the
		 * stack to work back up to its parent page.  If caller ends up
		 * splitting a page one level down, it usually ends up inserting a new
		 * pivot tuple/downlink immediately after the location recorded here.
		 */
		new_stack = (ABTStack) palloc(sizeof(ABTStackData));
		new_stack->abts_blkno = par_blkno;
		new_stack->abts_offset = offnum;
		new_stack->abts_last_update_id = opaque->abtpo_last_update_id;
		new_stack->abts_parent = stack_in;

		/*
		 * Page level 1 is lowest non-leaf page level prior to leaves.  So, if
		 * we're on the level 1 and asked to lock leaf page in write mode,
		 * then lock next page in write mode, because it must be a leaf.
		 */
		if (opaque->abtpo.level == 1 && access == ABT_WRITE)
			page_access = ABT_WRITE;
	
		/* drop the read lock on the parent page, acquire one on the child */
		*bufP = _abt_relandgetbuf(rel, *bufP, blkno, page_access);

		/* okay, all set to move down a level */
		stack_in = new_stack;
	}

	/*
	 * If we're asked to lock leaf in write mode, but didn't manage to, then
	 * relock.  This should only happen when the root page is a leaf page (and
	 * the only page in the index other than the metapage).
	 */
	if (access == ABT_WRITE && page_access == ABT_READ && level == 0)
	{
		/* trade in our read lock for a write lock */
		LockBuffer(*bufP, BUFFER_LOCK_UNLOCK);
		LockBuffer(*bufP, ABT_WRITE);

		/*
		 * If the page was split between the time that we surrendered our read
		 * lock and acquired our write lock, then this page may no longer be
		 * the right place for the key we want to insert.  In this case, we
		 * need to move right in the tree.  See Lehman and Yao for an
		 * excruciatingly precise description.
		 */
		*bufP = _abt_moveright(rel, key, *bufP, true, stack_in, ABT_WRITE,
							  snapshot, agg_info);
	}

	return stack_in;
}

/*
 *	_abt_moveright() -- move right in the btree if necessary.
 *
 * When we follow a pointer to reach a page, it is possible that
 * the page has changed in the meanwhile.  If this happens, we're
 * guaranteed that the page has "split right" -- that is, that any
 * data that appeared on the page originally is either on the page
 * or strictly to the right of it.
 *
 * This routine decides whether or not we need to move right in the
 * tree by examining the high key entry on the page.  If that entry is
 * strictly less than the scankey, or <= the scankey in the
 * key.nextkey=true case, then we followed the wrong link and we need
 * to move right.
 *
 * The passed insertion-type scankey can omit the rightmost column(s) of the
 * index. (see nbtree/README)
 *
 * When key.nextkey is false (the usual case), we are looking for the first
 * item >= key.  When key.nextkey is true, we are looking for the first item
 * strictly greater than key.
 *
 * If forupdate is true, we will attempt to finish any incomplete splits
 * that we encounter.  This is required when locking a target page for an
 * insertion, because we don't allow inserting on a page before the split
 * is completed.  'stack' and 'agg_info' is only used if forupdate is true.
 *
 * On entry, we have the buffer pinned and a lock of the type specified by
 * 'access'.  If we move right, we release the buffer and lock and acquire
 * the same on the right sibling.  Return value is the buffer we stop at.
 *
 * If the snapshot parameter is not NULL, "old snapshot" checking will take
 * place during the descent through the tree.  This is not needed when
 * positioning for an insert or delete, so NULL is used for those cases.
 */
Buffer
_abt_moveright(Relation rel,
			  ABTScanInsert key,
			  Buffer buf,
			  bool forupdate,
			  ABTStack stack,
			  int access,
			  Snapshot snapshot,
			  ABTCachedAggInfo agg_info)
{
	Page		page;
	ABTPageOpaque opaque;
	int32		cmpval;

	/*
	 * When nextkey = false (normal case): if the scan key that brought us to
	 * this page is > the high key stored on the page, then the page has split
	 * and we need to move right.  (pg_upgrade'd !heapkeyspace indexes could
	 * have some duplicates to the right as well as the left, but that's
	 * something that's only ever dealt with on the leaf level, after
	 * _abt_search has found an initial leaf page.)
	 *
	 * When nextkey = true: move right if the scan key is >= page's high key.
	 * (Note that key.scantid cannot be set in this case.)
	 *
	 * The page could even have split more than once, so scan as far as
	 * needed.
	 *
	 * We also have to move right if we followed a link that brought us to a
	 * dead page.
	 */
	cmpval = key->nextkey ? 0 : 1;

	for (;;)
	{
		page = BufferGetPage(buf);
		TestForOldSnapshot(snapshot, rel, page);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

		if (ABT_P_RIGHTMOST(opaque))
			break;

		/*
		 * Finish any incomplete splits we encounter along the way.
		 */
		if (forupdate && ABT_P_INCOMPLETE_SPLIT(opaque))
		{
			BlockNumber blkno = BufferGetBlockNumber(buf);

			/* upgrade our lock if necessary */
			if (access == ABT_READ)
			{
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				LockBuffer(buf, ABT_WRITE);
			}

			if (ABT_P_INCOMPLETE_SPLIT(opaque))
				_abt_finish_split(rel, buf, stack, agg_info);
			else
				_abt_relbuf(rel, buf);

			/* re-acquire the lock in the right mode, and re-check */
			buf = _abt_getbuf(rel, blkno, access);
			continue;
		}

		if (ABT_P_IGNORE(opaque) || _abt_compare(rel, key, page, ABTP_HIKEY) >= cmpval)
		{
			/* step right one page */
			buf = _abt_relandgetbuf(rel, buf, opaque->abtpo_next, access);
			continue;
		}
		else
			break;
	}

	if (ABT_P_IGNORE(opaque))
		elog(ERROR, "fell off the end of index \"%s\"",
			 RelationGetRelationName(rel));

	return buf;
}

/*
 *	_abt_binsrch() -- Do a binary search for a key on a particular page.
 *
 * On a leaf page, _abt_binsrch() returns the OffsetNumber of the first
 * key >= given scankey, or > scankey if nextkey is true.  (NOTE: in
 * particular, this means it is possible to return a value 1 greater than the
 * number of keys on the page, if the scankey is > all keys on the page.)
 *
 * On an internal (non-leaf) page, _abt_binsrch() returns the OffsetNumber
 * of the last key < given scankey, or last key <= given scankey if nextkey
 * is true.  (Since _abt_compare treats the first data key of such a page as
 * minus infinity, there will be at least one key < scankey, so the result
 * always points at one of the keys on the page.)  This key indicates the
 * right place to descend to be sure we find all leaf keys >= given scankey
 * (or leaf keys > given scankey when nextkey is true).
 *
 * This procedure is not responsible for walking right, it just examines
 * the given page.  _abt_binsrch() has no lock or refcount side effects
 * on the buffer.
 */
OffsetNumber
_abt_binsrch(Relation rel,
			ABTScanInsert key,
			Buffer buf)
{
	Page		page;
	ABTPageOpaque opaque;
	OffsetNumber low,
				high;
	int32		result,
				cmpval;

	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	/* Requesting nextkey semantics while using scantid seems nonsensical */
	/* Assert(!key->nextkey || key->scantid == NULL); */

	/* 
	 * XXX we use this to re-find the inserted tuple
	 * scantid-set callers must use _abt_binsrch_insert() on leaf pages 
	 */
	/* Assert(!ABT_P_ISLEAF(opaque) || key->scantid == NULL); */

	low = ABT_P_FIRSTDATAKEY(opaque);
	high = PageGetMaxOffsetNumber(page);

	/*
	 * If there are no keys on the page, return the first available slot. Note
	 * this covers two cases: the page is really empty (no keys), or it
	 * contains only a high key.  The latter case is possible after vacuuming.
	 * This can never happen on an internal page, however, since they are
	 * never empty (an internal page must have children).
	 */
	if (unlikely(high < low))
		return low;

	/*
	 * Binary search to find the first key on the page >= scan key, or first
	 * key > scankey when nextkey is true.
	 *
	 * For nextkey=false (cmpval=1), the loop invariant is: all slots before
	 * 'low' are < scan key, all slots at or after 'high' are >= scan key.
	 *
	 * For nextkey=true (cmpval=0), the loop invariant is: all slots before
	 * 'low' are <= scan key, all slots at or after 'high' are > scan key.
	 *
	 * We can fall out when high == low.
	 */
	high++;						/* establish the loop invariant for high */

	cmpval = key->nextkey ? 0 : 1;	/* select comparison value */

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		/* We have low <= mid < high, so mid points at a real slot */

		result = _abt_compare(rel, key, page, mid);

		if (result >= cmpval)
			low = mid + 1;
		else
			high = mid;
	}

	/*
	 * At this point we have high == low, but be careful: they could point
	 * past the last slot on the page.
	 *
	 * On a leaf page, we always return the first key >= scan key (resp. >
	 * scan key), which could be the last slot + 1.
	 */
	if (ABT_P_ISLEAF(opaque))
		return low;

	/*
	 * On a non-leaf page, return the last key < scan key (resp. <= scan key).
	 * There must be one if _abt_compare() is playing by the rules.
	 */
	Assert(low > ABT_P_FIRSTDATAKEY(opaque));

	return OffsetNumberPrev(low);
}

/*
 *
 *	_abt_binsrch_insert() -- Cacheable, incremental leaf page binary search.
 *
 * Like _abt_binsrch(), but with support for caching the binary search
 * bounds.  Only used during insertion, and only on the leaf page that it
 * looks like caller will insert tuple on.  Exclusive-locked and pinned
 * leaf page is contained within insertstate.
 *
 * Caches the bounds fields in insertstate so that a subsequent call can
 * reuse the low and strict high bounds of original binary search.  Callers
 * that use these fields directly must be prepared for the case where low
 * and/or stricthigh are not on the same page (one or both exceed maxoff
 * for the page).  The case where there are no items on the page (high <
 * low) makes bounds invalid.
 *
 * Caller is responsible for invalidating bounds when it modifies the page
 * before calling here a second time, and for dealing with posting list
 * tuple matches (callers can use insertstate's postingoff field to
 * determine which existing heap TID will need to be replaced by a posting
 * list split).
 */
OffsetNumber
_abt_binsrch_insert(Relation rel, ABTInsertState insertstate)
{
	ABTScanInsert key = insertstate->itup_key;
	Page		page;
	ABTPageOpaque opaque;
	OffsetNumber low,
				high,
				stricthigh;
	int32		result,
				cmpval;

	page = BufferGetPage(insertstate->buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	Assert(ABT_P_ISLEAF(opaque));
	Assert(!key->nextkey);
	Assert(insertstate->postingoff == 0);

	if (!insertstate->bounds_valid)
	{
		/* Start new binary search */
		low = ABT_P_FIRSTDATAKEY(opaque);
		high = PageGetMaxOffsetNumber(page);
	}
	else
	{
		/* Restore result of previous binary search against same page */
		low = insertstate->low;
		high = insertstate->stricthigh;
	}

	/* If there are no keys on the page, return the first available slot */
	if (unlikely(high < low))
	{
		/* Caller can't reuse bounds */
		insertstate->low = InvalidOffsetNumber;
		insertstate->stricthigh = InvalidOffsetNumber;
		insertstate->bounds_valid = false;
		return low;
	}

	/*
	 * Binary search to find the first key on the page >= scan key. (nextkey
	 * is always false when inserting).
	 *
	 * The loop invariant is: all slots before 'low' are < scan key, all slots
	 * at or after 'high' are >= scan key.  'stricthigh' is > scan key, and is
	 * maintained to save additional search effort for caller.
	 *
	 * We can fall out when high == low.
	 */
	if (!insertstate->bounds_valid)
		high++;					/* establish the loop invariant for high */
	stricthigh = high;			/* high initially strictly higher */

	cmpval = 1;					/* !nextkey comparison value */

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		/* We have low <= mid < high, so mid points at a real slot */

		result = _abt_compare(rel, key, page, mid);

		if (result >= cmpval)
			low = mid + 1;
		else
		{
			high = mid;
			if (result != 0)
				stricthigh = high;
		}

		/*
		 * If tuple at offset located by binary search is a posting list whose
		 * TID range overlaps with caller's scantid, perform posting list
		 * binary search to set postingoff for caller.  Caller must split the
		 * posting list when postingoff is set.  This should happen
		 * infrequently.
		 */
		if (unlikely(result == 0 && key->scantid != NULL))
			insertstate->postingoff = _abt_binsrch_posting(key, page, mid);
	}

	/*
	 * On a leaf page, a binary search always returns the first key >= scan
	 * key (at least in !nextkey case), which could be the last slot + 1. This
	 * is also the lower bound of cached search.
	 *
	 * stricthigh may also be the last slot + 1, which prevents caller from
	 * using bounds directly, but is still useful to us if we're called a
	 * second time with cached bounds (cached low will be < stricthigh when
	 * that happens).
	 */
	insertstate->low = low;
	insertstate->stricthigh = stricthigh;
	insertstate->bounds_valid = true;

	return low;
}

/*----------
 *	_abt_binsrch_posting() -- posting list binary search.
 *
 * Helper routine for _abt_binsrch_insert().
 *
 * Returns offset into posting list where caller's scantid belongs.
 *----------
 */
int
_abt_binsrch_posting(ABTScanInsert key, Page page, OffsetNumber offnum)
{
	IndexTuple	itup;
	ItemId		itemid;
	int			low,
				high,
				mid,
				res;

	/*
	 * If this isn't a posting tuple, then the index must be corrupt (if it is
	 * an ordinary non-pivot tuple then there must be an existing tuple with a
	 * heap TID that equals inserter's new heap TID/scantid).  Defensively
	 * check that tuple is a posting list tuple whose posting list range
	 * includes caller's scantid.
	 *
	 * (This is also needed because contrib/amcheck's rootdescend option needs
	 * to be able to relocate a non-pivot tuple using _abt_binsrch_insert().)
	 */
	itemid = PageGetItemId(page, offnum);
	itup = (IndexTuple) PageGetItem(page, itemid);
	if (!ABTreeTupleIsPosting(itup))
		return 0;

	Assert(key->heapkeyspace && key->allequalimage);

	/*
	 * In the event that posting list tuple has LP_DEAD bit set, indicate this
	 * to _abt_binsrch_insert() caller by returning -1, a sentinel value.  A
	 * second call to _abt_binsrch_insert() can take place when its caller has
	 * removed the dead item.
	 */
	if (ItemIdIsDead(itemid))
		return -1;

	/* "high" is past end of posting list for loop invariant */
	low = 0;
	high = ABTreeTupleGetNPosting(itup);
	Assert(high >= 2);

	while (high > low)
	{
		mid = low + ((high - low) / 2);
		res = ItemPointerCompare(key->scantid,
								 ABTreeTupleGetPostingN(itup, mid));

		if (res > 0)
			low = mid + 1;
		else if (res < 0)
			high = mid;
		else
			return mid;
	}

	/* Exact match not found */
	return low;
}

/*----------
 *	_abt_compare() -- Compare insertion-type scankey to tuple on a page.
 *
 *	page/offnum: location of btree item to be compared to.
 *
 *		This routine returns:
 *			<0 if scankey < tuple at offnum;
 *			 0 if scankey == tuple at offnum;
 *			>0 if scankey > tuple at offnum.
 *
 * NULLs in the keys are treated as sortable values.  Therefore
 * "equality" does not necessarily mean that the item should be returned
 * to the caller as a matching key.  Similarly, an insertion scankey
 * with its scantid set is treated as equal to a posting tuple whose TID
 * range overlaps with their scantid.  There generally won't be a
 * matching TID in the posting tuple, which caller must handle
 * themselves (e.g., by splitting the posting list tuple).
 *
 * CRUCIAL NOTE: on a non-leaf page, the first data key is assumed to be
 * "minus infinity": this routine will always claim it is less than the
 * scankey.  The actual key value stored is explicitly truncated to 0
 * attributes (explicitly minus infinity) with version 3+ indexes, but
 * that isn't relied upon.  This allows us to implement the Lehman and
 * Yao convention that the first down-link pointer is before the first
 * key.  See backend/access/nbtree/README for details.
 *----------
 */
int32
_abt_compare(Relation rel,
			ABTScanInsert key,
			Page page,
			OffsetNumber offnum)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	ABTPageOpaque opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	IndexTuple	itup;
	ItemPointer heapTid;
	ScanKey		scankey;
	int			ncmpkey;
	int			ntupatts;
	int32		result;

	Assert(_abt_check_natts(rel, key->heapkeyspace, page, offnum));
	Assert(key->keysz <= IndexRelationGetNumberOfKeyAttributes(rel));
	Assert(key->heapkeyspace || key->scantid == NULL);

	/*
	 * Force result ">" if target item is first data item on an internal page
	 * --- see NOTE above.
	 */
	if (!ABT_P_ISLEAF(opaque) && offnum == ABT_P_FIRSTDATAKEY(opaque))
		return 1;

	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));
	ntupatts = ABTreeTupleGetNAtts(itup, rel);

	/*
	 * The scan key is set up with the attribute number associated with each
	 * term in the key.  It is important that, if the index is multi-key, the
	 * scan contain the first k key attributes, and that they be in order.  If
	 * you think about how multi-key ordering works, you'll understand why
	 * this is.
	 *
	 * We don't test for violation of this condition here, however.  The
	 * initial setup for the index scan had better have gotten it right (see
	 * _abt_first).
	 */

	ncmpkey = Min(ntupatts, key->keysz);
	Assert(key->heapkeyspace || ncmpkey == key->keysz);
	Assert(!ABTreeTupleIsPosting(itup) || key->allequalimage);
	scankey = key->scankeys;
	for (int i = 1; i <= ncmpkey; i++)
	{
		Datum		datum;
		bool		isNull;

		datum = index_getattr(itup, scankey->sk_attno, itupdesc, &isNull);

		if (scankey->sk_flags & SK_ISNULL)	/* key is NULL */
		{
			if (isNull)
				result = 0;		/* NULL "=" NULL */
			else if (scankey->sk_flags & SK_ABT_NULLS_FIRST)
				result = -1;	/* NULL "<" NOT_NULL */
			else
				result = 1;		/* NULL ">" NOT_NULL */
		}
		else if (isNull)		/* key is NOT_NULL and item is NULL */
		{
			if (scankey->sk_flags & SK_ABT_NULLS_FIRST)
				result = 1;		/* NOT_NULL ">" NULL */
			else
				result = -1;	/* NOT_NULL "<" NULL */
		}
		else
		{
			/*
			 * The sk_func needs to be passed the index value as left arg and
			 * the sk_argument as right arg (they might be of different
			 * types).  Since it is convenient for callers to think of
			 * _abt_compare as comparing the scankey to the index item, we have
			 * to flip the sign of the comparison result.  (Unless it's a DESC
			 * column, in which case we *don't* flip the sign.)
			 */
			result = DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
													 scankey->sk_collation,
													 datum,
													 scankey->sk_argument));

			if (!(scankey->sk_flags & SK_ABT_DESC))
				INVERT_COMPARE_RESULT(result);
		}

		/* if the keys are unequal, return the difference */
		if (result != 0)
			return result;

		scankey++;
	}

	/*
	 * All non-truncated attributes (other than heap TID) were found to be
	 * equal.  Treat truncated attributes as minus infinity when scankey has a
	 * key attribute value that would otherwise be compared directly.
	 *
	 * Note: it doesn't matter if ntupatts includes non-key attributes;
	 * scankey won't, so explicitly excluding non-key attributes isn't
	 * necessary.
	 */
	if (key->keysz > ntupatts)
		return 1;

	/*
	 * Use the heap TID attribute and scantid to try to break the tie.  The
	 * rules are the same as any other key attribute -- only the
	 * representation differs.
	 */
	heapTid = ABTreeTupleGetHeapTID(itup);
	if (key->scantid == NULL)
	{
		/*
		 * Most searches have a scankey that is considered greater than a
		 * truncated pivot tuple if and when the scankey has equal values for
		 * attributes up to and including the least significant untruncated
		 * attribute in tuple.
		 *
		 * For example, if an index has the minimum two attributes (single
		 * user key attribute, plus heap TID attribute), and a page's high key
		 * is ('foo', -inf), and scankey is ('foo', <omitted>), the search
		 * will not descend to the page to the left.  The search will descend
		 * right instead.  The truncated attribute in pivot tuple means that
		 * all non-pivot tuples on the page to the left are strictly < 'foo',
		 * so it isn't necessary to descend left.  In other words, search
		 * doesn't have to descend left because it isn't interested in a match
		 * that has a heap TID value of -inf.
		 *
		 * However, some searches (pivotsearch searches) actually require that
		 * we descend left when this happens.  -inf is treated as a possible
		 * match for omitted scankey attribute(s).  This is needed by page
		 * deletion, which must re-find leaf pages that are targets for
		 * deletion using their high keys.
		 *
		 * Note: the heap TID part of the test ensures that scankey is being
		 * compared to a pivot tuple with one or more truncated key
		 * attributes.
		 *
		 * Note: pg_upgrade'd !heapkeyspace indexes must always descend to the
		 * left here, since they have no heap TID attribute (and cannot have
		 * any -inf key values in any case, since truncation can only remove
		 * non-key attributes).  !heapkeyspace searches must always be
		 * prepared to deal with matches on both sides of the pivot once the
		 * leaf level is reached.
		 */
		if (key->heapkeyspace && !key->pivotsearch &&
			key->keysz == ntupatts && heapTid == NULL)
			return 1;

		/* All provided scankey arguments found to be equal */
		return 0;
	}

	/*
	 * Treat truncated heap TID as minus infinity, since scankey has a key
	 * attribute value (scantid) that would otherwise be compared directly
	 */
	Assert(key->keysz == IndexRelationGetNumberOfKeyAttributes(rel));
	if (heapTid == NULL)
		return 1;

	/*
	 * Scankey must be treated as equal to a posting list tuple if its scantid
	 * value falls within the range of the posting list.  In all other cases
	 * there can only be a single heap TID value, which is compared directly
	 * with scantid.
	 */
	Assert(ntupatts >= IndexRelationGetNumberOfKeyAttributes(rel));
	result = ItemPointerCompare(key->scantid, heapTid);
	if (result <= 0 || !ABTreeTupleIsPosting(itup))
		return result;
	else
	{
		result = ItemPointerCompare(key->scantid,
									ABTreeTupleGetMaxHeapTID(itup));
		if (result > 0)
			return 1;
	}

	return 0;
}

/*
 *	_abt_first() -- Find the first item in a scan.
 *
 *		We need to be clever about the direction of scan, the search
 *		conditions, and the tree ordering.  We find the first item (or,
 *		if backwards scan, the last item) in the tree that satisfies the
 *		qualifications in the scan key.  On success exit, the page containing
 *		the current index tuple is pinned but not locked, and data about
 *		the matching tuple(s) on the page has been loaded into so->currPos.
 *		scan->xs_ctup.t_self is set to the heap TID of the current tuple,
 *		and if requested, scan->xs_itup points to a copy of the index tuple.
 *
 * If there are no matching items in the index, we return false, with no
 * pins or locks held.
 *
 * Note that scan->keyData[], and the so->keyData[] scankey built from it,
 * are both search-type scankeys (see nbtree/README for more about this).
 * Within this routine, we build a temporary insertion-type scankey to use
 * in locating the scan start position.
 */
bool
_abt_first(IndexScanDesc scan, ScanDirection dir)
{
	Relation	rel = scan->indexRelation;
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	Buffer		buf;
	ABTStack		stack;
	OffsetNumber offnum;
	bool		status = true;
	ABTScanPosItem *currItem;
	BlockNumber blkno;
	ABTCachedAggInfo agg_info = so->agg_info;
	
	Assert(agg_info);
	Assert(!ABTScanPosIsValid(so->currPos));

	pgstat_count_index_scan(rel);

	/*
	 * Examine the scan keys and eliminate any redundant keys; also mark the
	 * keys that must be matched to continue the scan.
	 */
	_abt_preprocess_keys(scan);

	/*
	 * Quit now if _abt_preprocess_keys() discovered that the scan keys can
	 * never be satisfied (eg, x == 1 AND x > 2).
	 */
	if (!so->qual_ok)
	{
		/* Notify any other workers that we're done with this scan key. */
		_abt_parallel_done(scan);
		return false;
	}

	/*
	 * For parallel scans, get the starting page from shared state. If the
	 * scan has not started, proceed to find out first leaf page in the usual
	 * way while keeping other participating processes waiting.  If the scan
	 * has already begun, use the page number from the shared structure.
	 */
	if (scan->parallel_scan != NULL)
	{
		status = _abt_parallel_seize(scan, &blkno);
		if (!status)
			return false;
		else if (blkno == ABTP_NONE)
		{
			_abt_parallel_done(scan);
			return false;
		}
		else if (blkno != InvalidBlockNumber)
		{
			if (!_abt_parallel_readpage(scan, blkno, dir))
				return false;
			goto readcomplete;
		}
	}

	Assert(!so->inskey);
	_abt_build_start_inskey(scan, dir);
	
	/* 
	 * _abt_build_start_inskey() may set qual_ok to false if found some row
	 * key that has its first member as NULL.
	 */
	if (!so->qual_ok)
	{
		/* Notify any other workers that we're done with this scan key. */
		_abt_parallel_done(scan);
		return false;
	}

	/*
	 * If we found no usable boundary keys, we have to start from one end of
	 * the tree.  Walk down that edge to the first or last key, and scan from
	 * there.
	 */
	if (!so->inskey)
	{
		bool		match;

		match = _abt_endpoint(scan, dir);

		if (!match)
		{
			/* No match, so mark (parallel) scan finished */
			_abt_parallel_done(scan);
		}

		return match;
	}

	/*
	 * Use the manufactured insertion scan key to descend the tree and
	 * position ourselves on the target leaf page.
	 */
	stack = _abt_search(rel, so->inskey, &buf, ABT_READ, scan->xs_snapshot,
						agg_info);

	/* don't need to keep the stack around... */
	_abt_freestack(rel, stack);

	if (!BufferIsValid(buf))
	{
		/*
		 * We only get here if the index is completely empty. Lock relation
		 * because nothing finer to lock exists.
		 */
		PredicateLockRelation(rel, scan->xs_snapshot);

		/*
		 * mark parallel scan as done, so that all the workers can finish
		 * their scan
		 */
		_abt_parallel_done(scan);
		ABTScanPosInvalidate(so->currPos);

		return false;
	}
	else
		PredicateLockPage(rel, BufferGetBlockNumber(buf),
						  scan->xs_snapshot);

	_abt_initialize_more_data(so, dir);

	/* position to the precise item on the page */
	offnum = _abt_binsrch(rel, so->inskey, buf);

	/*
	 * If nextkey = false, we are positioned at the first item >= scan key, or
	 * possibly at the end of a page on which all the existing items are less
	 * than the scan key and we know that everything on later pages is greater
	 * than or equal to scan key.
	 *
	 * If nextkey = true, we are positioned at the first item > scan key, or
	 * possibly at the end of a page on which all the existing items are less
	 * than or equal to the scan key and we know that everything on later
	 * pages is greater than scan key.
	 *
	 * The actually desired starting point is either this item or the prior
	 * one, or in the end-of-page case it's the first item on the next page or
	 * the last item on this page.  Adjust the starting offset if needed. (If
	 * this results in an offset before the first item or after the last one,
	 * _abt_readpage will report no items found, and then we'll step to the
	 * next page as needed.)
	 */
	if (so->goback)
		offnum = OffsetNumberPrev(offnum);

	/* remember which buffer we have pinned, if any */
	Assert(!ABTScanPosIsValid(so->currPos));
	so->currPos.buf = buf;

	/*
	 * Now load data from the first page of the scan.
	 */
	if (!_abt_readpage(scan, dir, offnum))
	{
		/*
		 * There's no actually-matching data on this page.  Try to advance to
		 * the next page.  Return false if there's no matching data at all.
		 */
		LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
		if (!_abt_steppage(scan, dir))
			return false;
	}
	else
	{
		/* Drop the lock, and maybe the pin, on the current page */
		_abt_drop_lock_and_maybe_pin(scan, &so->currPos);
	}

readcomplete:
	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;
	if (scan->xs_want_itup)
		scan->xs_itup = (IndexTuple) (so->currTuples + currItem->tupleOffset);

	return true;
}

/*
 *	_abt_next() -- Get the next item in a scan.
 *
 *		On entry, so->currPos describes the current page, which may be pinned
 *		but is not locked, and so->currPos.itemIndex identifies which item was
 *		previously returned.
 *
 *		On successful exit, scan->xs_ctup.t_self is set to the TID of the
 *		next heap tuple, and if requested, scan->xs_itup points to a copy of
 *		the index tuple.  so->currPos is updated as needed.
 *
 *		On failure exit (no more tuples), we release pin and set
 *		so->currPos.buf to InvalidBuffer.
 */
bool
_abt_next(IndexScanDesc scan, ScanDirection dir)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	ABTScanPosItem *currItem;

	/*
	 * Advance to next tuple on current page; or if there's no more, try to
	 * step to the next page with data.
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (++so->currPos.itemIndex > so->currPos.lastItem)
		{
			if (!_abt_steppage(scan, dir))
				return false;
		}
	}
	else
	{
		if (--so->currPos.itemIndex < so->currPos.firstItem)
		{
			if (!_abt_steppage(scan, dir))
				return false;
		}
	}

	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;
	if (scan->xs_want_itup)
		scan->xs_itup = (IndexTuple) (so->currTuples + currItem->tupleOffset);

	return true;
}

/*
 *	_abt_readpage() -- Load data from current index page into so->currPos
 *
 * Caller must have pinned and read-locked so->currPos.buf; the buffer's state
 * is not changed here.  Also, currPos.moreLeft and moreRight must be valid;
 * they are updated as appropriate.  All other fields of so->currPos are
 * initialized from scratch here.
 *
 * We scan the current page starting at offnum and moving in the indicated
 * direction.  All items matching the scan keys are loaded into currPos.items.
 * moreLeft or moreRight (as appropriate) is cleared if _abt_checkkeys reports
 * that there can be no more matching tuples in the current scan direction.
 *
 * In the case of a parallel scan, caller must have called _abt_parallel_seize
 * prior to calling this function; this function will invoke
 * _abt_parallel_release before returning.
 *
 * Returns true if any matching items found on the page, false if none.
 */
static bool
_abt_readpage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	Page		page;
	ABTPageOpaque opaque;
	OffsetNumber minoff;
	OffsetNumber maxoff;
	int			itemIndex;
	bool		continuescan;
	int			indnatts;

	/*
	 * We must have the buffer pinned and locked, but the usual macro can't be
	 * used here; this function is what makes it good for currPos.
	 */
	Assert(BufferIsValid(so->currPos.buf));

	page = BufferGetPage(so->currPos.buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	/* allow next page be processed by parallel worker */
	if (scan->parallel_scan)
	{
		if (ScanDirectionIsForward(dir))
			_abt_parallel_release(scan, opaque->abtpo_next);
		else
			_abt_parallel_release(scan, BufferGetBlockNumber(so->currPos.buf));
	}

	continuescan = true;		/* default assumption */
	indnatts = IndexRelationGetNumberOfAttributes(scan->indexRelation);
	minoff = ABT_P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * We note the buffer's block number so that we can release the pin later.
	 * This allows us to re-read the buffer if it is needed again for hinting.
	 */
	so->currPos.currPage = BufferGetBlockNumber(so->currPos.buf);

	/*
	 * We save the LSN of the page as we read it, so that we know whether it
	 * safe to apply LP_DEAD hints to the page later.  This allows us to drop
	 * the pin for MVCC scans, which allows vacuum to avoid blocking.
	 */
	so->currPos.lsn = BufferGetLSNAtomic(so->currPos.buf);

	/*
	 * we must save the page's right-link while scanning it; this tells us
	 * where to step right to after we're done with these items.  There is no
	 * corresponding need for the left-link, since splits always go right.
	 */
	so->currPos.nextPage = opaque->abtpo_next;

	/* initialize tuple workspace to empty */
	so->currPos.nextTupleOffset = 0;

	/*
	 * Now that the current page has been made consistent, the macro should be
	 * good.
	 */
	Assert(ABTScanPosIsPinned(so->currPos));

	if (ScanDirectionIsForward(dir))
	{
		/* load items[] in ascending order */
		itemIndex = 0;

		offnum = Max(offnum, minoff);

		while (offnum <= maxoff)
		{
			ItemId		iid = PageGetItemId(page, offnum);
			IndexTuple	itup;

			/*
			 * If the scan specifies not to return killed tuples, then we
			 * treat a killed tuple as not passing the qual
			 */
			if (scan->ignore_killed_tuples && ItemIdIsDead(iid))
			{
				offnum = OffsetNumberNext(offnum);
				continue;
			}

			itup = (IndexTuple) PageGetItem(page, iid);

			if (_abt_checkkeys(scan, itup, indnatts, dir, &continuescan))
			{
				/* tuple passes all scan key conditions */
				if (!ABTreeTupleIsPosting(itup))
				{
					/* Remember it */
					_abt_saveitem(so, itemIndex, offnum, itup);
					itemIndex++;
				}
				else
				{
					int			tupleOffset;

					/*
					 * Set up state to return posting list, and remember first
					 * TID
					 */
					tupleOffset =
						_abt_setuppostingitems(so, itemIndex, offnum,
											  ABTreeTupleGetPostingN(itup, 0),
											  itup);
					itemIndex++;
					/* Remember additional TIDs */
					for (int i = 1; i < ABTreeTupleGetNPosting(itup); i++)
					{
						_abt_savepostingitem(so, itemIndex, offnum,
											ABTreeTupleGetPostingN(itup, i),
											tupleOffset);
						itemIndex++;
					}
				}
			}
			/* When !continuescan, there can't be any more matches, so stop */
			if (!continuescan)
				break;

			offnum = OffsetNumberNext(offnum);
		}

		/*
		 * We don't need to visit page to the right when the high key
		 * indicates that no more matches will be found there.
		 *
		 * Checking the high key like this works out more often than you might
		 * think.  Leaf page splits pick a split point between the two most
		 * dissimilar tuples (this is weighed against the need to evenly share
		 * free space).  Leaf pages with high key attribute values that can
		 * only appear on non-pivot tuples on the right sibling page are
		 * common.
		 */
		if (continuescan && !ABT_P_RIGHTMOST(opaque))
		{
			ItemId		iid = PageGetItemId(page, ABTP_HIKEY);
			IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
			int			truncatt;

			truncatt = ABTreeTupleGetNAtts(itup, scan->indexRelation);
			_abt_checkkeys(scan, itup, truncatt, dir, &continuescan);
		}

		if (!continuescan)
			so->currPos.moreRight = false;

		Assert(itemIndex <= MaxTIDsPerABTreePage);
		so->currPos.firstItem = 0;
		so->currPos.lastItem = itemIndex - 1;
		so->currPos.itemIndex = 0;
	}
	else
	{
		/* load items[] in descending order */
		itemIndex = MaxTIDsPerABTreePage;

		offnum = Min(offnum, maxoff);

		while (offnum >= minoff)
		{
			ItemId		iid = PageGetItemId(page, offnum);
			IndexTuple	itup;
			bool		tuple_alive;
			bool		passes_quals;

			/*
			 * If the scan specifies not to return killed tuples, then we
			 * treat a killed tuple as not passing the qual.  Most of the
			 * time, it's a win to not bother examining the tuple's index
			 * keys, but just skip to the next tuple (previous, actually,
			 * since we're scanning backwards).  However, if this is the first
			 * tuple on the page, we do check the index keys, to prevent
			 * uselessly advancing to the page to the left.  This is similar
			 * to the high key optimization used by forward scans.
			 */
			if (scan->ignore_killed_tuples && ItemIdIsDead(iid))
			{
				Assert(offnum >= ABT_P_FIRSTDATAKEY(opaque));
				if (offnum > ABT_P_FIRSTDATAKEY(opaque))
				{
					offnum = OffsetNumberPrev(offnum);
					continue;
				}

				tuple_alive = false;
			}
			else
				tuple_alive = true;

			itup = (IndexTuple) PageGetItem(page, iid);

			passes_quals = _abt_checkkeys(scan, itup, indnatts, dir,
										 &continuescan);
			if (passes_quals && tuple_alive)
			{
				/* tuple passes all scan key conditions */
				if (!ABTreeTupleIsPosting(itup))
				{
					/* Remember it */
					itemIndex--;
					_abt_saveitem(so, itemIndex, offnum, itup);
				}
				else
				{
					int			tupleOffset;

					/*
					 * Set up state to return posting list, and remember first
					 * TID.
					 *
					 * Note that we deliberately save/return items from
					 * posting lists in ascending heap TID order for backwards
					 * scans.  This allows _abt_killitems() to make a
					 * consistent assumption about the order of items
					 * associated with the same posting list tuple.
					 */
					itemIndex--;
					tupleOffset =
						_abt_setuppostingitems(so, itemIndex, offnum,
											  ABTreeTupleGetPostingN(itup, 0),
											  itup);
					/* Remember additional TIDs */
					for (int i = 1; i < ABTreeTupleGetNPosting(itup); i++)
					{
						itemIndex--;
						_abt_savepostingitem(so, itemIndex, offnum,
											ABTreeTupleGetPostingN(itup, i),
											tupleOffset);
					}
				}
			}
			if (!continuescan)
			{
				/* there can't be any more matches, so stop */
				so->currPos.moreLeft = false;
				break;
			}

			offnum = OffsetNumberPrev(offnum);
		}

		Assert(itemIndex >= 0);
		so->currPos.firstItem = itemIndex;
		so->currPos.lastItem = MaxTIDsPerABTreePage - 1;
		so->currPos.itemIndex = MaxTIDsPerABTreePage - 1;
	}

	return (so->currPos.firstItem <= so->currPos.lastItem);
}

/* Save an index item into so->currPos.items[itemIndex] */
static void
_abt_saveitem(ABTScanOpaque so, int itemIndex,
			 OffsetNumber offnum, IndexTuple itup)
{
	ABTScanPosItem *currItem = &so->currPos.items[itemIndex];

	Assert(!ABTreeTupleIsPivot(itup) && !ABTreeTupleIsPosting(itup));

	currItem->heapTid = itup->t_tid;
	currItem->indexOffset = offnum;
	if (so->currTuples)
	{
		Size		itupsz = IndexTupleSize(itup);

		currItem->tupleOffset = so->currPos.nextTupleOffset;
		memcpy(so->currTuples + so->currPos.nextTupleOffset, itup, itupsz);
		so->currPos.nextTupleOffset += MAXALIGN(itupsz);
	}
}

/*
 * Setup state to save TIDs/items from a single posting list tuple.
 *
 * Saves an index item into so->currPos.items[itemIndex] for TID that is
 * returned to scan first.  Second or subsequent TIDs for posting list should
 * be saved by calling _abt_savepostingitem().
 *
 * Returns an offset into tuple storage space that main tuple is stored at if
 * needed.
 */
static int
_abt_setuppostingitems(ABTScanOpaque so, int itemIndex, OffsetNumber offnum,
					  ItemPointer heapTid, IndexTuple itup)
{
	ABTScanPosItem *currItem = &so->currPos.items[itemIndex];

	Assert(ABTreeTupleIsPosting(itup));

	currItem->heapTid = *heapTid;
	currItem->indexOffset = offnum;
	if (so->currTuples)
	{
		/* 
		 * Save base IndexTuple (truncate posting list).
		 * This also truncates the aggregation values. But that's fine because
		 * the caller wouldn't be interested in that anyway.
		 */
		IndexTuple	base;
		Size		itupsz = ABTreeTupleGetPostingOffset(itup);

		itupsz = MAXALIGN(itupsz);
		currItem->tupleOffset = so->currPos.nextTupleOffset;
		base = (IndexTuple) (so->currTuples + so->currPos.nextTupleOffset);
		memcpy(base, itup, itupsz);
		/* Defensively reduce work area index tuple header size */
		base->t_info &= ~INDEX_SIZE_MASK;
		base->t_info |= itupsz;
		so->currPos.nextTupleOffset += itupsz;

		return currItem->tupleOffset;
	}

	return 0;
}

/*
 * Save an index item into so->currPos.items[itemIndex] for current posting
 * tuple.
 *
 * Assumes that _abt_setuppostingitems() has already been called for current
 * posting list tuple.  Caller passes its return value as tupleOffset.
 */
static inline void
_abt_savepostingitem(ABTScanOpaque so, int itemIndex, OffsetNumber offnum,
					ItemPointer heapTid, int tupleOffset)
{
	ABTScanPosItem *currItem = &so->currPos.items[itemIndex];

	currItem->heapTid = *heapTid;
	currItem->indexOffset = offnum;

	/*
	 * Have index-only scans return the same base IndexTuple for every TID
	 * that originates from the same posting list
	 */
	if (so->currTuples)
		currItem->tupleOffset = tupleOffset;
}

/*
 *	_abt_steppage() -- Step to next page containing valid data for scan
 *
 * On entry, if so->currPos.buf is valid the buffer is pinned but not locked;
 * if pinned, we'll drop the pin before moving to next page.  The buffer is
 * not locked on entry.
 *
 * For success on a scan using a non-MVCC snapshot we hold a pin, but not a
 * read lock, on that page.  If we do not hold the pin, we set so->currPos.buf
 * to InvalidBuffer.  We return true to indicate success.
 */
static bool
_abt_steppage(IndexScanDesc scan, ScanDirection dir)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	BlockNumber blkno = InvalidBlockNumber;
	bool		status = true;

	Assert(ABTScanPosIsValid(so->currPos));

	/* Before leaving current page, deal with any killed items */
	if (so->numKilled > 0)
		_abt_killitems(scan);

	/*
	 * Before we modify currPos, make a copy of the page data if there was a
	 * mark position that needs it.
	 */
	if (so->markItemIndex >= 0)
	{
		/* bump pin on current buffer for assignment to mark buffer */
		if (ABTScanPosIsPinned(so->currPos))
			IncrBufferRefCount(so->currPos.buf);
		memcpy(&so->markPos, &so->currPos,
			   offsetof(ABTScanPosData, items[1]) +
			   so->currPos.lastItem * sizeof(ABTScanPosItem));
		if (so->markTuples)
			memcpy(so->markTuples, so->currTuples,
				   so->currPos.nextTupleOffset);
		so->markPos.itemIndex = so->markItemIndex;
		so->markItemIndex = -1;
	}

	if (ScanDirectionIsForward(dir))
	{
		/* Walk right to the next page with data */
		if (scan->parallel_scan != NULL)
		{
			/*
			 * Seize the scan to get the next block number; if the scan has
			 * ended already, bail out.
			 */
			status = _abt_parallel_seize(scan, &blkno);
			if (!status)
			{
				/* release the previous buffer, if pinned */
				ABTScanPosUnpinIfPinned(so->currPos);
				ABTScanPosInvalidate(so->currPos);
				return false;
			}
		}
		else
		{
			/* Not parallel, so use the previously-saved nextPage link. */
			blkno = so->currPos.nextPage;
		}

		/* Remember we left a page with data */
		so->currPos.moreLeft = true;

		/* release the previous buffer, if pinned */
		ABTScanPosUnpinIfPinned(so->currPos);
	}
	else
	{
		/* Remember we left a page with data */
		so->currPos.moreRight = true;

		if (scan->parallel_scan != NULL)
		{
			/*
			 * Seize the scan to get the current block number; if the scan has
			 * ended already, bail out.
			 */
			status = _abt_parallel_seize(scan, &blkno);
			ABTScanPosUnpinIfPinned(so->currPos);
			if (!status)
			{
				ABTScanPosInvalidate(so->currPos);
				return false;
			}
		}
		else
		{
			/* Not parallel, so just use our own notion of the current page */
			blkno = so->currPos.currPage;
		}
	}

	if (!_abt_readnextpage(scan, blkno, dir))
		return false;

	/* Drop the lock, and maybe the pin, on the current page */
	_abt_drop_lock_and_maybe_pin(scan, &so->currPos);

	return true;
}

/*
 *	_abt_readnextpage() -- Read next page containing valid data for scan
 *
 * On success exit, so->currPos is updated to contain data from the next
 * interesting page.  Caller is responsible to release lock and pin on
 * buffer on success.  We return true to indicate success.
 *
 * If there are no more matching records in the given direction, we drop all
 * locks and pins, set so->currPos.buf to InvalidBuffer, and return false.
 */
static bool
_abt_readnextpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	Relation	rel;
	Page		page;
	ABTPageOpaque opaque;
	bool		status = true;

	rel = scan->indexRelation;

	if (ScanDirectionIsForward(dir))
	{
		for (;;)
		{
			/*
			 * if we're at end of scan, give up and mark parallel scan as
			 * done, so that all the workers can finish their scan
			 */
			if (blkno == ABTP_NONE || !so->currPos.moreRight)
			{
				_abt_parallel_done(scan);
				ABTScanPosInvalidate(so->currPos);
				return false;
			}
			/* check for interrupts while we're not holding any buffer lock */
			CHECK_FOR_INTERRUPTS();
			/* step right one page */
			so->currPos.buf = _abt_getbuf(rel, blkno, ABT_READ);
			page = BufferGetPage(so->currPos.buf);
			TestForOldSnapshot(scan->xs_snapshot, rel, page);
			opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
			/* check for deleted page */
			if (!ABT_P_IGNORE(opaque))
			{
				PredicateLockPage(rel, blkno, scan->xs_snapshot);
				/* see if there are any matches on this page */
				/* note that this will clear moreRight if we can stop */
				if (_abt_readpage(scan, dir, ABT_P_FIRSTDATAKEY(opaque)))
					break;
			}
			else if (scan->parallel_scan != NULL)
			{
				/* allow next page be processed by parallel worker */
				_abt_parallel_release(scan, opaque->abtpo_next);
			}

			/* nope, keep going */
			if (scan->parallel_scan != NULL)
			{
				_abt_relbuf(rel, so->currPos.buf);
				status = _abt_parallel_seize(scan, &blkno);
				if (!status)
				{
					ABTScanPosInvalidate(so->currPos);
					return false;
				}
			}
			else
			{
				blkno = opaque->abtpo_next;
				_abt_relbuf(rel, so->currPos.buf);
			}
		}
	}
	else
	{
		/*
		 * Should only happen in parallel cases, when some other backend
		 * advanced the scan.
		 */
		if (so->currPos.currPage != blkno)
		{
			ABTScanPosUnpinIfPinned(so->currPos);
			so->currPos.currPage = blkno;
		}

		/*
		 * Walk left to the next page with data.  This is much more complex
		 * than the walk-right case because of the possibility that the page
		 * to our left splits while we are in flight to it, plus the
		 * possibility that the page we were on gets deleted after we leave
		 * it.  See nbtree/README for details.
		 *
		 * It might be possible to rearrange this code to have less overhead
		 * in pinning and locking, but that would require capturing the left
		 * pointer when the page is initially read, and using it here, along
		 * with big changes to _abt_walk_left() and the code below.  It is not
		 * clear whether this would be a win, since if the page immediately to
		 * the left splits after we read this page and before we step left, we
		 * would need to visit more pages than with the current code.
		 *
		 * Note that if we change the code so that we drop the pin for a scan
		 * which uses a non-MVCC snapshot, we will need to modify the code for
		 * walking left, to allow for the possibility that a referenced page
		 * has been deleted.  As long as the buffer is pinned or the snapshot
		 * is MVCC the page cannot move past the half-dead state to fully
		 * deleted.
		 */
		if (ABTScanPosIsPinned(so->currPos))
			LockBuffer(so->currPos.buf, ABT_READ);
		else
			so->currPos.buf = _abt_getbuf(rel, so->currPos.currPage, ABT_READ);

		for (;;)
		{
			/* Done if we know there are no matching keys to the left */
			if (!so->currPos.moreLeft)
			{
				_abt_relbuf(rel, so->currPos.buf);
				_abt_parallel_done(scan);
				ABTScanPosInvalidate(so->currPos);
				return false;
			}

			/* Step to next physical page */
			so->currPos.buf = _abt_walk_left(rel, so->currPos.buf,
											scan->xs_snapshot);

			/* if we're physically at end of index, return failure */
			if (so->currPos.buf == InvalidBuffer)
			{
				_abt_parallel_done(scan);
				ABTScanPosInvalidate(so->currPos);
				return false;
			}

			/*
			 * Okay, we managed to move left to a non-deleted page. Done if
			 * it's not half-dead and contains matching tuples. Else loop back
			 * and do it all again.
			 */
			page = BufferGetPage(so->currPos.buf);
			TestForOldSnapshot(scan->xs_snapshot, rel, page);
			opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
			if (!ABT_P_IGNORE(opaque))
			{
				PredicateLockPage(rel, BufferGetBlockNumber(so->currPos.buf), scan->xs_snapshot);
				/* see if there are any matches on this page */
				/* note that this will clear moreLeft if we can stop */
				if (_abt_readpage(scan, dir, PageGetMaxOffsetNumber(page)))
					break;
			}
			else if (scan->parallel_scan != NULL)
			{
				/* allow next page be processed by parallel worker */
				_abt_parallel_release(scan, BufferGetBlockNumber(so->currPos.buf));
			}

			/*
			 * For parallel scans, get the last page scanned as it is quite
			 * possible that by the time we try to seize the scan, some other
			 * worker has already advanced the scan to a different page.  We
			 * must continue based on the latest page scanned by any worker.
			 */
			if (scan->parallel_scan != NULL)
			{
				_abt_relbuf(rel, so->currPos.buf);
				status = _abt_parallel_seize(scan, &blkno);
				if (!status)
				{
					ABTScanPosInvalidate(so->currPos);
					return false;
				}
				so->currPos.buf = _abt_getbuf(rel, blkno, ABT_READ);
			}
		}
	}

	return true;
}

/*
 *	_abt_parallel_readpage() -- Read current page containing valid data for scan
 *
 * On success, release lock and maybe pin on buffer.  We return true to
 * indicate success.
 */
static bool
_abt_parallel_readpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;

	_abt_initialize_more_data(so, dir);

	if (!_abt_readnextpage(scan, blkno, dir))
		return false;

	/* Drop the lock, and maybe the pin, on the current page */
	_abt_drop_lock_and_maybe_pin(scan, &so->currPos);

	return true;
}

/*
 * _abt_walk_left() -- step left one page, if possible
 *
 * The given buffer must be pinned and read-locked.  This will be dropped
 * before stepping left.  On return, we have pin and read lock on the
 * returned page, instead.
 *
 * Returns InvalidBuffer if there is no page to the left (no lock is held
 * in that case).
 *
 * When working on a non-leaf level, it is possible for the returned page
 * to be half-dead; the caller should check that condition and step left
 * again if it's important.
 */
static Buffer
_abt_walk_left(Relation rel, Buffer buf, Snapshot snapshot)
{
	Page		page;
	ABTPageOpaque opaque;

	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	for (;;)
	{
		BlockNumber obknum;
		BlockNumber lblkno;
		BlockNumber blkno;
		int			tries;

		/* if we're at end of tree, release buf and return failure */
		if (ABT_P_LEFTMOST(opaque))
		{
			_abt_relbuf(rel, buf);
			break;
		}
		/* remember original page we are stepping left from */
		obknum = BufferGetBlockNumber(buf);
		/* step left */
		blkno = lblkno = opaque->abtpo_prev;
		_abt_relbuf(rel, buf);
		/* check for interrupts while we're not holding any buffer lock */
		CHECK_FOR_INTERRUPTS();
		buf = _abt_getbuf(rel, blkno, ABT_READ);
		page = BufferGetPage(buf);
		TestForOldSnapshot(snapshot, rel, page);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

		/*
		 * If this isn't the page we want, walk right till we find what we
		 * want --- but go no more than four hops (an arbitrary limit). If we
		 * don't find the correct page by then, the most likely bet is that
		 * the original page got deleted and isn't in the sibling chain at all
		 * anymore, not that its left sibling got split more than four times.
		 *
		 * Note that it is correct to test ABT_P_ISDELETED not ABT_P_IGNORE here,
		 * because half-dead pages are still in the sibling chain.  Caller
		 * must reject half-dead pages if wanted.
		 */
		tries = 0;
		for (;;)
		{
			if (!ABT_P_ISDELETED(opaque) && opaque->abtpo_next == obknum)
			{
				/* Found desired page, return it */
				return buf;
			}
			if (ABT_P_RIGHTMOST(opaque) || ++tries > 4)
				break;
			blkno = opaque->abtpo_next;
			buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
			page = BufferGetPage(buf);
			TestForOldSnapshot(snapshot, rel, page);
			opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		}

		/* Return to the original page to see what's up */
		buf = _abt_relandgetbuf(rel, buf, obknum, ABT_READ);
		page = BufferGetPage(buf);
		TestForOldSnapshot(snapshot, rel, page);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		if (ABT_P_ISDELETED(opaque))
		{
			/*
			 * It was deleted.  Move right to first nondeleted page (there
			 * must be one); that is the page that has acquired the deleted
			 * one's keyspace, so stepping left from it will take us where we
			 * want to be.
			 */
			for (;;)
			{
				if (ABT_P_RIGHTMOST(opaque))
					elog(ERROR, "fell off the end of index \"%s\"",
						 RelationGetRelationName(rel));
				blkno = opaque->abtpo_next;
				buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
				page = BufferGetPage(buf);
				TestForOldSnapshot(snapshot, rel, page);
				opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
				if (!ABT_P_ISDELETED(opaque))
					break;
			}

			/*
			 * Now return to top of loop, resetting obknum to point to this
			 * nondeleted page, and try again.
			 */
		}
		else
		{
			/*
			 * It wasn't deleted; the explanation had better be that the page
			 * to the left got split or deleted. Without this check, we'd go
			 * into an infinite loop if there's anything wrong.
			 */
			if (opaque->abtpo_prev == lblkno)
				elog(ERROR, "could not find left sibling of block %u in index \"%s\"",
					 obknum, RelationGetRelationName(rel));
			/* Okay to try again with new lblkno value */
		}
	}

	return InvalidBuffer;
}

/*
 * _abt_get_endpoint() -- Find the first or last page on a given tree level
 *
 * If the index is empty, we will return InvalidBuffer; any other failure
 * condition causes ereport().  We will not return a dead page.
 *
 * The returned buffer is pinned and read-locked.
 */
Buffer
_abt_get_endpoint(Relation rel, uint32 level, bool rightmost,
				 Snapshot snapshot)
{
	Buffer		buf;
	Page		page;
	ABTPageOpaque opaque;
	OffsetNumber offnum;
	BlockNumber blkno;
	IndexTuple	itup;

	/*
	 * If we are looking for a leaf page, okay to descend from fast root;
	 * otherwise better descend from true root.  (There is no point in being
	 * smarter about intermediate levels.)
	 */
	if (level == 0)
		buf = _abt_getroot(rel, ABT_READ, /* reject_non_fastroot = */false,
						   NULL);
	else
		buf = _abt_gettrueroot(rel);

	if (!BufferIsValid(buf))
		return InvalidBuffer;

	page = BufferGetPage(buf);
	TestForOldSnapshot(snapshot, rel, page);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	for (;;)
	{
		/*
		 * If we landed on a deleted page, step right to find a live page
		 * (there must be one).  Also, if we want the rightmost page, step
		 * right if needed to get to it (this could happen if the page split
		 * since we obtained a pointer to it).
		 */
		while (ABT_P_IGNORE(opaque) ||
			   (rightmost && !ABT_P_RIGHTMOST(opaque)))
		{
			blkno = opaque->abtpo_next;
			if (blkno == ABTP_NONE)
				elog(ERROR, "fell off the end of index \"%s\"",
					 RelationGetRelationName(rel));
			buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
			page = BufferGetPage(buf);
			TestForOldSnapshot(snapshot, rel, page);
			opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		}

		/* Done? */
		if (opaque->abtpo.level == level)
			break;
		if (opaque->abtpo.level < level)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg_internal("btree level %u not found in index \"%s\"",
									 level, RelationGetRelationName(rel))));

		/* Descend to leftmost or rightmost child page */
		if (rightmost)
			offnum = PageGetMaxOffsetNumber(page);
		else
			offnum = ABT_P_FIRSTDATAKEY(opaque);

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));
		blkno = ABTreeTupleGetDownLink(itup);

		buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
		page = BufferGetPage(buf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	}

	return buf;
}

/*
 * The same as _abt_get_endpoint() but also returns a stack of pointers leading
 * to the left-most/right-most page requested on the level.
 */
Buffer
_abt_get_endpoint_stack(Relation rel, uint32 level, bool rightmost,
				        Snapshot snapshot, ABTStack *p_stack)
{
	Buffer		buf;
	Page		page;
	ABTPageOpaque opaque;
	OffsetNumber offnum = InvalidOffsetNumber;
	BlockNumber blkno;
	IndexTuple	itup;
    ABTStack    new_stack;
    
    /* Allow for fast root here as well. */
    buf = _abt_getroot(rel, ABT_READ, /* reject_non_fastroot = */false,
                       NULL);
    *p_stack = NULL;

	if (!BufferIsValid(buf))
    {
		return InvalidBuffer;
    }

	page = BufferGetPage(buf);
	TestForOldSnapshot(snapshot, rel, page);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	for (;;)
	{
		/*
		 * If we landed on a deleted page, step right to find a live page
		 * (there must be one).  Also, if we want the rightmost page, step
		 * right if needed to get to it (this could happen if the page split
		 * since we obtained a pointer to it).
		 */
		while (ABT_P_IGNORE(opaque) ||
			   (rightmost && !ABT_P_RIGHTMOST(opaque)))
		{
			blkno = opaque->abtpo_next;
			if (blkno == ABTP_NONE)
				elog(ERROR, "fell off the end of index \"%s\"",
					 RelationGetRelationName(rel));
			buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
			page = BufferGetPage(buf);
			TestForOldSnapshot(snapshot, rel, page);
			opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		}

		/* Done? */
		if (opaque->abtpo.level == level)
			break;
		if (opaque->abtpo.level < level)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg_internal("btree level %u not found in index \"%s\"",
									 level, RelationGetRelationName(rel))));

        /* Save the page on this level to the stack before we continue. */
        new_stack = (ABTStack) palloc(sizeof(ABTStackData));
        new_stack->abts_blkno = BufferGetBlockNumber(buf);
        new_stack->abts_offset = offnum;
        new_stack->abts_last_update_id = opaque->abtpo_last_update_id;
        new_stack->abts_parent = *p_stack;
        *p_stack = new_stack;

		/* Descend to leftmost or rightmost child page */
		if (rightmost)
			offnum = PageGetMaxOffsetNumber(page);
		else
			offnum = ABT_P_FIRSTDATAKEY(opaque);

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));
		blkno = ABTreeTupleGetDownLink(itup);

		buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
		page = BufferGetPage(buf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	}

	return buf;
}

/*
 * _abt_sample() -- Sample one item from the range specified in the scan.
 *
 *
 */
bool
_abt_sample(IndexScanDesc scan, double random_number)
{
	Relation		rel = scan->indexRelation;
	ABTScanOpaque	so = (ABTScanOpaque) scan->opaque;
	ABTSnapshot		snapshot = so->snapshot;
	ABTCachedAggInfo agg_info = so->agg_info;
	Buffer			buf;
	Page			page;
	ABTPageOpaque	opaque;
	uint64			*prefix_sums = so->prefix_sums,
					agg;
	uint32			*item_indexes = so->item_indexes;
	int				num_items;
	OffsetNumber	minoff,
					maxoff,
					offnum,
					chosen_leaf_item_offnum;
	int				chosen_leaf_item_posting_off = 0; /* suppress warning */
	BlockNumber		blkno;
	ItemId			itemid;
	IndexTuple		itup = NULL,
					expected_hikey = NULL;
	Pointer			agg_ptr = NULL; /* suppress compiler warning */
	uint64			cutoff;
	int				cutoff_index;
	int				indnatts;
	bool			continuescan;
	TransactionId	*xid_ptr,
					xid;
	ABTScanInsert	inskey_hikey = so->inskey_pivot;
	uint64			acc;
	int32			prev_cmpres;
	
	/* 
	 * Doesn't make sense to do parallel sampling as different backends should
	 * return independent samples.
	 */
	Assert(!scan->parallel_scan);
	
	/* 
	 * The caller should have preprocessed the search-type keys in the
	 * IndexScanDesc to build the insertion-type keys in the opaque. It should
	 * also have initialized the working space for sampling.
	 */
	Assert(so->preprocessed);
	Assert(agg_info && prefix_sums);
	/* The caller should set inskey_low and/or inskey_high instead of inskey. */
	Assert(!so->inskey);
	Assert(!ABTScanPosIsValid(so->currPos));
	Assert(!ABTScanPosIsValid(so->markPos));
	
	/* 
	 * Don't waste time to descend the tree if the key is not satisfiable.
	 */
	if (!so->qual_ok)
    {
        so->inv_prob = 0.0;
		return false;
    }
	
	/*
	 * Get the root or the fast root. Note that, a stale ``fastroot'' that is
	 * now not alone on its level won't work because that means we might need
	 * to hold locks on all live pages on that level to correctly find the
	 * starting prefix sum of the aggregation value.
	 */
	buf = _abt_getroot(rel, ABT_READ, /* reject_non_fastroot =*/true, agg_info);
	if (!BufferIsValid(buf))
	{
        so->inv_prob = 0.0;
		return false;
	}

	/* search for the minoff and maxoff if we have scan keys */
	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	/* 
	 * quick_descend_path handles the case where there is just one matching
	 * index tuple in the range. In this case, we do not need to make any
	 * ranodm choice and should just follow the that link. We reuse the code
	 * for descending from a root because essentially we are desceding from a
	 * subtree root.
	 */
quick_descend_path:
	if (so->inskey_low)
	{
		minoff = _abt_binsrch(rel, so->inskey_low, buf);
	}
	else
	{
		minoff = ABT_P_FIRSTDATAKEY(opaque);
	}

	if (so->inskey_high)
	{
		maxoff = _abt_binsrch(rel, so->inskey_high, buf);
		/* goback is always true for the high key */
		if (ABT_P_ISLEAF(opaque))
			maxoff = OffsetNumberPrev(maxoff);
	}
	else
	{
		maxoff = PageGetMaxOffsetNumber(page);
	}
	
	/* we can't find any matching ranges on the root */
	if (minoff > maxoff)
	{
		_abt_relbuf(rel, buf);
        so->inv_prob = 0.0;
		return false;
	}

    /*
     * Starting from this point, so->inv_prob must be set to the total sum of
     * weights in the root level in the matching range before go to
     * done_sampling.
     */

    /* 
     * We have only have one tuple that is possibly overlapping with our range.
     */
	if (minoff == maxoff)
	{
		itemid = PageGetItemId(page, minoff);
		itup = (IndexTuple) PageGetItem(page, itemid);
		if (ABT_P_ISLEAF(opaque))
		{
			/* 
			 * This page is already a leaf, we're done searching for a starting
			 * page.
			 */
			if (ABTreeTupleIsPosting(itup))
			{
				/* 
				 * Posting list has multiple items in it. So we still need to
				 * search for the correct one.
				 */
				if (!agg_info->leaf_has_agg)
				{
					/* 
					 * We have uniform weight among all items in the posting
					 * list. Choose the one at the cutoff value.
					 */
					int n = ABTreeTupleGetNPosting(itup);
					cutoff_index = (int) floor(n * random_number);
					if (cutoff_index == n)
						--cutoff_index;

					chosen_leaf_item_offnum = minoff;
					chosen_leaf_item_posting_off = cutoff_index;

                    /* choosing 1 out of n items */
                    /* XXX(zy): what if some items here are not visible yet? */
                    so->inv_prob = n;
					goto done_sampling;
				}
				/* 
				 * Otherwise, we still need to build prefix sum and find
				 * the correct cut off index . Falls through after the
				 * outer if (minoff == maxoff) block.
				 */
			}
			else
			{
				chosen_leaf_item_offnum = minoff;

                /* choosing 1 out of 1 item */
                if (agg_info->leaf_has_agg)
                {
                    /*
                     * XXX(zy): this is a fairly rare case where we only have
                     * exactly one matching tuple in the root level which
                     * happens to be a leaf. Due to the invariant we needed for
                     * done_sampling, we must set the inv_prob to the
                     * weight of the tuple and that will be divided by the
                     * same weight immediately.
                     *
                     * Since this is supposed to be rare, I'll leave it as is
                     * unoptimized.
                     */
                    Pointer agg_ptr = ABTreeNonPivotTupleGetAggPtr(itup,
                                                                   agg_info);
                    uint64 w = _abt_atomic_read_agg_ptr(agg_ptr, agg_info);
                    so->inv_prob = w;
                }
                else
                {
                    so->inv_prob = 1.0;
                }
				goto done_sampling;
			}
		}
		else
		{
			uint64	agg;
			/* 
             * Need to set up three things: the child page target blkno, a
             * cutoff value and the expected high key in the child page. See
             * the comments below.
			 */
			blkno = ABTreeTupleGetDownLink(itup);
			
			/* we still compute the cut off here just in case there's a
			 * concurrent split
             */
			agg = _abt_read_agg(rel, itup, snapshot, agg_info);
			cutoff = (uint64) floor(agg * random_number);
			if (agg == cutoff)
				--cutoff;

            /* 
             * Set the numerator of the inverse probability.
             * When it gets to a leaf tuple, it needs to be divided by the
             * weight of the tuple.
             */
            so->inv_prob = agg;
			
			/* 
			 * don't compare minoff with maxoff here, because maxoff could be
			 * somewhere in the middle of the page found by a binary search.
			 *
			 * Use PageGetMadxOffsteNumber() to get the true last item on the
			 * page.
			 */
			if (minoff == PageGetMaxOffsetNumber(page))
			{
				/* min off is the last item */
				if (ABT_P_RIGHTMOST(opaque))
					expected_hikey = NULL;
				else
				{
					itemid = PageGetItemId(page, ABTP_HIKEY);
					expected_hikey = (IndexTuple) PageGetItem(page, itemid);
				}
			}
			else
			{
				itemid = PageGetItemId(page, minoff + 1);
				expected_hikey = (IndexTuple) PageGetItem(page, itemid);
			}
			/* 
			 * save a copy of the expected high key before we go to the child
			 * level 
			 */
			if (expected_hikey)
				expected_hikey = CopyIndexTuple(expected_hikey);
			_abt_mkscankey_extended(rel, expected_hikey, inskey_hikey);
			inskey_hikey->pivotsearch = true;

			/*
             * At this point, we have to descend into the child page. At the
             * same time, the child page could be split, so we need to check if
             * we have a matching key once we're there. If not, we go with the
             * regular path descend path.
			 */
			buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
			buf = _abt_moveright_to_next_live_page(rel, buf);
			if (buf == InvalidBuffer)
			{
				if (expected_hikey)
					pfree(expected_hikey);
				_abt_relbuf(rel, buf);
                /*XXX(zy) this is probably an error case */
                so->inv_prob = 0.0;
				return false;
			}

			page = BufferGetPage(buf);
			opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

			Assert(!ABT_P_IGNORE(opaque));
			if (ABT_P_RIGHTMOST(opaque))
			{
				/* right most page's high key is larger than any non-null key */
				prev_cmpres = (expected_hikey == NULL) ? 0 : 1;
			}
			else
			{
				prev_cmpres = _abt_compare(rel, inskey_hikey, page, ABTP_HIKEY);
			}
			
			// This is the page we're expecting.
			if (prev_cmpres == 0 || ABT_P_RIGHTMOST(opaque)) {
				// we can continue the quick descending path from here,
				// where we do not make any random choice right now
				goto quick_descend_path;
			}
			
			// Otherwise, we need the logic for searching for the cutoff value
			// by moving right
			//
			// Note: maybe we could reject this one early if this is a page
			// higher up in the tree? The rationale is that it's likely that we
			// are going to reject it soon once we are on the next level,
			// because we could be selecting from a very small fraction of the
			// key space it's covering. May not worth the effort though (need
			// more investigation).
			acc = 0;
			goto continue_descent;
		}
	}
	
	/* 
	 * We are here only if we don't know where to descend through the tree
	 * because we have multiple choices, or we have a single posting list where
	 * the aggregate values may be different.
	 */
	Assert(minoff < maxoff || (ABT_P_ISLEAF(opaque) && itup &&
				ABTreeTupleIsPosting(itup) && agg_info->leaf_has_agg));
	
	/* 
	 * Populate the prefix sums of the tuples on the root page here.
	 */
	num_items = 0;
	/* 
	 * Much of the loops are similar in both leaf and non-leaf cases, but we
	 * can save some cycles for determining whether a pivot tuple is posting or
	 * a non-pivot tuple is a pivot tuple when fetching the aggregation value
	 * pointers.
	 */
	Assert(agg_info->agg_byval);
	if (ABT_P_ISLEAF(opaque))
	{
		for (offnum = minoff; offnum <= maxoff; ++offnum)
		{
			itemid = PageGetItemId(page, offnum);
			itup = (IndexTuple) PageGetItem(page, itemid);

			if (ABTreeTupleIsPosting(itup))
			{
				/* posting tuple case */
				int16	n = ABTreeTupleGetNPosting(itup),
						i;

				xid_ptr = ABTreePostingTupleGetXminPtr(itup);
				if (agg_info->leaf_has_agg)
				{
					agg_ptr = ABTreePostingTupleGetAggPtr(itup, agg_info);
					for (i = 0; i < n; ++i)
					{
						Assert(num_items < so->max_num_items);
						
						/* 
						 * read the xmin from the tuple, we need to do
						 * atomic read because _abt_doinsert() may atomically
						 * overwrite its value with a read lock
						 */
						xid = (TransactionId) pg_atomic_read_u32(
								(pg_atomic_uint32 *) &xid_ptr[i]);
						if (_abt_xid_in_snapshot(xid, snapshot))
							continue;
						
						prefix_sums[num_items] = _abt_atomic_read_agg_ptr(
							agg_ptr, agg_info);
						item_indexes[num_items] =
							ABT_OFFSET_AND_POSTING_OFF_GET_ITEMINDEX(
								offnum, i);
						++num_items;
						agg_ptr += agg_info->agg_stride;
					}
				}
				else
				{
					for (i = 0; i < n; ++i)
					{
						Assert(num_items < so->max_num_items);

						xid = (TransactionId) pg_atomic_read_u32(
								(pg_atomic_uint32 *) &xid_ptr[i]);
						if (_abt_xid_in_snapshot(xid, snapshot))
							continue;

						/* 
						 * Don't bother with the prefix sum, just use the total
						 * number of items as the sum as the accumulated values
						 * are the same constant. Simplifies things a lot.
						 */
						item_indexes[num_items] =
							ABT_OFFSET_AND_POSTING_OFF_GET_ITEMINDEX(
								offnum, i);
						++num_items;
					}
				}
			}
			else
			{
				/* plain (non-posting) leaf tuple case */
				Assert(num_items < so->max_num_items);

				xid_ptr = ABTreeNonPivotTupleGetXminPtr(itup);
				xid = (TransactionId) pg_atomic_read_u32(
						(pg_atomic_uint32 *) xid_ptr);
				if (_abt_xid_in_snapshot(xid, snapshot))
					continue;

				if (agg_info->leaf_has_agg)
				{
					agg_ptr = ABTreeNonPivotTupleGetAggPtr(itup, agg_info);
					prefix_sums[num_items] =
						_abt_atomic_read_agg_ptr(agg_ptr, agg_info);
				}
				else
				{
					/* 
					 * Same as above. Don't bother compute the prefix sums
					 * at all.
					 */
				}

				item_indexes[num_items] = ABT_OFFSET_GET_ITEMINDEX(offnum);
				++num_items;
			}
		}
	}
	else
	{
		for (offnum = minoff; offnum <= maxoff; ++offnum)
		{
			itemid = PageGetItemId(page, offnum);
			itup = (IndexTuple) PageGetItem(page, itemid);

			Assert(num_items < so->max_num_items);
			prefix_sums[num_items] =
				_abt_read_agg(rel, itup, snapshot, agg_info);
			if (prefix_sums[num_items] == 0)
				continue;
			item_indexes[num_items] = ABT_OFFSET_GET_ITEMINDEX(offnum);
			++num_items;
		}
	}
	
	/* nothing visible to us. */
	if (num_items == 0)
	{
        so->inv_prob = 0.0;
		_abt_relbuf(rel, buf);
		return false;
	}

	/* 
	 * Compute the cutoff number by floor(SUM(aggregation_value) *
	 * random_number). The sampled item is the first item whose prefix sum >
	 * the cutoff number.
	 */
	if (ABT_P_ISLEAF(opaque) && !agg_info->leaf_has_agg)
	{
		/* 
		 * Simple case where the leaf aggregations are all constant. Hence
		 * we only need to use the num_items as the sum. 
		 */
		cutoff_index = (int)floor(num_items * random_number);
		if (cutoff_index == num_items)
			--cutoff_index;
		Assert(cutoff_index >= 0 && cutoff_index < num_items);

        /* 
         * we are selecting 1 item out of `num_items` items uniformly at random
         */
        so->inv_prob = num_items;
	}
	else
	{
		int		low,
				high,
				mid,
				i;

		for (i = 1; i < num_items; ++i)
		{
			prefix_sums[i] += prefix_sums[i - 1];
		}

		cutoff = (uint64) floor(prefix_sums[num_items - 1] * random_number);
		if (cutoff == prefix_sums[num_items - 1])
			--cutoff;
        
        /*
         * Similar to the case minoff == maxoff && !leaf, this is the total
         * weight of matching range in the root level. It needs to be divided
         * by the weight of the sample in the end.
         *
         * Note that this includes the subranges that do not match our
         * predicate. But these have to be included because they introduce
         * non-zero probability of sample rejection.
         */
        so->inv_prob = prefix_sums[num_items - 1];

		/* Binary search for the cutoff index. */
		low = 0;
		high = num_items;
		while (low < high)
		{
			mid = (low + high) >> 1;
			if (prefix_sums[mid] > cutoff)
			{
				high = mid;
			}
			else
			{
				low = mid + 1;
			}
		}
		Assert(low == high && low < num_items);
		cutoff_index = low;
	}

	if (ABT_P_ISLEAF(opaque))
	{
		/* 
		 * If this page is leaf, we're already at one chosen sample. 
		 */
		chosen_leaf_item_offnum =
			ABT_ITEMINDEX_GET_OFFSET(item_indexes[cutoff_index]);
		chosen_leaf_item_posting_off =
			ABT_ITEMINDEX_GET_POSTING_OFF(item_indexes[cutoff_index]);
		goto done_sampling;
	}

	/* Otherwise, continue descending the tree using the chosen prefix sum. */
	offnum = ABT_ITEMINDEX_GET_OFFSET(item_indexes[cutoff_index]);
	itemid = PageGetItemId(page, offnum);
	itup = (IndexTuple) PageGetItem(page, itemid);
	blkno = ABTreeTupleGetDownLink(itup);
	/* 
	 * Also need to adjust the offnum to the subrange of the subtree we're
	 * descending into. 
	 */
	if (cutoff_index != 0)
		cutoff -= prefix_sums[cutoff_index - 1];

	if (offnum == PageGetMaxOffsetNumber(page))
	{
		if (ABT_P_RIGHTMOST(opaque))
			expected_hikey = NULL;
		else
		{
			itemid = PageGetItemId(page, ABTP_HIKEY);
			expected_hikey = (IndexTuple) PageGetItem(page, itemid);
		}
	}
	else
	{
		itemid = PageGetItemId(page, offnum + 1);
		expected_hikey = (IndexTuple) PageGetItem(page, itemid);
	}
	/* make a copy of expected high key before we move to the child page */
	if (expected_hikey)
		expected_hikey = CopyIndexTuple(expected_hikey);
	
	_abt_mkscankey_extended(rel, expected_hikey, inskey_hikey);
	inskey_hikey->pivotsearch = true;
	buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
	acc = 0;
	for (;;) {

		buf = _abt_moveright_to_next_live_page(rel, buf);
		if (buf == InvalidBuffer)
		{
			if (expected_hikey)
				pfree(expected_hikey);
			_abt_relbuf(rel, buf);
			return false;
		}

		page = BufferGetPage(buf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

		Assert(!ABT_P_IGNORE(opaque));
		if (ABT_P_RIGHTMOST(opaque))
		{
			/* right most page's high key is larger than any non-null key */
			prev_cmpres = (expected_hikey == NULL) ? 0 : 1;
		}
		else
		{
			prev_cmpres = _abt_compare(rel, inskey_hikey, page, ABTP_HIKEY);
		}

		/* 
		 * At this point, we should have set up the child page target blkno,
		 * the remaining cutoff value, the expected high key and initialized
		 * acc = 0.
		 */
continue_descent:
		if (prev_cmpres > 0)
		{
			/* 
			 * expected high key is larger than the high key on this page,
			 * meaning we have moved out of the range.
			 */
			if (expected_hikey)
				pfree(expected_hikey);
			_abt_relbuf(rel, buf);
			return false;
		}

		if (ABT_P_ISLEAF(opaque))
		{
			int i;

			minoff = ABT_P_FIRSTDATAKEY(opaque);
			maxoff = PageGetMaxOffsetNumber(page);
			for (offnum = minoff; offnum <= maxoff; ++offnum)
			{
				itemid = PageGetItemId(page, offnum);
				itup = (IndexTuple) PageGetItem(page, itemid);

				if (ABTreeTupleIsPosting(itup))
				{
					int n = ABTreeTupleGetNPosting(itup);
					
					if (agg_info->leaf_has_agg)
						agg_ptr = ABTreePostingTupleGetAggPtr(itup, agg_info);
					xid_ptr = ABTreePostingTupleGetXminPtr(itup);
					for (i = 0; i < n; ++i)
					{
						xid = (TransactionId) pg_atomic_read_u32(
							(pg_atomic_uint32 *) &xid_ptr[i]);
						if (_abt_xid_in_snapshot(xid, snapshot))
							continue;

						if (agg_info->leaf_has_agg)
						{
							agg = _abt_atomic_read_agg_ptr(agg_ptr, agg_info);
							agg_ptr += agg_info->agg_stride;
						}
						else
						{
							/* XXX this assumes int8, int4 or int2 */
							agg = (uint64) agg_info->agg_map_val;
						}
						acc += agg;
						if (acc > cutoff)
						{
							chosen_leaf_item_offnum = offnum;
							chosen_leaf_item_posting_off = i;
							goto done_sampling;
						}
					}
				}
				else
				{
					xid_ptr = ABTreeNonPivotTupleGetXminPtr(itup);
					xid = (TransactionId) pg_atomic_read_u32(
						(pg_atomic_uint32 *) xid_ptr);
					if (_abt_xid_in_snapshot(xid, snapshot))
						continue;

					if (agg_info->leaf_has_agg)
					{
						agg_ptr = ABTreeNonPivotTupleGetAggPtr(itup, agg_info);
						agg = _abt_atomic_read_agg_ptr(agg_ptr, agg_info);
					}
					else
					{
						/* XXX assumes int8, int4, or int2 */
						agg = (uint64) agg_info->agg_map_val;
					}
					acc += agg;
					if (acc > cutoff)
					{
						/* we found it.*/
						chosen_leaf_item_offnum = offnum;
						goto done_sampling;
					}
				}
			}
		}
		else
		{
			/* internal page */
			minoff = ABT_P_FIRSTDATAKEY(opaque);
			maxoff = PageGetMaxOffsetNumber(page);
			for (offnum = minoff; offnum <= maxoff; ++offnum)
			{
				itemid = PageGetItemId(page, offnum);
				itup = (IndexTuple) PageGetItem(page, itemid);

				agg = _abt_read_agg(rel, itup, snapshot, agg_info);
				acc += agg;

				if (acc > cutoff)
				{
					break;
				}
			}

			if (offnum <= maxoff)
			{
				/* 
				 * Found it. Subtract the previous ``acc'' from cutoff
				 * before we continue.
				 */
				Assert(acc - agg <= cutoff);
				cutoff -= acc - agg;

				/* get the new descent target */
				blkno = ABTreeTupleGetDownLink(itup);
			
				/* set up expected high key */
				if (expected_hikey)
					pfree(expected_hikey);
				if (offnum == maxoff)
				{
					if (ABT_P_RIGHTMOST(opaque))
						expected_hikey = NULL;
					else
					{
						itemid = PageGetItemId(page, ABTP_HIKEY);
						expected_hikey = (IndexTuple) PageGetItem(page, itemid);
					}
				}
				else
				{
					itemid = PageGetItemId(page, offnum + 1);
					expected_hikey = (IndexTuple) PageGetItem(page, itemid);
				}
				if (expected_hikey)
					expected_hikey = CopyIndexTuple(expected_hikey);
				
				/* reinitialize the per-level state */
				_abt_mkscankey_extended(rel, expected_hikey, inskey_hikey);
				inskey_hikey->pivotsearch = true;
				acc = 0;
					
				/* and descend into the child page */
				buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
				continue;
			}
		}

			
		/* 
		 * The accumulated aggregates after including all visible items on
		 * this page is still no larger than cutoff. Do we have to move right?
		 */
		Assert(acc <= cutoff);
		
		/* This page is the last one because we have a matching high key. */
		if (prev_cmpres == 0 || ABT_P_RIGHTMOST(opaque))
		{
			/* reject the result */
			if (expected_hikey)
				pfree(expected_hikey);
			_abt_relbuf(rel, buf);
			return false;
		}
		
		/* Now move to the next page. */
		Assert(prev_cmpres < 0);
		blkno = opaque->abtpo_next;
		Assert(blkno != InvalidBlockNumber);
		buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
	}

done_sampling:
	/* 
	 * Ok, we have successfully selected one leaf item as sample and buf is
	 * pinned and read-locked. First check if it satisfies all the keys.
	 */
	indnatts = IndexRelationGetNumberOfAttributes(scan->indexRelation);
	itemid = PageGetItemId(page, chosen_leaf_item_offnum);
	itup = (IndexTuple) PageGetItem(page, itemid);

    /* non-uniform weight */
    if (agg_info->leaf_has_agg)
    {
        Pointer agg_ptr = ABTreeNonPivotTupleGetAggPtr(itup, agg_info);
        uint64 w = _abt_atomic_read_agg_ptr(agg_ptr, agg_info);
        so->inv_prob /= w;
    }

	if ((scan->ignore_killed_tuples && ItemIdIsDead(itemid)) ||
		!_abt_checkkeys(scan, itup, indnatts, ForwardScanDirection,
						&continuescan))
	{
		/* 
		 * This sample is dead or doesn't satisfy all keys and thus should be
		 * rejected. This is the same as in _abt_readpage().
		 */
		_abt_relbuf(rel, buf);
		return false;
	}
	
	/* 
	 * Finally, this is a valid sample to return. Store it in currPos and
	 * return to the caller.
	 */
	so->currPos.buf = buf;
	so->currPos.lsn = BufferGetLSNAtomic(buf);
	so->currPos.currPage = BufferGetBlockNumber(buf);
	so->currPos.nextPage = opaque->abtpo_next;
	so->currPos.moreLeft = so->currPos.moreRight = false;
	so->currPos.nextTupleOffset = 0;	

	if (ABTreeTupleIsPosting(itup))
	{
		_abt_setuppostingitems(so, 0, chosen_leaf_item_offnum,
			ABTreeTupleGetPostingN(itup, chosen_leaf_item_posting_off),
			itup);
	}
	else
	{
		_abt_saveitem(so, 0, chosen_leaf_item_offnum, itup);
	}
	so->currPos.firstItem = 0;
	so->currPos.lastItem = 0;
	so->currPos.itemIndex = 0;
	
	/* drop the lock on the leaf and maybe the pin as well. */
	_abt_drop_lock_and_maybe_pin(scan, &so->currPos);

	/* also fill in the xs_heaptid and xs_itup */
	scan->xs_heaptid = so->currPos.items[0].heapTid;
	if (scan->xs_want_itup)
	{
		/* We only have only tuple saved, and its offset must be zero. */
		Assert(so->currPos.items[0].tupleOffset == 0);
		scan->xs_itup = (IndexTuple) (so->currTuples);
	}
	return true;
}

/*
 * _abt_build_start_inskey() -- Build the insertion-type key used to search for
 *		the starting point.
 *
 *	This routine was part of _abt_first() and now is shared by abtsampletuple()
 *	and _abt_first(). We cache the resulting inskey in ABTScanOpaque if there
 *	is one because _abt_sample() will repeatedly use this inskey to perform
 *	binsrch() until abtrescan() is called.
 */
void
_abt_build_start_inskey(IndexScanDesc scan, ScanDirection dir)
{
	Relation		rel = scan->indexRelation;
	ABTScanOpaque	so = (ABTScanOpaque) scan->opaque;
	StrategyNumber	strat;
	StrategyNumber	strat_total;
	ScanKey			startKeys[INDEX_MAX_KEYS];
	ScanKeyData		notnullkeys[INDEX_MAX_KEYS];
	int				keysCount = 0;
	int				inskey_size;
	bool			nextkey;
	bool			goback;
	int				i;

	Assert(so->preprocessed);
	/* The previous rescan call should have already reset inskey to NULL. */
	Assert(!so->inskey);
	
	/* The key is unsatisfiable, just return. */
	if (!so->qual_ok)
		return ;

	/*----------
	 * Examine the scan keys to discover where we need to start the scan.
	 *
	 * We want to identify the keys that can be used as starting boundaries;
	 * these are =, >, or >= keys for a forward scan or =, <, <= keys for
	 * a backwards scan.  We can use keys for multiple attributes so long as
	 * the prior attributes had only =, >= (resp. =, <=) keys.  Once we accept
	 * a > or < boundary or find an attribute with no boundary (which can be
	 * thought of as the same as "> -infinity"), we can't use keys for any
	 * attributes to its right, because it would break our simplistic notion
	 * of what initial positioning strategy to use.
	 *
	 * When the scan keys include cross-type operators, _abt_preprocess_keys
	 * may not be able to eliminate redundant keys; in such cases we will
	 * arbitrarily pick a usable one for each attribute.  This is correct
	 * but possibly not optimal behavior.  (For example, with keys like
	 * "x >= 4 AND x >= 5" we would elect to scan starting at x=4 when
	 * x=5 would be more efficient.)  Since the situation only arises given
	 * a poorly-worded query plus an incomplete opfamily, live with it.
	 *
	 * When both equality and inequality keys appear for a single attribute
	 * (again, only possible when cross-type operators appear), we *must*
	 * select one of the equality keys for the starting point, because
	 * _abt_checkkeys() will stop the scan as soon as an equality qual fails.
	 * For example, if we have keys like "x >= 4 AND x = 10" and we elect to
	 * start at x=4, we will fail and stop before reaching x=10.  If multiple
	 * equality quals survive preprocessing, however, it doesn't matter which
	 * one we use --- by definition, they are either redundant or
	 * contradictory.
	 *
	 * Any regular (not SK_SEARCHNULL) key implies a NOT NULL qualifier.
	 * If the index stores nulls at the end of the index we'll be starting
	 * from, and we have no boundary key for the column (which means the key
	 * we deduced NOT NULL from is an inequality key that constrains the other
	 * end of the index), then we cons up an explicit SK_SEARCHNOTNULL key to
	 * use as a boundary key.  If we didn't do this, we might find ourselves
	 * traversing a lot of null entries at the start of the scan.
	 *
	 * In this loop, row-comparison keys are treated the same as keys on their
	 * first (leftmost) columns.  We'll add on lower-order columns of the row
	 * comparison below, if possible.
	 *
	 * The selected scan keys (at most one per index column) are remembered by
	 * storing their addresses into the local startKeys[] array.
	 *----------
	 */
	strat_total = BTEqualStrategyNumber;
	if (so->numberOfKeys > 0)
	{
		AttrNumber	curattr;
		ScanKey		chosen;
		ScanKey		impliesNN;
		ScanKey		cur;

		/*
		 * chosen is the so-far-chosen key for the current attribute, if any.
		 * We don't cast the decision in stone until we reach keys for the
		 * next attribute.
		 */
		curattr = 1;
		chosen = NULL;
		/* Also remember any scankey that implies a NOT NULL constraint */
		impliesNN = NULL;

		/*
		 * Loop iterates from 0 to numberOfKeys inclusive; we use the last
		 * pass to handle after-last-key processing.  Actual exit from the
		 * loop is at one of the "break" statements below.
		 */
		for (cur = so->keyData, i = 0;; cur++, i++)
		{
			if (i >= so->numberOfKeys || cur->sk_attno != curattr)
			{
				/*
				 * Done looking at keys for curattr.  If we didn't find a
				 * usable boundary key, see if we can deduce a NOT NULL key.
				 */
				if (chosen == NULL && impliesNN != NULL &&
					((impliesNN->sk_flags & SK_ABT_NULLS_FIRST) ?
					 ScanDirectionIsForward(dir) :
					 ScanDirectionIsBackward(dir)))
				{
					/* Yes, so build the key in notnullkeys[keysCount] */
					chosen = &notnullkeys[keysCount];
					ScanKeyEntryInitialize(chosen,
										   (SK_SEARCHNOTNULL | SK_ISNULL |
											(impliesNN->sk_flags &
											 (SK_ABT_DESC | SK_ABT_NULLS_FIRST))),
										   curattr,
										   ((impliesNN->sk_flags & SK_ABT_NULLS_FIRST) ?
											BTGreaterStrategyNumber :
											BTLessStrategyNumber),
										   InvalidOid,
										   InvalidOid,
										   InvalidOid,
										   (Datum) 0);
				}

				/*
				 * If we still didn't find a usable boundary key, quit; else
				 * save the boundary key pointer in startKeys.
				 */
				if (chosen == NULL)
					break;
				startKeys[keysCount++] = chosen;
	
				/* 
				 * We look for nulls in the first member in a row comparison.
				 * If there's one, the condition is unsatisfiable. Just mark
				 * qual_ok as false and return.
				 *
				 * This was in the conversion loop from startKeys to inskey. We
				 * do it here to simplify the logic when we add the subkeys
				 * to the inskey if the last key is a row comparison.
				 */
				if (chosen->sk_flags & SK_ROW_HEADER)
				{
					ScanKey subkey =
						(ScanKey) DatumGetPointer(chosen->sk_argument);
					if (subkey->sk_flags & SK_ISNULL)
					{
						so->qual_ok = false;
						return ;
					}
				}

				/*
				 * Adjust strat_total, and quit if we have stored a > or <
				 * key.
				 */
				strat = chosen->sk_strategy;
				if (strat != BTEqualStrategyNumber)
				{
					strat_total = strat;
					if (strat == BTGreaterStrategyNumber ||
						strat == BTLessStrategyNumber)
						break;
				}

				/*
				 * Done if that was the last attribute, or if next key is not
				 * in sequence (implying no boundary key is available for the
				 * next attribute).
				 */
				if (i >= so->numberOfKeys ||
					cur->sk_attno != curattr + 1)
					break;

				/*
				 * Reset for next attr.
				 */
				curattr = cur->sk_attno;
				chosen = NULL;
				impliesNN = NULL;
			}

			/*
			 * Can we use this key as a starting boundary for this attr?
			 *
			 * If not, does it imply a NOT NULL constraint?  (Because
			 * SK_SEARCHNULL keys are always assigned BTEqualStrategyNumber,
			 * *any* inequality key works for that; we need not test.)
			 */
			switch (cur->sk_strategy)
			{
				case BTLessStrategyNumber:
				case BTLessEqualStrategyNumber:
					if (chosen == NULL)
					{
						if (ScanDirectionIsBackward(dir))
							chosen = cur;
						else
							impliesNN = cur;
					}
					break;
				case BTEqualStrategyNumber:
					/* override any non-equality choice */
					chosen = cur;
					break;
				case BTGreaterEqualStrategyNumber:
				case BTGreaterStrategyNumber:
					if (chosen == NULL)
					{
						if (ScanDirectionIsForward(dir))
							chosen = cur;
						else
							impliesNN = cur;
					}
					break;
			}
		}
	}
	
	/*
	 * If we found no usable boundary keys, we'll leave the inskey to be NULL
	 * to indicate we should start from one end of the tree.
	 */
	if (keysCount == 0)
		return ;
	
	/*
	 * Normally, inskey_size is the same as keysCount, unless the last key is
	 * a row comparison and we are able to use some of the subkeys. Originally
	 * that was accounted for in the conversion loop to inskey, but we do need
	 * to compute it here to know how large the inskey structure needs to be.
	 */
	inskey_size = keysCount;
	if (startKeys[keysCount - 1]->sk_flags & SK_ROW_HEADER)
	{
		ScanKey cur = startKeys[keysCount - 1];
		ScanKey subkey =
			(ScanKey) DatumGetPointer(startKeys[keysCount - 1]->sk_argument);

		Assert(subkey->sk_flags & SK_ROW_MEMBER);
		Assert(!(subkey->sk_flags & SK_ISNULL));
	
		/* 
		 * A dry run loop that increments the inskey_size without actually
		 * copying the subkeys into the inskey (which has not been allocated
		 * yet).
		 */
		for (;;)
		{
			subkey++;
			Assert(subkey->sk_flags & SK_ROW_MEMBER);
			if (subkey->sk_attno != inskey_size + 1)
				break;	/* out-of-sequence, can't use it */
			if (subkey->sk_strategy != cur->sk_strategy)
				break;	/* wrong direction, can't use it */
			if (subkey->sk_flags & SK_ISNULL)
				break;	/* can't use null keys */
			Assert(inskey_size < INDEX_MAX_KEYS);
			inskey_size++;
			if (subkey->sk_flags & SK_ROW_END)
				break;
		}
	}
	
	/* Allocate the inskey structure. */
	Assert(inskey_size <= INDEX_MAX_KEYS);
	so->inskey = palloc(offsetof(ABTScanInsertData, scankeys) +
						sizeof(ScanKeyData) * inskey_size);

	/*
	 * We want to start the scan somewhere within the index.  Set up an
	 * insertion scankey we can use to search for the boundary point we
	 * identified above.  The insertion scankey is built using the keys
	 * identified by startKeys[].  (Remaining insertion scankey fields are
	 * initialized after initial-positioning strategy is finalized.)
	 */
	for (i = 0; i < keysCount; i++)
	{
		ScanKey		cur = startKeys[i];

		Assert(cur->sk_attno == i + 1);

		if (cur->sk_flags & SK_ROW_HEADER)
		{
			/*
			 * Row comparison header: look to the first row member instead.
			 *
			 * The member scankeys are already in insertion format (ie, they
			 * have sk_func = 3-way-comparison function). No need to check for
			 * NULL here, since we have moved that logic to the first loop.
			 */
			ScanKey		subkey = (ScanKey) DatumGetPointer(cur->sk_argument);

			Assert(subkey->sk_flags & SK_ROW_MEMBER);
			memcpy(so->inskey->scankeys + i, subkey, sizeof(ScanKeyData));

			/*
			 * If the row comparison is the last positioning key we accepted,
			 * try to add additional keys from the lower-order row members.
			 * (If we accepted independent conditions on additional index
			 * columns, we use those instead --- doesn't seem worth trying to
			 * determine which is more restrictive.)  Note that this is OK
			 * even if the row comparison is of ">" or "<" type, because the
			 * condition applied to all but the last row member is effectively
			 * ">=" or "<=", and so the extra keys don't break the positioning
			 * scheme.  But, by the same token, if we aren't able to use all
			 * the row members, then the part of the row comparison that we
			 * did use has to be treated as just a ">=" or "<=" condition, and
			 * so we'd better adjust strat_total accordingly.
			 */
			if (i == keysCount - 1)
			{
				bool		used_all_subkeys;

				Assert(!(subkey->sk_flags & SK_ROW_END));
				while (keysCount < inskey_size)
				{
					subkey++;
					Assert(subkey->sk_flags & SK_ROW_MEMBER);
					memcpy(so->inskey->scankeys + keysCount, subkey,
						   sizeof(ScanKeyData));
					keysCount++;
				}
				if (subkey->sk_flags & SK_ROW_END)
					used_all_subkeys = true;
				else
					used_all_subkeys = false;

				if (!used_all_subkeys)
				{
					switch (strat_total)
					{
						case BTLessStrategyNumber:
							strat_total = BTLessEqualStrategyNumber;
							break;
						case BTGreaterStrategyNumber:
							strat_total = BTGreaterEqualStrategyNumber;
							break;
					}
				}
				break;			/* done with outer loop */
			}
		}
		else
		{
			/*
			 * Ordinary comparison key.  Transform the search-style scan key
			 * to an insertion scan key by replacing the sk_func with the
			 * appropriate btree comparison function.
			 *
			 * If scankey operator is not a cross-type comparison, we can use
			 * the cached comparison function; otherwise gotta look it up in
			 * the catalogs.  (That can't lead to infinite recursion, since no
			 * indexscan initiated by syscache lookup will use cross-data-type
			 * operators.)
			 *
			 * We support the convention that sk_subtype == InvalidOid means
			 * the opclass input type; this is a hack to simplify life for
			 * ScanKeyInit().
			 */
			if (cur->sk_subtype == rel->rd_opcintype[i] ||
				cur->sk_subtype == InvalidOid)
			{
				FmgrInfo   *procinfo;

				procinfo = index_getprocinfo(rel, cur->sk_attno, ABTORDER_PROC);
				ScanKeyEntryInitializeWithInfo(so->inskey->scankeys + i,
											   cur->sk_flags,
											   cur->sk_attno,
											   InvalidStrategy,
											   cur->sk_subtype,
											   cur->sk_collation,
											   procinfo,
											   cur->sk_argument);
			}
			else
			{
				RegProcedure cmp_proc;

				cmp_proc = get_opfamily_proc(rel->rd_opfamily[i],
											 rel->rd_opcintype[i],
											 cur->sk_subtype,
											 ABTORDER_PROC);
				if (!RegProcedureIsValid(cmp_proc))
					elog(ERROR, "missing support function %d(%u,%u) for attribute %d of index \"%s\"",
						 ABTORDER_PROC, rel->rd_opcintype[i], cur->sk_subtype,
						 cur->sk_attno, RelationGetRelationName(rel));
				ScanKeyEntryInitialize(so->inskey->scankeys + i,
									   cur->sk_flags,
									   cur->sk_attno,
									   InvalidStrategy,
									   cur->sk_subtype,
									   cur->sk_collation,
									   cmp_proc,
									   cur->sk_argument);
			}
		}
	}
	
	/* We should have copied over all inskey data. */
	Assert(keysCount == inskey_size);

	/*----------
	 * Examine the selected initial-positioning strategy to determine exactly
	 * where we need to start the scan, and set flag variables to control the
	 * code below.
	 *
	 * If nextkey = false, _abt_search and _abt_binsrch will locate the first
	 * item >= scan key.  If nextkey = true, they will locate the first
	 * item > scan key.
	 *
	 * If goback = true, we will then step back one item, while if
	 * goback = false, we will start the scan on the located item.
	 *----------
	 */
	switch (strat_total)
	{
		case BTLessStrategyNumber:

			/*
			 * Find first item >= scankey, then back up one to arrive at last
			 * item < scankey.  (Note: this positioning strategy is only used
			 * for a backward scan, so that is always the correct starting
			 * position.)
			 */
			nextkey = false;
			goback = true;
			break;

		case BTLessEqualStrategyNumber:

			/*
			 * Find first item > scankey, then back up one to arrive at last
			 * item <= scankey.  (Note: this positioning strategy is only used
			 * for a backward scan, so that is always the correct starting
			 * position.)
			 */
			nextkey = true;
			goback = true;
			break;

		case BTEqualStrategyNumber:

			/*
			 * If a backward scan was specified, need to start with last equal
			 * item not first one.
			 */
			if (ScanDirectionIsBackward(dir))
			{
				/*
				 * This is the same as the <= strategy.  We will check at the
				 * end whether the found item is actually =.
				 */
				nextkey = true;
				goback = true;
			}
			else
			{
				/*
				 * This is the same as the >= strategy.  We will check at the
				 * end whether the found item is actually =.
				 */
				nextkey = false;
				goback = false;
			}
			break;

		case BTGreaterEqualStrategyNumber:

			/*
			 * Find first item >= scankey.  (This is only used for forward
			 * scans.)
			 */
			nextkey = false;
			goback = false;
			break;

		case BTGreaterStrategyNumber:

			/*
			 * Find first item > scankey.  (This is only used for forward
			 * scans.)
			 */
			nextkey = true;
			goback = false;
			break;

		default:
			/* can't get here, but keep compiler quiet */
			elog(ERROR, "unrecognized strat_total: %d", (int) strat_total);
	}

	/* Initialize remaining insertion scan key fields */
	_abt_metaversion(rel, &so->inskey->heapkeyspace, &so->inskey->allequalimage);
	so->inskey->anynullkeys = false; /* unused */
	so->inskey->nextkey = nextkey;
	so->inskey->pivotsearch = false;
	so->inskey->scantid = NULL;
	so->inskey->keysz = keysCount;

	/* And the goback bit which _abt_first() needs. */
	so->goback = goback;
}

/*
 *	_abt_endpoint() -- Find the first or last page in the index, and scan
 * from there to the first key satisfying all the quals.
 *
 * This is used by _abt_first() to set up a scan when we've determined
 * that the scan must start at the beginning or end of the index (for
 * a forward or backward scan respectively).  Exit conditions are the
 * same as for _abt_first().
 */
static bool
_abt_endpoint(IndexScanDesc scan, ScanDirection dir)
{
	Relation	rel = scan->indexRelation;
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	Buffer		buf;
	Page		page;
	ABTPageOpaque opaque;
	OffsetNumber start;
	ABTScanPosItem *currItem;

	/*
	 * Scan down to the leftmost or rightmost leaf page.  This is a simplified
	 * version of _abt_search().  We don't maintain a stack since we know we
	 * won't need it.
	 */
	buf = _abt_get_endpoint(rel, 0, ScanDirectionIsBackward(dir), scan->xs_snapshot);

	if (!BufferIsValid(buf))
	{
		/*
		 * Empty index. Lock the whole relation, as nothing finer to lock
		 * exists.
		 */
		PredicateLockRelation(rel, scan->xs_snapshot);
		ABTScanPosInvalidate(so->currPos);
		return false;
	}

	PredicateLockPage(rel, BufferGetBlockNumber(buf), scan->xs_snapshot);
	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	Assert(ABT_P_ISLEAF(opaque));

	if (ScanDirectionIsForward(dir))
	{
		/* There could be dead pages to the left, so not this: */
		/* Assert(ABT_P_LEFTMOST(opaque)); */

		start = ABT_P_FIRSTDATAKEY(opaque);
	}
	else if (ScanDirectionIsBackward(dir))
	{
		Assert(ABT_P_RIGHTMOST(opaque));

		start = PageGetMaxOffsetNumber(page);
	}
	else
	{
		elog(ERROR, "invalid scan direction: %d", (int) dir);
		start = 0;				/* keep compiler quiet */
	}

	/* remember which buffer we have pinned */
	so->currPos.buf = buf;

	_abt_initialize_more_data(so, dir);

	/*
	 * Now load data from the first page of the scan.
	 */
	if (!_abt_readpage(scan, dir, start))
	{
		/*
		 * There's no actually-matching data on this page.  Try to advance to
		 * the next page.  Return false if there's no matching data at all.
		 */
		LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
		if (!_abt_steppage(scan, dir))
			return false;
	}
	else
	{
		/* Drop the lock, and maybe the pin, on the current page */
		_abt_drop_lock_and_maybe_pin(scan, &so->currPos);
	}

	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;
	if (scan->xs_want_itup)
		scan->xs_itup = (IndexTuple) (so->currTuples + currItem->tupleOffset);

	return true;
}

/*
 * _abt_initialize_more_data() -- initialize moreLeft/moreRight appropriately
 * for scan direction
 */
static inline void
_abt_initialize_more_data(ABTScanOpaque so, ScanDirection dir)
{
	/* initialize moreLeft/moreRight appropriately for scan direction */
	if (ScanDirectionIsForward(dir))
	{
		so->currPos.moreLeft = false;
		so->currPos.moreRight = true;
	}
	else
	{
		so->currPos.moreLeft = true;
		so->currPos.moreRight = false;
	}
	so->numKilled = 0;			/* just paranoia */
	so->markItemIndex = -1;		/* ditto */
}

static Buffer
_abt_moveright_to_next_live_page(Relation rel, Buffer buf)
{
	BlockNumber		blkno;
	Page			page;
	ABTPageOpaque	opaque;

	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	while (ABT_P_IGNORE(opaque)) {
		blkno = opaque->abtpo_next;
		buf = _abt_relandgetbuf(rel, buf, blkno, ABT_READ);
		page = BufferGetPage(buf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		if (ABT_P_RIGHTMOST(opaque))
		{
			_abt_relbuf(rel, buf);
			return InvalidBuffer;
		}
	}

	return buf;
}
