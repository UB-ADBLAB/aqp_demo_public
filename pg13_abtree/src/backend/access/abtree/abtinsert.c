/*-------------------------------------------------------------------------
 *
 * nbtinsert.c
 *	  Item insertion in Lehman and Yao btrees for Postgres.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtinsert.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/abtree.h"
#include "access/abtxlog.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr.h"

/* Minimum tree height for application of fastpath optimization */
#define ABTREE_FASTPATH_MIN_LEVEL	2

static TransactionId _abt_check_unique(Relation rel, ABTInsertState insertstate,
									  Relation heapRel,
									  IndexUniqueCheck checkUnique, bool *is_unique,
									  uint32 *speculativeToken);
static OffsetNumber _abt_findinsertloc(Relation rel,
									  ABTInsertState insertstate,
									  bool checkingunique,
									  ABTStack stack,
									  Relation heapRel,
									  ABTCachedAggInfo agg_info);
static void _abt_stepright(Relation rel, ABTInsertState insertstate,
						   ABTStack stack, ABTCachedAggInfo agg_info);
static void _abt_insertonpg(Relation rel, ABTScanInsert itup_key,
						   Buffer buf,
						   Buffer cbuf,
						   ABTStack *ptr_to_stack,
						   IndexTuple itup,
						   Size itemsz,
						   OffsetNumber newitemoff,
						   int postingoff,
						   bool split_only_page,
						   ABTCachedAggInfo agg_info,
						   Datum previtem_newagg);
static Buffer _abt_split(Relation rel, ABTScanInsert itup_key, Buffer buf,
						Buffer cbuf, OffsetNumber newitemoff, Size newitemsz,
						IndexTuple newitem, IndexTuple orignewitem,
						IndexTuple nposting, uint16 postingoff,
						ABTCachedAggInfo agg_info,
						Datum previtem_newagg);
static ABTStack _abt_insert_parent(Relation rel, Buffer buf, Buffer rbuf,
								   ABTStack stack,
								   bool is_root, bool is_only,
								   ABTCachedAggInfo agg_info);
static Buffer _abt_newroot(Relation rel, Buffer lbuf, Buffer rbuf,
						   ABTCachedAggInfo agg_info, Datum lagg, Datum ragg);
static inline bool _abt_pgaddtup(Page page, Size itemsize, IndexTuple itup,
								OffsetNumber itup_off, bool newfirstdataitem,
								ABTCachedAggInfo agg_info);
static void _abt_vacuum_one_page(Relation rel, Buffer buffer, Relation heapRel);
static void _abt_fix_path_aggregation_itup(Relation rel, ABTScanInsert itup_key,
										   IndexTuple inserted_itup,
										   OffsetNumber newitemoff,
										   int postingoff, ABTStack stack,
										   BlockNumber leaf_blkno,
										   ABTCachedAggInfo agg_info);
static OffsetNumber _abt_try_find_downlink(Page page, OffsetNumber start_off,
										   BlockNumber cblkno);
static Buffer _abt_moveright_and_binsrch(Relation rel, ABTScanInsert itup_key,
										 Buffer buf, ABTStack stack);

/*
 *	_abt_doinsert() -- Handle insertion of a single index tuple in the tree.
 *
 *		This routine is called by the public interface routine, btinsert.
 *		By here, itup is filled in, including the TID and the aggregation value.
 *
 *		If checkUnique is UNIQUE_CHECK_NO or UNIQUE_CHECK_PARTIAL, this
 *		will allow duplicates.  Otherwise (UNIQUE_CHECK_YES or
 *		UNIQUE_CHECK_EXISTING) it will throw error for a duplicate.
 *		For UNIQUE_CHECK_EXISTING we merely run the duplicate check, and
 *		don't actually insert.
 *
 *		The result value is only significant for UNIQUE_CHECK_PARTIAL:
 *		it must be true if the entry is known unique, else false.
 *		(In the current implementation we'll also return true after a
 *		successful UNIQUE_CHECK_YES or UNIQUE_CHECK_EXISTING call, but
 *		that's just a coding artifact.)
 */
bool
_abt_doinsert(Relation rel, IndexTuple itup,
			 IndexUniqueCheck checkUnique, Relation heapRel,
			 ABTCachedAggInfo agg_info)
{
	bool		is_unique = false;
	ABTInsertStateData insertstate;
	ABTScanInsert itup_key;
	ABTStack		stack;
	bool		checkingunique = (checkUnique != UNIQUE_CHECK_NO);

	/* we need an insertion scan key to do our search, so build one */
	itup_key = _abt_mkscankey(rel, itup);

	if (checkingunique)
	{
		if (!itup_key->anynullkeys)
		{
			/* No (heapkeyspace) scantid until uniqueness established */
			itup_key->scantid = NULL;
		}
		else
		{
			/*
			 * Scan key for new tuple contains NULL key values.  Bypass
			 * checkingunique steps.  They are unnecessary because core code
			 * considers NULL unequal to every value, including NULL.
			 *
			 * This optimization avoids O(N^2) behavior within the
			 * _abt_findinsertloc() heapkeyspace path when a unique index has a
			 * large number of "duplicates" with NULL key values.
			 */
			checkingunique = false;
			/* Tuple is unique in the sense that core code cares about */
			Assert(checkUnique != UNIQUE_CHECK_EXISTING);
			is_unique = true;
		}
	}

	/*
	 * Fill in the ABTInsertState working area, to track the current page and
	 * position within the page to insert on.
	 *
	 * Note that itemsz is passed down to lower level code that deals with
	 * inserting the item.  It must be MAXALIGN()'d.  This ensures that space
	 * accounting code consistently considers the alignment overhead that we
	 * expect PageAddItem() will add later.  (Actually, index_form_tuple() is
	 * already conservative about alignment, but we don't rely on that from
	 * this distance.  Besides, preserving the "true" tuple size in index
	 * tuple headers for the benefit of nbtsplitloc.c might happen someday.
	 * Note that heapam does not MAXALIGN() each heap tuple's lp_len field.)
	 */
	insertstate.itup = itup;
	insertstate.itemsz = MAXALIGN(IndexTupleSize(itup));
	insertstate.itup_key = itup_key;
	insertstate.bounds_valid = false;
	insertstate.buf = InvalidBuffer;
	insertstate.postingoff = 0;

search:

	/*
	 * Find and lock the leaf page that the tuple should be added to by
	 * searching from the root page.  insertstate.buf will hold a buffer that
	 * is locked in exclusive mode afterwards.
	 */
	stack = _abt_search(rel, insertstate.itup_key, &insertstate.buf,
						ABT_WRITE, /* snapshot = */NULL, agg_info);

	/*
	 * checkingunique inserts are not allowed to go ahead when two tuples with
	 * equal key attribute values would be visible to new MVCC snapshots once
	 * the xact commits.  Check for conflicts in the locked page/buffer (if
	 * needed) here.
	 *
	 * It might be necessary to check a page to the right in _abt_check_unique,
	 * though that should be very rare.  In practice the first page the value
	 * could be on (with scantid omitted) is almost always also the only page
	 * that a matching tuple might be found on.  This is due to the behavior
	 * of _abt_findsplitloc with duplicate tuples -- a group of duplicates can
	 * only be allowed to cross a page boundary when there is no candidate
	 * leaf page split point that avoids it.  Also, _abt_check_unique can use
	 * the leaf page high key to determine that there will be no duplicates on
	 * the right sibling without actually visiting it (it uses the high key in
	 * cases where the new item happens to belong at the far right of the leaf
	 * page).
	 *
	 * NOTE: obviously, _abt_check_unique can only detect keys that are already
	 * in the index; so it cannot defend against concurrent insertions of the
	 * same key.  We protect against that by means of holding a write lock on
	 * the first page the value could be on, with omitted/-inf value for the
	 * implicit heap TID tiebreaker attribute.  Any other would-be inserter of
	 * the same key must acquire a write lock on the same page, so only one
	 * would-be inserter can be making the check at one time.  Furthermore,
	 * once we are past the check we hold write locks continuously until we
	 * have performed our insertion, so no later inserter can fail to see our
	 * insertion.  (This requires some care in _abt_findinsertloc.)
	 *
	 * If we must wait for another xact, we release the lock while waiting,
	 * and then must perform a new search.
	 *
	 * For a partial uniqueness check, we don't wait for the other xact. Just
	 * let the tuple in and return false for possibly non-unique, or true for
	 * definitely unique.
	 */
	if (checkingunique)
	{
		TransactionId xwait;
		uint32		speculativeToken;

		xwait = _abt_check_unique(rel, &insertstate, heapRel, checkUnique,
								 &is_unique, &speculativeToken);

		if (unlikely(TransactionIdIsValid(xwait)))
		{
			/* Have to wait for the other guy ... */
			_abt_relbuf(rel, insertstate.buf);
			insertstate.buf = InvalidBuffer;
	
			/* Drop the pins and locks on the buffers of the path. */
			if (stack)
				_abt_freestack(rel, stack);

			/*
			 * If it's a speculative insertion, wait for it to finish (ie. to
			 * go ahead with the insertion, or kill the tuple).  Otherwise
			 * wait for the transaction to finish as usual.
			 */
			if (speculativeToken)
				SpeculativeInsertionWait(xwait, speculativeToken);
			else
				XactLockTableWait(xwait, rel, &itup->t_tid, XLTW_InsertIndex);

			/* start over... */
			goto search;
		}

		/* Uniqueness is established -- restore heap tid as scantid */
		if (itup_key->heapkeyspace)
			itup_key->scantid = &itup->t_tid;
	}

	if (checkUnique != UNIQUE_CHECK_EXISTING)
	{
		OffsetNumber newitemoff;
		BlockNumber  leaf_blkno;
		/* ABTStackData leaf_stack; */

		/*
		 * The only conflict predicate locking cares about for indexes is when
		 * an index tuple insert conflicts with an existing lock.  We don't
		 * know the actual page we're going to insert on for sure just yet in
		 * checkingunique and !heapkeyspace cases, but it's okay to use the
		 * first page the value could be on (with scantid omitted) instead.
		 */
		CheckForSerializableConflictIn(rel, NULL, BufferGetBlockNumber(insertstate.buf));

		/*
		 * Do the insertion.  Note that insertstate contains cached binary
		 * search bounds established within _abt_check_unique when insertion is
		 * checkingunique.
		 */
		newitemoff = _abt_findinsertloc(rel, &insertstate, checkingunique,
									   stack, heapRel, agg_info);
		leaf_blkno = BufferGetBlockNumber(insertstate.buf);

		/* 
		 * Set previtem_newagg and leafitem_in_left_subtree to a dummy value
		 * for leaf level.
		 */
		_abt_insertonpg(rel, itup_key, insertstate.buf, InvalidBuffer,
						&stack, itup, insertstate.itemsz,
						newitemoff, insertstate.postingoff, false, agg_info,
					    /*previtem_newagg = */Int16GetDatum(0));
		/* no lock is hold on any page at this point */
	
		/* descend in the tree again to fix the all the aggregates */
		_abt_fix_path_aggregation_itup(rel, itup_key, itup, newitemoff,
				insertstate.postingoff, stack, leaf_blkno, agg_info);
	}
	else
	{
		/* just release the buffer. */
		_abt_relbuf(rel, insertstate.buf);
	}

	/* 
	 * be tidy.
	 */
	if (stack)
		_abt_freestack(rel, stack);
	pfree(itup_key);

	return is_unique;
}

/*
 *	_abt_check_unique() -- Check for violation of unique index constraint
 *
 * Returns InvalidTransactionId if there is no conflict, else an xact ID
 * we must wait for to see if it commits a conflicting tuple.   If an actual
 * conflict is detected, no return --- just ereport().  If an xact ID is
 * returned, and the conflicting tuple still has a speculative insertion in
 * progress, *speculativeToken is set to non-zero, and the caller can wait for
 * the verdict on the insertion using SpeculativeInsertionWait().
 *
 * However, if checkUnique == UNIQUE_CHECK_PARTIAL, we always return
 * InvalidTransactionId because we don't want to wait.  In this case we
 * set *is_unique to false if there is a potential conflict, and the
 * core code must redo the uniqueness check later.
 *
 * As a side-effect, sets state in insertstate that can later be used by
 * _abt_findinsertloc() to reuse most of the binary search work we do
 * here.
 *
 * Do not call here when there are NULL values in scan key.  NULL should be
 * considered unequal to NULL when checking for duplicates, but we are not
 * prepared to handle that correctly.
 */
static TransactionId
_abt_check_unique(Relation rel, ABTInsertState insertstate, Relation heapRel,
				 IndexUniqueCheck checkUnique, bool *is_unique,
				 uint32 *speculativeToken)
{
	IndexTuple	itup = insertstate->itup;
	IndexTuple	curitup = NULL;
	ItemId		curitemid;
	ABTScanInsert itup_key = insertstate->itup_key;
	SnapshotData SnapshotDirty;
	OffsetNumber offset;
	OffsetNumber maxoff;
	Page		page;
	ABTPageOpaque opaque;
	Buffer		nbuf = InvalidBuffer;
	bool		found = false;
	bool		inposting = false;
	bool		prevalldead = true;
	int			curposti = 0;

	/* Assume unique until we find a duplicate */
	*is_unique = true;

	InitDirtySnapshot(SnapshotDirty);

	page = BufferGetPage(insertstate->buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Find the first tuple with the same key.
	 *
	 * This also saves the binary search bounds in insertstate.  We use them
	 * in the fastpath below, but also in the _abt_findinsertloc() call later.
	 */
	Assert(!insertstate->bounds_valid);
	offset = _abt_binsrch_insert(rel, insertstate);

	/*
	 * Scan over all equal tuples, looking for live conflicts.
	 */
	Assert(!insertstate->bounds_valid || insertstate->low == offset);
	Assert(!itup_key->anynullkeys);
	Assert(itup_key->scantid == NULL);
	for (;;)
	{
		/*
		 * Each iteration of the loop processes one heap TID, not one index
		 * tuple.  Current offset number for page isn't usually advanced on
		 * iterations that process heap TIDs from posting list tuples.
		 *
		 * "inposting" state is set when _inside_ a posting list --- not when
		 * we're at the start (or end) of a posting list.  We advance curposti
		 * at the end of the iteration when inside a posting list tuple.  In
		 * general, every loop iteration either advances the page offset or
		 * advances curposti --- an iteration that handles the rightmost/max
		 * heap TID in a posting list finally advances the page offset (and
		 * unsets "inposting").
		 *
		 * Make sure the offset points to an actual index tuple before trying
		 * to examine it...
		 */
		if (offset <= maxoff)
		{
			/*
			 * Fastpath: In most cases, we can use cached search bounds to
			 * limit our consideration to items that are definitely
			 * duplicates.  This fastpath doesn't apply when the original page
			 * is empty, or when initial offset is past the end of the
			 * original page, which may indicate that we need to examine a
			 * second or subsequent page.
			 *
			 * Note that this optimization allows us to avoid calling
			 * _abt_compare() directly when there are no duplicates, as long as
			 * the offset where the key will go is not at the end of the page.
			 */
			if (nbuf == InvalidBuffer && offset == insertstate->stricthigh)
			{
				Assert(insertstate->bounds_valid);
				Assert(insertstate->low >= ABT_P_FIRSTDATAKEY(opaque));
				Assert(insertstate->low <= insertstate->stricthigh);
				Assert(_abt_compare(rel, itup_key, page, offset) < 0);
				break;
			}

			/*
			 * We can skip items that are already marked killed.
			 *
			 * In the presence of heavy update activity an index may contain
			 * many killed items with the same key; running _abt_compare() on
			 * each killed item gets expensive.  Just advance over killed
			 * items as quickly as we can.  We only apply _abt_compare() when
			 * we get to a non-killed item.  We could reuse the bounds to
			 * avoid _abt_compare() calls for known equal tuples, but it
			 * doesn't seem worth it.  Workloads with heavy update activity
			 * tend to have many deduplication passes, so we'll often avoid
			 * most of those comparisons, too (we call _abt_compare() when the
			 * posting list tuple is initially encountered, though not when
			 * processing later TIDs from the same tuple).
			 */
			if (!inposting)
				curitemid = PageGetItemId(page, offset);
			if (inposting || !ItemIdIsDead(curitemid))
			{
				ItemPointerData htid;
				bool		all_dead = false;

				if (!inposting)
				{
					/* Plain tuple, or first TID in posting list tuple */
					if (_abt_compare(rel, itup_key, page, offset) != 0)
						break;	/* we're past all the equal tuples */

					/* Advanced curitup */
					curitup = (IndexTuple) PageGetItem(page, curitemid);
					Assert(!ABTreeTupleIsPivot(curitup));
				}

				/* okay, we gotta fetch the heap tuple using htid ... */
				if (!ABTreeTupleIsPosting(curitup))
				{
					/* ... htid is from simple non-pivot tuple */
					Assert(!inposting);
					htid = curitup->t_tid;
				}
				else if (!inposting)
				{
					/* ... htid is first TID in new posting list */
					inposting = true;
					prevalldead = true;
					curposti = 0;
					htid = *ABTreeTupleGetPostingN(curitup, 0);
				}
				else
				{
					/* ... htid is second or subsequent TID in posting list */
					Assert(curposti > 0);
					htid = *ABTreeTupleGetPostingN(curitup, curposti);
				}

				/*
				 * If we are doing a recheck, we expect to find the tuple we
				 * are rechecking.  It's not a duplicate, but we have to keep
				 * scanning.
				 */
				if (checkUnique == UNIQUE_CHECK_EXISTING &&
					ItemPointerCompare(&htid, &itup->t_tid) == 0)
				{
					found = true;
				}

				/*
				 * Check if there's any table tuples for this index entry
				 * satisfying SnapshotDirty. This is necessary because for AMs
				 * with optimizations like heap's HOT, we have just a single
				 * index entry for the entire chain.
				 */
				else if (table_index_fetch_tuple_check(heapRel, &htid,
													   &SnapshotDirty,
													   &all_dead))
				{
					TransactionId xwait;

					/*
					 * It is a duplicate. If we are only doing a partial
					 * check, then don't bother checking if the tuple is being
					 * updated in another transaction. Just return the fact
					 * that it is a potential conflict and leave the full
					 * check till later. Don't invalidate binary search
					 * bounds.
					 */
					if (checkUnique == UNIQUE_CHECK_PARTIAL)
					{
						if (nbuf != InvalidBuffer)
							_abt_relbuf(rel, nbuf);
						*is_unique = false;
						return InvalidTransactionId;
					}

					/*
					 * If this tuple is being updated by other transaction
					 * then we have to wait for its commit/abort.
					 */
					xwait = (TransactionIdIsValid(SnapshotDirty.xmin)) ?
						SnapshotDirty.xmin : SnapshotDirty.xmax;

					if (TransactionIdIsValid(xwait))
					{
						if (nbuf != InvalidBuffer)
							_abt_relbuf(rel, nbuf);
						/* Tell _abt_doinsert to wait... */
						*speculativeToken = SnapshotDirty.speculativeToken;
						/* Caller releases lock on buf immediately */
						insertstate->bounds_valid = false;
						return xwait;
					}

					/*
					 * Otherwise we have a definite conflict.  But before
					 * complaining, look to see if the tuple we want to insert
					 * is itself now committed dead --- if so, don't complain.
					 * This is a waste of time in normal scenarios but we must
					 * do it to support CREATE INDEX CONCURRENTLY.
					 *
					 * We must follow HOT-chains here because during
					 * concurrent index build, we insert the root TID though
					 * the actual tuple may be somewhere in the HOT-chain.
					 * While following the chain we might not stop at the
					 * exact tuple which triggered the insert, but that's OK
					 * because if we find a live tuple anywhere in this chain,
					 * we have a unique key conflict.  The other live tuple is
					 * not part of this chain because it had a different index
					 * entry.
					 */
					htid = itup->t_tid;
					if (table_index_fetch_tuple_check(heapRel, &htid,
													  SnapshotSelf, NULL))
					{
						/* Normal case --- it's still live */
					}
					else
					{
						/*
						 * It's been deleted, so no error, and no need to
						 * continue searching
						 */
						break;
					}

					/*
					 * Check for a conflict-in as we would if we were going to
					 * write to this page.  We aren't actually going to write,
					 * but we want a chance to report SSI conflicts that would
					 * otherwise be masked by this unique constraint
					 * violation.
					 */
					CheckForSerializableConflictIn(rel, NULL, BufferGetBlockNumber(insertstate->buf));

					/*
					 * This is a definite conflict.  Break the tuple down into
					 * datums and report the error.  But first, make sure we
					 * release the buffer locks we're holding ---
					 * BuildIndexValueDescription could make catalog accesses,
					 * which in the worst case might touch this same index and
					 * cause deadlocks.
					 */
					if (nbuf != InvalidBuffer)
						_abt_relbuf(rel, nbuf);
					_abt_relbuf(rel, insertstate->buf);
					insertstate->buf = InvalidBuffer;
					insertstate->bounds_valid = false;

					{
						Datum		values[INDEX_MAX_KEYS];
						bool		isnull[INDEX_MAX_KEYS];
						char	   *key_desc;

						index_deform_tuple(itup, RelationGetDescr(rel),
										   values, isnull);

						key_desc = BuildIndexValueDescription(rel, values,
															  isnull);

						ereport(ERROR,
								(errcode(ERRCODE_UNIQUE_VIOLATION),
								 errmsg("duplicate key value violates unique constraint \"%s\"",
										RelationGetRelationName(rel)),
								 key_desc ? errdetail("Key %s already exists.",
													  key_desc) : 0,
								 errtableconstraint(heapRel,
													RelationGetRelationName(rel))));
					}
				}
				else if (all_dead && (!inposting ||
									  (prevalldead &&
									   curposti == ABTreeTupleGetNPosting(curitup) - 1)))
				{
					/*
					 * The conflicting tuple (or all HOT chains pointed to by
					 * all posting list TIDs) is dead to everyone, so mark the
					 * index entry killed.
					 */
					ItemIdMarkDead(curitemid);
					opaque->abtpo_flags |= ABTP_HAS_GARBAGE;

					/*
					 * Mark buffer with a dirty hint, since state is not
					 * crucial. Be sure to mark the proper buffer dirty.
					 */
					if (nbuf != InvalidBuffer)
						MarkBufferDirtyHint(nbuf, true);
					else
						MarkBufferDirtyHint(insertstate->buf, true);
				}

				/*
				 * Remember if posting list tuple has even a single HOT chain
				 * whose members are not all dead
				 */
				if (!all_dead && inposting)
					prevalldead = false;
			}
		}

		if (inposting && curposti < ABTreeTupleGetNPosting(curitup) - 1)
		{
			/* Advance to next TID in same posting list */
			curposti++;
			continue;
		}
		else if (offset < maxoff)
		{
			/* Advance to next tuple */
			curposti = 0;
			inposting = false;
			offset = OffsetNumberNext(offset);
		}
		else
		{
			int			highkeycmp;

			/* If scankey == hikey we gotta check the next page too */
			if (ABT_P_RIGHTMOST(opaque))
				break;
			highkeycmp = _abt_compare(rel, itup_key, page, ABTP_HIKEY);
			Assert(highkeycmp <= 0);
			if (highkeycmp != 0)
				break;
			/* Advance to next non-dead page --- there must be one */
			for (;;)
			{
				BlockNumber nblkno = opaque->abtpo_next;

				nbuf = _abt_relandgetbuf(rel, nbuf, nblkno, ABT_READ);
				page = BufferGetPage(nbuf);
				opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
				if (!ABT_P_IGNORE(opaque))
					break;
				if (ABT_P_RIGHTMOST(opaque))
					elog(ERROR, "fell off the end of index \"%s\"",
						 RelationGetRelationName(rel));
			}
			/* Will also advance to next tuple */
			curposti = 0;
			inposting = false;
			maxoff = PageGetMaxOffsetNumber(page);
			offset = ABT_P_FIRSTDATAKEY(opaque);
			/* Don't invalidate binary search bounds */
		}
	}

	/*
	 * If we are doing a recheck then we should have found the tuple we are
	 * checking.  Otherwise there's something very wrong --- probably, the
	 * index is on a non-immutable expression.
	 */
	if (checkUnique == UNIQUE_CHECK_EXISTING && !found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to re-find tuple within index \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("This may be because of a non-immutable index expression."),
				 errtableconstraint(heapRel,
									RelationGetRelationName(rel))));

	if (nbuf != InvalidBuffer)
		_abt_relbuf(rel, nbuf);

	return InvalidTransactionId;
}


/*
 *	_abt_findinsertloc() -- Finds an insert location for a tuple
 *
 *		On entry, insertstate buffer contains the page the new tuple belongs
 *		on.  It is exclusive-locked and pinned by the caller.
 *
 *		If 'checkingunique' is true, the buffer on entry is the first page
 *		that contains duplicates of the new key.  If there are duplicates on
 *		multiple pages, the correct insertion position might be some page to
 *		the right, rather than the first page.  In that case, this function
 *		moves right to the correct target page.
 *
 *		(In a !heapkeyspace index, there can be multiple pages with the same
 *		high key, where the new tuple could legitimately be placed on.  In
 *		that case, the caller passes the first page containing duplicates,
 *		just like when checkingunique=true.  If that page doesn't have enough
 *		room for the new tuple, this function moves right, trying to find a
 *		legal page that does.)
 *
 *		On exit, insertstate buffer contains the chosen insertion page, and
 *		the offset within that page is returned.  If _abt_findinsertloc needed
 *		to move right, the lock and pin on the original page are released, and
 *		the new buffer is exclusively locked and pinned instead.
 *
 *		If insertstate contains cached binary search bounds, we will take
 *		advantage of them.  This avoids repeating comparisons that we made in
 *		_abt_check_unique() already.
 *
 *		If there is not enough room on the page for the new tuple, we try to
 *		make room by removing any LP_DEAD tuples.
 */
static OffsetNumber
_abt_findinsertloc(Relation rel,
				  ABTInsertState insertstate,
				  bool checkingunique,
				  ABTStack stack,
				  Relation heapRel,
				  ABTCachedAggInfo agg_info)
{
	ABTScanInsert itup_key = insertstate->itup_key;
	Page		page = BufferGetPage(insertstate->buf);
	ABTPageOpaque lpageop;
	OffsetNumber newitemoff;

	lpageop = (ABTPageOpaque) PageGetSpecialPointer(page);

	/* Check 1/3 of a page restriction */
	if (unlikely(insertstate->itemsz > ABTMaxItemSize(page, agg_info)))
		_abt_check_third_page(rel, heapRel, page, insertstate->itup, agg_info);

	/* Aggregate B-tree is always heapkeyspace. */
	Assert(itup_key->heapkeyspace);
	Assert(ABT_P_ISLEAF(lpageop) && !ABT_P_INCOMPLETE_SPLIT(lpageop));
	Assert(!insertstate->bounds_valid || checkingunique);
	Assert(itup_key->scantid != NULL);
	
	{
		/* Keep track of whether checkingunique duplicate seen */
		bool		uniquedup = false;

		/*
		 * If we're inserting into a unique index, we may have to walk right
		 * through leaf pages to find the one leaf page that we must insert on
		 * to.
		 *
		 * This is needed for checkingunique callers because a scantid was not
		 * used when we called _abt_search().  scantid can only be set after
		 * _abt_check_unique() has checked for duplicates.  The buffer
		 * initially stored in insertstate->buf has the page where the first
		 * duplicate key might be found, which isn't always the page that new
		 * tuple belongs on.  The heap TID attribute for new tuple (scantid)
		 * could force us to insert on a sibling page, though that should be
		 * very rare in practice.
		 */
		if (checkingunique)
		{
			if (insertstate->low < insertstate->stricthigh)
			{
				/* Encountered a duplicate in _abt_check_unique() */
				Assert(insertstate->bounds_valid);
				uniquedup = true;
			}

			for (;;)
			{
				/*
				 * Does the new tuple belong on this page?
				 *
				 * The earlier _abt_check_unique() call may well have
				 * established a strict upper bound on the offset for the new
				 * item.  If it's not the last item of the page (i.e. if there
				 * is at least one tuple on the page that goes after the tuple
				 * we're inserting) then we know that the tuple belongs on
				 * this page.  We can skip the high key check.
				 */
				if (insertstate->bounds_valid &&
					insertstate->low <= insertstate->stricthigh &&
					insertstate->stricthigh <= PageGetMaxOffsetNumber(page))
					break;

				/* Test '<=', not '!=', since scantid is set now */
				if (ABT_P_RIGHTMOST(lpageop) ||
					_abt_compare(rel, itup_key, page, ABTP_HIKEY) <= 0)
					break;

				_abt_stepright(rel, insertstate, stack, agg_info);
				/* Update local state after stepping right */
				page = BufferGetPage(insertstate->buf);
				lpageop = (ABTPageOpaque) PageGetSpecialPointer(page);
				/* Assume duplicates (if checkingunique) */
				uniquedup = true;
			}
		}

		/*
		 * If the target page is full, see if we can obtain enough space by
		 * erasing LP_DEAD items.  If that fails to free enough space, see if
		 * we can avoid a page split by performing a deduplication pass over
		 * the page.
		 *
		 * We only perform a deduplication pass for a checkingunique caller
		 * when the incoming item is a duplicate of an existing item on the
		 * leaf page.  This heuristic avoids wasting cycles -- we only expect
		 * to benefit from deduplicating a unique index page when most or all
		 * recently added items are duplicates.  See nbtree/README.
		 */
		if (PageGetFreeSpace(page) < insertstate->itemsz)
		{
			if (ABT_P_HAS_GARBAGE(lpageop))
			{
				_abt_vacuum_one_page(rel, insertstate->buf, heapRel);
				insertstate->bounds_valid = false;

				/* Might as well assume duplicates (if checkingunique) */
				uniquedup = true;
			}

			if (itup_key->allequalimage && ABTGetDeduplicateItems(rel) &&
				(!checkingunique || uniquedup) &&
				PageGetFreeSpace(page) < insertstate->itemsz)
			{
				_abt_dedup_one_page(rel, insertstate->buf, heapRel,
									insertstate->itup, insertstate->itemsz,
									checkingunique, agg_info);
				insertstate->bounds_valid = false;
			}
		}
	}

	/*
	 * We should now be on the correct page.  Find the offset within the page
	 * for the new tuple. (Possibly reusing earlier search bounds.)
	 */
	Assert(ABT_P_RIGHTMOST(lpageop) ||
		   _abt_compare(rel, itup_key, page, ABTP_HIKEY) <= 0);

	newitemoff = _abt_binsrch_insert(rel, insertstate);

	if (insertstate->postingoff == -1)
	{
		/*
		 * There is an overlapping posting list tuple with its LP_DEAD bit
		 * set.  We don't want to unnecessarily unset its LP_DEAD bit while
		 * performing a posting list split, so delete all LP_DEAD items early.
		 * This is the only case where LP_DEAD deletes happen even though
		 * there is space for newitem on the page.
		 */
		_abt_vacuum_one_page(rel, insertstate->buf, heapRel);

		/*
		 * Do new binary search.  New insert location cannot overlap with any
		 * posting list now.
		 */
		insertstate->bounds_valid = false;
		insertstate->postingoff = 0;
		newitemoff = _abt_binsrch_insert(rel, insertstate);
		Assert(insertstate->postingoff == 0);
	}

	/* 
	 * We can't safely fix the aggregates without releasing the exclusive lock
	 * on the leaf page and track all the deleted tuples even if we have
	 * deleted something from the leaf page in the _abt_vacuum_one_page() or
	 * _abt_dedup_one_page() calls. Note the vacuum or dedup are purely
	 * opportunistic to find space for the incoming tuple, and, hence, it would
	 * be reasonable to leave the updates to the next vacuum.
	 */
	return newitemoff;
}

/*
 * Step right to next non-dead page, during insertion.
 *
 * This is a bit more complicated than moving right in a search.  We must
 * write-lock the target page before releasing write lock on current page;
 * else someone else's _abt_check_unique scan could fail to see our insertion.
 * Write locks on intermediate dead pages won't do because we don't know when
 * they will get de-linked from the tree.
 *
 * This is more aggressive than it needs to be for non-unique !heapkeyspace
 * indexes.
 */
static void
_abt_stepright(Relation rel, ABTInsertState insertstate, ABTStack stack,
			   ABTCachedAggInfo agg_info)
{
	Page		page;
	ABTPageOpaque lpageop;
	Buffer		rbuf;
	BlockNumber rblkno;

	page = BufferGetPage(insertstate->buf);
	lpageop = (ABTPageOpaque) PageGetSpecialPointer(page);

	rbuf = InvalidBuffer;
	rblkno = lpageop->abtpo_next;
	for (;;)
	{
		rbuf = _abt_relandgetbuf(rel, rbuf, rblkno, ABT_WRITE);
		page = BufferGetPage(rbuf);
		lpageop = (ABTPageOpaque) PageGetSpecialPointer(page);

		/*
		 * If this page was incompletely split, finish the split now.  We do
		 * this while holding a lock on the left sibling, which is not good
		 * because finishing the split could be a fairly lengthy operation.
		 * But this should happen very seldom.
		 */
		if (ABT_P_INCOMPLETE_SPLIT(lpageop))
		{
			_abt_finish_split(rel, rbuf, stack, agg_info);
			rbuf = InvalidBuffer;
			continue;
		}

		if (!ABT_P_IGNORE(lpageop))
			break;
		if (ABT_P_RIGHTMOST(lpageop))
			elog(ERROR, "fell off the end of index \"%s\"",
				 RelationGetRelationName(rel));

		rblkno = lpageop->abtpo_next;
	}
	/* rbuf locked; unlock buf, update state for caller */
	_abt_relbuf(rel, insertstate->buf);
	insertstate->buf = rbuf;
	insertstate->bounds_valid = false;
}

/*----------
 *	_abt_insertonpg() -- Insert a tuple on a particular page in the index.
 *
 *		This recursive procedure does the following things:
 *
 *			+  if postingoff != 0, splits existing posting list tuple
 *			   (since it overlaps with new 'itup' tuple).
 *			+  if necessary, splits the target page, using 'itup_key' for
 *			   suffix truncation on leaf pages (caller passes NULL for
 *			   non-leaf pages).
 *			+  inserts the new tuple (might be split from posting list).
 *			+  if the page was split, pops the parent stack, and finds the
 *			   right place to insert the new child pointer (by walking
 *			   right using information stored in the parent stack).
 *			+  invokes itself with the appropriate tuple for the right
 *			   child page on the parent.
 *			+  updates the metapage if a true root or fast root is split.
 *
 *		On entry, we must have the correct buffer in which to do the
 *		insertion, and the buffer must be pinned and write-locked.  On return,
 *		we will have dropped both the pin and the lock on the buffer.
 *
 *		This routine only performs retail tuple insertions.  'itup' should
 *		always be either a non-highkey leaf item, or a downlink (new high
 *		key items are created indirectly, when a page is split).  When
 *		inserting to a non-leaf page, 'cbuf' is the left-sibling of the page
 *		we're inserting the downlink for.  This function will clear the
 *		INCOMPLETE_SPLIT flag on it, and release the buffer.
 *
 *		ptr_to_stack: pointer to the parent stack of buf. Must not be NULL but
 *		its value may be NULL. May be updated if its content is NULL, meaning
 *		buf pointed to a root/fastroot when we search for the insertion point.
 *		If updated, that stack frame may help a second descend in the tree.
 *
 *		previtem_newagg: the old pivot tuple's aggregation value. It will not
 *		be touched if buf is leaf.
 *
 *----------
 */
static void
_abt_insertonpg(Relation rel,
			   ABTScanInsert itup_key,
			   Buffer buf,
			   Buffer cbuf,
			   ABTStack *ptr_to_stack,
			   IndexTuple itup,
			   Size itemsz,
			   OffsetNumber newitemoff,
			   int postingoff,
			   bool split_only_page,
			   ABTCachedAggInfo agg_info,
			   Datum previtem_newagg)
{
	Page		page;
	ABTPageOpaque lpageop;
	IndexTuple	oposting = NULL;
	IndexTuple	origitup = NULL;
	IndexTuple	nposting = NULL;
	ABTStack	stack = *ptr_to_stack;

	page = BufferGetPage(buf);
	lpageop = (ABTPageOpaque) PageGetSpecialPointer(page);

	/* child buffer must be given iff inserting on an internal page */
	Assert(ABT_P_ISLEAF(lpageop) == !BufferIsValid(cbuf));
	/* tuple must have appropriate number of attributes */
	Assert(!ABT_P_ISLEAF(lpageop) ||
		   ABTreeTupleGetNAtts(itup, rel) ==
		   IndexRelationGetNumberOfAttributes(rel));
	Assert(ABT_P_ISLEAF(lpageop) ||
		   ABTreeTupleGetNAtts(itup, rel) <=
		   IndexRelationGetNumberOfKeyAttributes(rel));
	Assert(!ABTreeTupleIsPosting(itup));
	Assert(MAXALIGN(IndexTupleSize(itup)) == itemsz);

	/*
	 * Every internal page should have exactly one negative infinity item at
	 * all times.  Only _abt_split() and _abt_newroot() should add items that
	 * become negative infinity items through truncation, since they're the
	 * only routines that allocate new internal pages.
	 */
	Assert(ABT_P_ISLEAF(lpageop) || newitemoff > ABT_P_FIRSTDATAKEY(lpageop));

	/* The caller should've finished any incomplete splits already. */
	if (ABT_P_INCOMPLETE_SPLIT(lpageop))
		elog(ERROR, "cannot insert to incompletely split page %u",
			 BufferGetBlockNumber(buf));

	/*
	 * Do we need to split an existing posting list item?
	 */
	if (postingoff != 0)
	{
		ItemId		itemid = PageGetItemId(page, newitemoff);

		/*
		 * The new tuple is a duplicate with a heap TID that falls inside the
		 * range of an existing posting list tuple on a leaf page.  Prepare to
		 * split an existing posting list.  Overwriting the posting list with
		 * its post-split version is treated as an extra step in either the
		 * insert or page split critical section.
		 */
		Assert(ABT_P_ISLEAF(lpageop) && !ItemIdIsDead(itemid));
		Assert(itup_key->heapkeyspace && itup_key->allequalimage);
		oposting = (IndexTuple) PageGetItem(page, itemid);

		/* use a mutable copy of itup as our itup from here on */
		origitup = itup;
		itup = CopyIndexTuple(origitup);
		nposting = _abt_swap_posting(itup, oposting, postingoff, agg_info);
		/* 
		 * itup now contains rightmost/max TID from oposting as well as its
		 * leaf-level agg value if agg_info->leaf_has_agg == true.
		 */

		/* Alter offset so that newitem goes after posting list */
		newitemoff = OffsetNumberNext(newitemoff);
	}

	/*
	 * Do we need to split the page to fit the item on it?
	 *
	 * Note: PageGetFreeSpace() subtracts sizeof(ItemIdData) from its result,
	 * so this comparison is correct even though we appear to be accounting
	 * only for the item and not for its line pointer.
	 */
	if (PageGetFreeSpace(page) < itemsz)
	{
		bool		is_root = ABT_P_ISROOT(lpageop);
		bool		is_only = ABT_P_LEFTMOST(lpageop) && ABT_P_RIGHTMOST(lpageop);
		Buffer		rbuf;
		ABTStack	new_parentstack;

		Assert(!split_only_page);

		/* split the buffer into left and right halves */
		rbuf = _abt_split(rel, itup_key, buf, cbuf, newitemoff, itemsz, itup,
						 origitup, nposting, postingoff, agg_info,
						 previtem_newagg);
		PredicateLockPageSplit(rel,
							   BufferGetBlockNumber(buf),
							   BufferGetBlockNumber(rbuf));

		/*----------
		 * By here,
		 *
		 *		+  our target page has been split;
		 *		+  the original tuple has been inserted;
		 *		+  we have write locks on both the old (left half)
		 *		   and new (right half) buffers, after the split; and
		 *		+  we know the key we want to insert into the parent
		 *		   (it's the "high key" on the left child page).
		 *
		 * We're ready to do the parent insertion.  We need to hold onto the
		 * locks for the child pages until we locate the parent, but we can
		 * at least release the lock on the right child before doing the
		 * actual insertion.  The lock on the left child will be released
		 * last of all by parent insertion, where it is the 'cbuf' of parent
		 * page.
		 *----------
		 */
		new_parentstack =
			_abt_insert_parent(rel, buf, rbuf, stack, is_root, is_only, agg_info);

		/* 
		 * This page (buf) was the root/fast root if new_parentstack is not
		 * NULL. Update the current stack frame to save the new root in the
		 * stack.
		 */
		Assert(!new_parentstack || !stack);
		if (new_parentstack)
			*ptr_to_stack = new_parentstack;
	}
	else
	{
		bool		isleaf = ABT_P_ISLEAF(lpageop);
		/* bool		isrightmost = ABT_P_RIGHTMOST(lpageop); */
		Buffer		metabuf = InvalidBuffer;
		Page		metapg = NULL;
		ABTMetaPageData *metad = NULL;
		/* BlockNumber blockcache; */

		/*
		 * If we are doing this insert because we split a page that was the
		 * only one on its tree level, but was not the root, it may have been
		 * the "fast root".  We need to ensure that the fast root link points
		 * at or above the current page.  We can safely acquire a lock on the
		 * metapage here --- see comments for _abt_newroot().
		 */
		if (split_only_page)
		{
			Assert(!isleaf);
			Assert(BufferIsValid(cbuf));

			metabuf = _abt_getbuf(rel, ABTREE_METAPAGE, ABT_WRITE);
			metapg = BufferGetPage(metabuf);
			metad = ABTPageGetMeta(metapg);

			if (metad->abtm_fastlevel >= lpageop->abtpo.level)
			{
				/* no update wanted */
				_abt_relbuf(rel, metabuf);
				metabuf = InvalidBuffer;
			}
		}

		/* Do the update.  No ereport(ERROR) until changes are logged */
		START_CRIT_SECTION();

		if (postingoff != 0)
			memcpy(oposting, nposting, MAXALIGN(IndexTupleSize(nposting)));
	
		/* update the agg of the pivot tuple to the left child */
		if (!isleaf)
		{
			ItemId			previtemid;
			IndexTuple		previtem;
			OffsetNumber	previtem_off;
			ABTLastUpdateId	*last_update_id_ptr;

			previtem_off = OffsetNumberPrev(newitemoff);
			Assert(previtem_off >= ABT_P_FIRSTDATAKEY(lpageop));
			previtemid = PageGetItemId(page, previtem_off);
			previtem = (IndexTuple) PageGetItem(page, previtemid);
			_abt_set_pivot_tuple_aggregation_value(previtem,
												   previtem_newagg,
												   agg_info);

			/* 
			 * and also increment its last update id, this doesn't have to
			 * be xlog'd for the same reason below about page last update id.
			 */
			last_update_id_ptr = ABTreeTupleGetLastUpdateIdPtr(previtem);
			*last_update_id_ptr = ABTAdvanceLastUpdateId(*last_update_id_ptr);
		}

		if (PageAddItem(page, (Item) itup, itemsz, newitemoff, false,
						false) == InvalidOffsetNumber)
			elog(PANIC, "failed to add new item to block %u in index \"%s\"",
				 BufferGetBlockNumber(buf), RelationGetRelationName(rel));
	
		/* 
		 * Increment the last update id on the page we're insering into. This
		 * doesn't need to be XLOG'd as any reader of the last update id won't
		 * survive a crash.
		 */
		if (!isleaf)
			lpageop->abtpo_last_update_id = 
				ABTAdvanceLastUpdateId(lpageop->abtpo_last_update_id);

		MarkBufferDirty(buf);

		if (BufferIsValid(metabuf))
		{
			/* upgrade meta-page if needed */
			if (metad->abtm_version < ABTREE_NOVAC_VERSION)
				_abt_upgrademetapage(metapg);
			metad->abtm_fastroot = BufferGetBlockNumber(buf);
			metad->abtm_fastlevel = lpageop->abtpo.level;
			MarkBufferDirty(metabuf);
		}

		/*
		 * Clear INCOMPLETE_SPLIT flag on child if inserting the new item
		 * finishes a split
		 */
		if (!isleaf)
		{
			Page		cpage = BufferGetPage(cbuf);
			ABTPageOpaque cpageop = (ABTPageOpaque) PageGetSpecialPointer(cpage);

			Assert(ABT_P_INCOMPLETE_SPLIT(cpageop));
			cpageop->abtpo_flags &= ~ABTP_INCOMPLETE_SPLIT;
			MarkBufferDirty(cbuf);
		}

		/* XLOG stuff */
		if (RelationNeedsWAL(rel))
		{
			xl_abtree_insert xlrec;
			xl_abtree_metadata xlmeta;
			uint8		xlinfo;
			XLogRecPtr	recptr;
			uint16		upostingoff;

			xlrec.offnum = newitemoff;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfABtreeInsert);

			if (isleaf && postingoff == 0)
			{
				/* Simple leaf insert */
				xlinfo = XLOG_ABTREE_INSERT_LEAF;
			}
			else if (postingoff != 0)
			{
				xl_abtree_minimal_agg_info	min_agg_info;

				/*
				 * Leaf insert with posting list split.  Must include
				 * postingoff field before newitem/orignewitem.
				 */
				Assert(isleaf);
				xlinfo = XLOG_ABTREE_INSERT_POST;

				if (agg_info->leaf_has_agg)
				{
					/* 
					 * Need to store the minimal agg_info for
					 * _abt_swap_posting()
					 */
					_abt_prepare_min_agg_info(&min_agg_info, agg_info);
					XLogRegisterData((char *) &min_agg_info,
									 SizeOfABtreeMinimalAggInfo);
				}
			}
			else
			{
				/* Internal page insert, which finishes a split on cbuf */
				xlinfo = XLOG_ABTREE_INSERT_UPPER;
				XLogRegisterBuffer(1, cbuf, REGBUF_STANDARD);

				if (BufferIsValid(metabuf))
				{
					ABTAggSupport agg_support;
					
					agg_support = ABTMetaPageGetAggSupport(metad);

					/* Actually, it's an internal page insert + meta update */
					xlinfo = XLOG_ABTREE_INSERT_META;

					Assert(metad->abtm_version >= ABTREE_NOVAC_VERSION);
					xlmeta.version = metad->abtm_version;
					xlmeta.root = metad->abtm_root;
					xlmeta.level = metad->abtm_level;
					xlmeta.fastroot = metad->abtm_fastroot;
					xlmeta.fastlevel = metad->abtm_fastlevel;
					xlmeta.oldest_abtpo_xact = metad->abtm_oldest_abtpo_xact;
					xlmeta.last_cleanup_num_heap_tuples =
						metad->abtm_last_cleanup_num_heap_tuples;
					xlmeta.allequalimage = metad->abtm_allequalimage;

					XLogRegisterBuffer(2, metabuf,
									   REGBUF_WILL_INIT | REGBUF_STANDARD);
					XLogRegisterBufData(2, (char *) &xlmeta,
										sizeof(xl_abtree_metadata));
					XLogRegisterBufData(2, (char *) agg_support,
						ABTASGetStructSize(agg_support));
				}
			}

			XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
			if (postingoff == 0)
			{
				/* Just log itup from caller */
				XLogRegisterBufData(0, (char *) itup, IndexTupleSize(itup));
			}
			else
			{
				/*
				 * Insert with posting list split (XLOG_ABTREE_INSERT_POST
				 * record) case.
				 *
				 * Log postingoff.  Also log origitup, not itup.  REDO routine
				 * must reconstruct final itup (as well as nposting) using
				 * _abt_swap_posting().
				 */
				upostingoff = postingoff;

				XLogRegisterBufData(0, (char *) &upostingoff, sizeof(uint16));
				XLogRegisterBufData(0, (char *) origitup,
									IndexTupleSize(origitup));
			}

			recptr = XLogInsert(RM_ABTREE_ID, xlinfo);

			if (BufferIsValid(metabuf))
				PageSetLSN(metapg, recptr);
			if (!isleaf)
				PageSetLSN(BufferGetPage(cbuf), recptr);

			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		/* Release subsidiary buffers */
		if (BufferIsValid(metabuf))
			_abt_relbuf(rel, metabuf);
		if (!isleaf)
			_abt_relbuf(rel, cbuf);

		/*
		 * XXX can't set block number cache
		 * 
		 * old comments:
		 * Cache the block number if this is the rightmost leaf page.  Cache
		 * may be used by a future inserter within _abt_search_insert().
		 */
		/*blockcache = InvalidBlockNumber;
		if (isrightmost && isleaf && !ABT_P_ISROOT(lpageop))
			blockcache = BufferGetBlockNumber(buf); */
	
		/* Release buffer for insertion target block */
		_abt_relbuf(rel, buf);

		/*
		 * XXX can't set block number cache 
		 *
		 * old comments:
		 * If we decided to cache the insertion target block before releasing
		 * its buffer lock, then cache it now.  Check the height of the tree
		 * first, though.  We don't go for the optimization with small
		 * indexes.  Defer final check to this point to ensure that we don't
		 * call _abt_getrootheight while holding a buffer lock.
		 */
		/*if (BlockNumberIsValid(blockcache) &&
			_abt_getrootheight(rel) >= ABTREE_FASTPATH_MIN_LEVEL)
			RelationSetTargetBlock(rel, blockcache); */
	}

	/* be tidy */
	if (postingoff != 0)
	{
		/* itup is actually a modified copy of caller's original */
		pfree(nposting);
		pfree(itup);
	}
}

/*
 *	_abt_split() -- split a page in the btree.
 *
 *		On entry, buf is the page to split, and is pinned and write-locked.
 *		newitemoff etc. tell us about the new item that must be inserted
 *		along with the data from the original page.
 *
 *		itup_key is used for suffix truncation on leaf pages (internal
 *		page callers pass NULL).  When splitting a non-leaf page, 'cbuf'
 *		is the left-sibling of the page we're inserting the downlink for.
 *		This function will clear the INCOMPLETE_SPLIT flag on it, and
 *		release the buffer.
 *
 *		orignewitem, nposting, and postingoff are needed when an insert of
 *		orignewitem results in both a posting list split and a page split.
 *		These extra posting list split details are used here in the same
 *		way as they are used in the more common case where a posting list
 *		split does not coincide with a page split.  We need to deal with
 *		posting list splits directly in order to ensure that everything
 *		that follows from the insert of orignewitem is handled as a single
 *		atomic operation (though caller's insert of a new pivot/downlink
 *		into parent page will still be a separate operation).  See
 *		nbtree/README for details on the design of posting list splits.
 *		
 *		Returns the new right sibling of buf, pinned and write-locked.
 *		The pin and lock on buf are maintained.
 */
static Buffer
_abt_split(Relation rel, ABTScanInsert itup_key, Buffer buf, Buffer cbuf,
		  OffsetNumber newitemoff, Size newitemsz, IndexTuple newitem,
		  IndexTuple orignewitem, IndexTuple nposting, uint16 postingoff,
		  ABTCachedAggInfo agg_info, Datum previtem_newagg)
{
	Buffer		rbuf;
	Page		origpage;
	Page		leftpage,
				rightpage;
	BlockNumber origpagenumber,
				rightpagenumber;
	ABTPageOpaque ropaque,
				lopaque,
				oopaque;
	Buffer		sbuf = InvalidBuffer;
	Page		spage = NULL;
	ABTPageOpaque sopaque = NULL;
	Size		itemsz;
	ItemId		itemid;
	IndexTuple	firstright,
				lefthighkey;
	OffsetNumber firstrightoff;
	OffsetNumber afterleftoff,
				afterrightoff,
				minusinfoff;
	OffsetNumber origpagepostingoff;
	OffsetNumber maxoff;
	OffsetNumber i;
	bool		newitemonleft,
				isleaf,
				isrightmost;
	OffsetNumber previtem_off = 0; /* suppress compiler warning */

	/*
	 * origpage is the original page to be split.  leftpage is a temporary
	 * buffer that receives the left-sibling data, which will be copied back
	 * into origpage on success.  rightpage is the new page that will receive
	 * the right-sibling data.
	 *
	 * leftpage is allocated after choosing a split point.  rightpage's new
	 * buffer isn't acquired until after leftpage is initialized and has new
	 * high key, the last point where splitting the page may fail (barring
	 * corruption).  Failing before acquiring new buffer won't have lasting
	 * consequences, since origpage won't have been modified and leftpage is
	 * only workspace.
	 */
	origpage = BufferGetPage(buf);
	oopaque = (ABTPageOpaque) PageGetSpecialPointer(origpage);
	isleaf = ABT_P_ISLEAF(oopaque);
	isrightmost = ABT_P_RIGHTMOST(oopaque);
	maxoff = PageGetMaxOffsetNumber(origpage);
	origpagenumber = BufferGetBlockNumber(buf);
	
	/* Also need to set the new agg value of the split item. Find its offset. */
	if (!isleaf)
	{
		previtem_off = OffsetNumberPrev(newitemoff);
	}

	/*
	 * Choose a point to split origpage at.
	 *
	 * A split point can be thought of as a point _between_ two existing data
	 * items on origpage (the lastleft and firstright tuples), provided you
	 * pretend that the new item that didn't fit is already on origpage.
	 *
	 * Since origpage does not actually contain newitem, the representation of
	 * split points needs to work with two boundary cases: splits where
	 * newitem is lastleft, and splits where newitem is firstright.
	 * newitemonleft resolves the ambiguity that would otherwise exist when
	 * newitemoff == firstrightoff.  In all other cases it's clear which side
	 * of the split every tuple goes on from context.  newitemonleft is
	 * usually (but not always) redundant information.
	 *
	 * firstrightoff is supposed to be an origpage offset number, but it's
	 * possible that its value will be maxoff+1, which is "past the end" of
	 * origpage.  This happens in the rare case where newitem goes after all
	 * existing items (i.e. newitemoff is maxoff+1) and we end up splitting
	 * origpage at the point that leaves newitem alone on new right page.  Any
	 * "!newitemonleft && newitemoff == firstrightoff" split point makes
	 * newitem the firstright tuple, though, so this case isn't a special
	 * case.
	 */
	firstrightoff = _abt_findsplitloc(rel, origpage, newitemoff, newitemsz,
									 newitem, &newitemonleft, agg_info);
	
	leftpage = PageGetTempPage(origpage);
	_abt_pageinit(leftpage, BufferGetPageSize(buf));
	lopaque = (ABTPageOpaque) PageGetSpecialPointer(leftpage);

	/*
	 * leftpage won't be the root when we're done.  Also, clear the SPLIT_END
	 * and HAS_GARBAGE flags. Also the ABTP_HAD_GARBAGE_REMOVED flag as its
	 * aggregate value will be recomputed when finishing the split.
	 */
	lopaque->abtpo_flags = oopaque->abtpo_flags;
	lopaque->abtpo_flags &= ~(ABTP_ROOT | ABTP_SPLIT_END | ABTP_HAS_GARBAGE |
							  ABTP_HAD_GARBAGE_REMOVED);
	/* set flag in leftpage indicating that rightpage has no downlink yet */
	lopaque->abtpo_flags |= ABTP_INCOMPLETE_SPLIT;
	lopaque->abtpo_prev = oopaque->abtpo_prev;
	/* handle abtpo_next after rightpage buffer acquired */
	lopaque->abtpo.level = oopaque->abtpo.level;
	/* handle abtpo_cycleid after rightpage buffer acquired */
	if (!isleaf)
		lopaque->abtpo_last_update_id =
			ABTAdvanceLastUpdateId(lopaque->abtpo_last_update_id);

	/*
	 * Copy the original page's LSN into leftpage, which will become the
	 * updated version of the page.  We need this because XLogInsert will
	 * examine the LSN and possibly dump it in a page image.
	 */
	PageSetLSN(leftpage, PageGetLSN(origpage));

	/*
	 * Determine page offset number of existing overlapped-with-orignewitem
	 * posting list when it is necessary to perform a posting list split in
	 * passing.  Note that newitem was already changed by caller (newitem no
	 * longer has the orignewitem TID).
	 *
	 * This page offset number (origpagepostingoff) will be used to pretend
	 * that the posting split has already taken place, even though the
	 * required modifications to origpage won't occur until we reach the
	 * critical section.  The lastleft and firstright tuples of our page split
	 * point should, in effect, come from an imaginary version of origpage
	 * that has the nposting tuple instead of the original posting list tuple.
	 *
	 * Note: _abt_findsplitloc() should have compensated for coinciding posting
	 * list splits in just the same way, at least in theory.  It doesn't
	 * bother with that, though.  In practice it won't affect its choice of
	 * split point.
	 */
	origpagepostingoff = InvalidOffsetNumber;
	if (postingoff != 0)
	{
		Assert(isleaf);
		Assert(ItemPointerCompare(&orignewitem->t_tid,
								  &newitem->t_tid) < 0);
		Assert(ABTreeTupleIsPosting(nposting));
		origpagepostingoff = OffsetNumberPrev(newitemoff);
	}

	/*
	 * The high key for the new left page is a possibly-truncated copy of
	 * firstright on the leaf level (it's "firstright itself" on internal
	 * pages; see !isleaf comments below).  This may seem to be contrary to
	 * Lehman & Yao's approach of using a copy of lastleft as the new high key
	 * when splitting on the leaf level.  It isn't, though.
	 *
	 * Suffix truncation will leave the left page's high key fully equal to
	 * lastleft when lastleft and firstright are equal prior to heap TID (that
	 * is, the tiebreaker TID value comes from lastleft).  It isn't actually
	 * necessary for a new leaf high key to be a copy of lastleft for the L&Y
	 * "subtree" invariant to hold.  It's sufficient to make sure that the new
	 * leaf high key is strictly less than firstright, and greater than or
	 * equal to (not necessarily equal to) lastleft.  In other words, when
	 * suffix truncation isn't possible during a leaf page split, we take
	 * L&Y's exact approach to generating a new high key for the left page.
	 * (Actually, that is slightly inaccurate.  We don't just use a copy of
	 * lastleft.  A tuple with all the keys from firstright but the max heap
	 * TID from lastleft is used, to avoid introducing a special case.)
	 */
	if (!newitemonleft && newitemoff == firstrightoff)
	{
		/* incoming tuple becomes firstright */
		itemsz = newitemsz;
		firstright = newitem;
	}
	else
	{
		/* existing item at firstrightoff becomes firstright */
		itemid = PageGetItemId(origpage, firstrightoff);
		itemsz = ItemIdGetLength(itemid);
		firstright = (IndexTuple) PageGetItem(origpage, itemid);
		if (firstrightoff == origpagepostingoff)
			firstright = nposting;
	}

	if (isleaf)
	{
		IndexTuple	lastleft;

		/* Attempt suffix truncation for leaf page splits */
		if (newitemonleft && newitemoff == firstrightoff)
		{
			/* incoming tuple becomes lastleft */
			lastleft = newitem;
		}
		else
		{
			OffsetNumber lastleftoff;

			/* existing item before firstrightoff becomes lastleft */
			lastleftoff = OffsetNumberPrev(firstrightoff);
			Assert(lastleftoff >= ABT_P_FIRSTDATAKEY(oopaque));
			itemid = PageGetItemId(origpage, lastleftoff);
			lastleft = (IndexTuple) PageGetItem(origpage, itemid);
			if (lastleftoff == origpagepostingoff)
				lastleft = nposting;
		}
	
		/* 
		 * _abt_truncate() will also truncate away the aggregation value and
		 * xmin from the high key. 
		 */
		lefthighkey = _abt_truncate(rel, lastleft, firstright, itup_key,
									agg_info);
		itemsz = IndexTupleSize(lefthighkey);
	}
	else
	{
		/*
		 * Don't perform suffix truncation on a copy of firstright to make
		 * left page high key for internal page splits.  Must use firstright
		 * as new high key directly.
		 *
		 * Each distinct separator key value originates as a leaf level high
		 * key; all other separator keys/pivot tuples are copied from one
		 * level down.  A separator key in a grandparent page must be
		 * identical to high key in rightmost parent page of the subtree to
		 * its left, which must itself be identical to high key in rightmost
		 * child page of that same subtree (this even applies to separator
		 * from grandparent's high key).  There must always be an unbroken
		 * "seam" of identical separator keys that guide index scans at every
		 * level, starting from the grandparent.  That's why suffix truncation
		 * is unsafe here.
		 *
		 * Internal page splits will truncate firstright into a "negative
		 * infinity" data item when it gets inserted on the new right page
		 * below, though.  This happens during the call to _abt_pgaddtup() for
		 * the new first data item for right page.  Do not confuse this
		 * mechanism with suffix truncation.  It is just a convenient way of
		 * implementing page splits that split the internal page "inside"
		 * firstright.  The lefthighkey separator key cannot appear a second
		 * time in the right page (only firstright's downlink goes in right
		 * page).
		 *
		 * However, we still want to purge the space of the aggregation value
		 * in the high key.
		 */
		lefthighkey = _abt_pivot_tuple_purge_agg_space(firstright, agg_info);
		itemsz = IndexTupleSize(lefthighkey);
	}

	/*
	 * Add new high key to leftpage
	 */
	afterleftoff = ABTP_HIKEY;

	Assert(ABTreeTupleGetNAtts(lefthighkey, rel) > 0);
	Assert(ABTreeTupleGetNAtts(lefthighkey, rel) <=
		   IndexRelationGetNumberOfKeyAttributes(rel));
	Assert(itemsz == MAXALIGN(IndexTupleSize(lefthighkey)));
	if (PageAddItem(leftpage, (Item) lefthighkey, itemsz, afterleftoff, false,
					false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add high key to the left sibling"
			 " while splitting block %u of index \"%s\"",
			 origpagenumber, RelationGetRelationName(rel));
	afterleftoff = OffsetNumberNext(afterleftoff);

	/*
	 * Acquire a new right page to split into, now that left page has a new
	 * high key.  From here on, it's not okay to throw an error without
	 * zeroing rightpage first.  This coding rule ensures that we won't
	 * confuse future VACUUM operations, which might otherwise try to re-find
	 * a downlink to a leftover junk page as the page undergoes deletion.
	 *
	 * It would be reasonable to start the critical section just after the new
	 * rightpage buffer is acquired instead; that would allow us to avoid
	 * leftover junk pages without bothering to zero rightpage.  We do it this
	 * way because it avoids an unnecessary PANIC when either origpage or its
	 * existing sibling page are corrupt.
	 */
	rbuf = _abt_getbuf(rel, P_NEW, ABT_WRITE);
	rightpage = BufferGetPage(rbuf);
	rightpagenumber = BufferGetBlockNumber(rbuf);
	/* rightpage was initialized by _abt_getbuf */
	ropaque = (ABTPageOpaque) PageGetSpecialPointer(rightpage);

	/*
	 * Finish off remaining leftpage special area fields.  They cannot be set
	 * before both origpage (leftpage) and rightpage buffers are acquired and
	 * locked.
	 *
	 * abtpo_cycleid is only used with leaf pages, though we set it here in all
	 * cases just to be consistent.
	 */
	lopaque->abtpo_next = rightpagenumber;
	lopaque->abtpo_cycleid = _abt_vacuum_cycleid(rel);

	/*
	 * rightpage won't be the root when we're done.  Also, clear the SPLIT_END
	 * and HAS_GARBAGE flags. Also remove the ABTP_HAD_GARBAGE_REMOVED, as the
	 * aggregate of the right page will be recomputed when we insert its
	 * downlink into the parent.
	 */
	ropaque->abtpo_flags = oopaque->abtpo_flags;
	ropaque->abtpo_flags &= ~(ABTP_ROOT | ABTP_SPLIT_END | ABTP_HAS_GARBAGE |
							  ABTP_HAD_GARBAGE_REMOVED);
	ropaque->abtpo_prev = origpagenumber;
	ropaque->abtpo_next = oopaque->abtpo_next;
	ropaque->abtpo.level = oopaque->abtpo.level;
	ropaque->abtpo_cycleid = lopaque->abtpo_cycleid;
	if (!isleaf)
		ropaque->abtpo_last_update_id = ABT_MIN_LAST_UPDATE_ID;

	/*
	 * Add new high key to rightpage where necessary.
	 *
	 * If the page we're splitting is not the rightmost page at its level in
	 * the tree, then the first entry on the page is the high key from
	 * origpage.
	 */
	afterrightoff = ABTP_HIKEY;

	if (!isrightmost)
	{
		IndexTuple	righthighkey;

		itemid = PageGetItemId(origpage, ABTP_HIKEY);
		itemsz = ItemIdGetLength(itemid);
		righthighkey = (IndexTuple) PageGetItem(origpage, itemid);
		Assert(ABTreeTupleGetNAtts(righthighkey, rel) > 0);
		Assert(ABTreeTupleGetNAtts(righthighkey, rel) <=
			   IndexRelationGetNumberOfKeyAttributes(rel));
		if (PageAddItem(rightpage, (Item) righthighkey, itemsz, afterrightoff,
						false, false) == InvalidOffsetNumber)
		{
			memset(rightpage, 0, BufferGetPageSize(rbuf));
			elog(ERROR, "failed to add high key to the right sibling"
				 " while splitting block %u of index \"%s\"",
				 origpagenumber, RelationGetRelationName(rel));
		}
		afterrightoff = OffsetNumberNext(afterrightoff);
	}

	/*
	 * Internal page splits truncate first data item on right page -- it
	 * becomes "minus infinity" item for the page.  Set this up here.
	 */
	minusinfoff = InvalidOffsetNumber;
	if (!isleaf)
		minusinfoff = afterrightoff;

	/*
	 * Now transfer all the data items (non-pivot tuples in isleaf case, or
	 * additional pivot tuples in !isleaf case) to the appropriate page.
	 *
	 * Note: we *must* insert at least the right page's items in item-number
	 * order, for the benefit of _abt_restore_page().
	 */
	for (i = ABT_P_FIRSTDATAKEY(oopaque); i <= maxoff; i = OffsetNumberNext(i))
	{
		IndexTuple	dataitem;

		itemid = PageGetItemId(origpage, i);
		itemsz = ItemIdGetLength(itemid);
		dataitem = (IndexTuple) PageGetItem(origpage, itemid);

		/* replace original item with nposting due to posting split? */
		if (i == origpagepostingoff)
		{
			Assert(ABTreeTupleIsPosting(dataitem));
			Assert(itemsz == MAXALIGN(IndexTupleSize(nposting)));
			dataitem = nposting;
		}

		/* does new item belong before this one? */
		else if (i == newitemoff)
		{
			if (newitemonleft)
			{
				Assert(newitemoff <= firstrightoff);
				if (!_abt_pgaddtup(leftpage, newitemsz, newitem, afterleftoff,
								  false, agg_info))
				{
					memset(rightpage, 0, BufferGetPageSize(rbuf));
					elog(ERROR, "failed to add new item to the left sibling"
						 " while splitting block %u of index \"%s\"",
						 origpagenumber, RelationGetRelationName(rel));
				}
				afterleftoff = OffsetNumberNext(afterleftoff);
			}
			else
			{
				Assert(newitemoff >= firstrightoff);
				if (!_abt_pgaddtup(rightpage, newitemsz, newitem, afterrightoff,
								  afterrightoff == minusinfoff, agg_info))
				{
					memset(rightpage, 0, BufferGetPageSize(rbuf));
					elog(ERROR, "failed to add new item to the right sibling"
						 " while splitting block %u of index \"%s\"",
						 origpagenumber, RelationGetRelationName(rel));
				}
				afterrightoff = OffsetNumberNext(afterrightoff);
			}
		}
	
		/* Set the new aggregation value for the item that points to cbuf. */
		if (!isleaf && i == previtem_off)
		{
			/* make sure we are setting the correct pivot tuple. */
			Assert(BufferIsValid(cbuf) &&
				ABTreeTupleGetDownLink(dataitem) == BufferGetBlockNumber(cbuf));
			/* 
			 * The old page is torn down anyway, so we can probably directly
			 * set the agg in the dataitem.
			 */
			_abt_set_pivot_tuple_aggregation_value(dataitem,
												   previtem_newagg,
												   agg_info);
		}

		/* decide which page to put it on */
		if (i < firstrightoff)
		{
			if (!_abt_pgaddtup(leftpage, itemsz, dataitem, afterleftoff, false,
								agg_info))
			{
				memset(rightpage, 0, BufferGetPageSize(rbuf));
				elog(ERROR, "failed to add old item to the left sibling"
					 " while splitting block %u of index \"%s\"",
					 origpagenumber, RelationGetRelationName(rel));
			}
			afterleftoff = OffsetNumberNext(afterleftoff);
		}
		else
		{
			if (!_abt_pgaddtup(rightpage, itemsz, dataitem, afterrightoff,
							  afterrightoff == minusinfoff, agg_info))
			{
				memset(rightpage, 0, BufferGetPageSize(rbuf));
				elog(ERROR, "failed to add old item to the right sibling"
					 " while splitting block %u of index \"%s\"",
					 origpagenumber, RelationGetRelationName(rel));
			}
			afterrightoff = OffsetNumberNext(afterrightoff);
		}
	}

	/* Handle case where newitem goes at the end of rightpage */
	if (i <= newitemoff)
	{
		/*
		 * Can't have newitemonleft here; that would imply we were told to put
		 * *everything* on the left page, which cannot fit (if it could, we'd
		 * not be splitting the page).
		 */
		Assert(!newitemonleft && newitemoff == maxoff + 1);
		if (!_abt_pgaddtup(rightpage, newitemsz, newitem, afterrightoff,
						  afterrightoff == minusinfoff, agg_info))
		{
			memset(rightpage, 0, BufferGetPageSize(rbuf));
			elog(ERROR, "failed to add new item to the right sibling"
				 " while splitting block %u of index \"%s\"",
				 origpagenumber, RelationGetRelationName(rel));
		}
		afterrightoff = OffsetNumberNext(afterrightoff);
	}

	/*
	 * We have to grab the right sibling (if any) and fix the prev pointer
	 * there. We are guaranteed that this is deadlock-free since no other
	 * writer will be holding a lock on that page and trying to move left, and
	 * all readers release locks on a page before trying to fetch its
	 * neighbors.
	 */
	if (!isrightmost)
	{
		sbuf = _abt_getbuf(rel, oopaque->abtpo_next, ABT_WRITE);
		spage = BufferGetPage(sbuf);
		sopaque = (ABTPageOpaque) PageGetSpecialPointer(spage);
		if (sopaque->abtpo_prev != origpagenumber)
		{
			memset(rightpage, 0, BufferGetPageSize(rbuf));
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg_internal("right sibling's left-link doesn't match: "
									 "block %u links to %u instead of expected %u in index \"%s\"",
									 oopaque->abtpo_next, sopaque->abtpo_prev, origpagenumber,
									 RelationGetRelationName(rel))));
		}

		/*
		 * Check to see if we can set the SPLIT_END flag in the right-hand
		 * split page; this can save some I/O for vacuum since it need not
		 * proceed to the right sibling.  We can set the flag if the right
		 * sibling has a different cycleid: that means it could not be part of
		 * a group of pages that were all split off from the same ancestor
		 * page.  If you're confused, imagine that page A splits to A B and
		 * then again, yielding A C B, while vacuum is in progress.  Tuples
		 * originally in A could now be in either B or C, hence vacuum must
		 * examine both pages.  But if D, our right sibling, has a different
		 * cycleid then it could not contain any tuples that were in A when
		 * the vacuum started.
		 */
		if (sopaque->abtpo_cycleid != ropaque->abtpo_cycleid)
			ropaque->abtpo_flags |= ABTP_SPLIT_END;
	}

	/*
	 * Right sibling is locked, new siblings are prepared, but original page
	 * is not updated yet.
	 *
	 * NO EREPORT(ERROR) till right sibling is updated.  We can get away with
	 * not starting the critical section till here because we haven't been
	 * scribbling on the original page yet; see comments above.
	 */
	START_CRIT_SECTION();

	/*
	 * By here, the original data page has been split into two new halves, and
	 * these are correct.  The algorithm requires that the left page never
	 * move during a split, so we copy the new left page back on top of the
	 * original.  We need to do this before writing the WAL record, so that
	 * XLogInsert can WAL log an image of the page if necessary.
	 */
	PageRestoreTempPage(leftpage, origpage);
	/* leftpage, lopaque must not be used below here */

	MarkBufferDirty(buf);
	MarkBufferDirty(rbuf);

	if (!isrightmost)
	{
		sopaque->abtpo_prev = rightpagenumber;
		MarkBufferDirty(sbuf);
	}

	/*
	 * Clear INCOMPLETE_SPLIT flag on child if inserting the new item finishes
	 * a split
	 */
	if (!isleaf)
	{
		Page		cpage = BufferGetPage(cbuf);
		ABTPageOpaque cpageop = (ABTPageOpaque) PageGetSpecialPointer(cpage);

		cpageop->abtpo_flags &= ~ABTP_INCOMPLETE_SPLIT;
		MarkBufferDirty(cbuf);
	}

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_abtree_split xlrec;
		uint8		xlinfo;
		XLogRecPtr	recptr;

		xlrec.level = ropaque->abtpo.level;
		/* See comments below on newitem, orignewitem, and posting lists */
		xlrec.firstrightoff = firstrightoff;
		xlrec.newitemoff = newitemoff;
		xlrec.postingoff = 0;
		if (postingoff != 0 && origpagepostingoff < firstrightoff)
			xlrec.postingoff = postingoff;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfABtreeSplit);
		
		if ((isleaf && agg_info->leaf_has_agg && postingoff != 0) ||
			(!isleaf && previtem_off < firstrightoff))
		{
			/* 
			 * Need to store the minimal agg_info for _abt_swap_posting() or
			 * reading the stored sum of page aggregation values for the new
			 * pivot tuple.
			 */
			xl_abtree_minimal_agg_info	min_agg_info;
			_abt_prepare_min_agg_info(&min_agg_info, agg_info);
			XLogRegisterData((char *) &min_agg_info,
							 SizeOfABtreeMinimalAggInfo);
		}

		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterBuffer(1, rbuf, REGBUF_WILL_INIT);
		/* Log original right sibling, since we've changed its prev-pointer */
		if (!isrightmost)
			XLogRegisterBuffer(2, sbuf, REGBUF_STANDARD);
		if (!isleaf)
			XLogRegisterBuffer(3, cbuf, REGBUF_STANDARD);

		/*
		 * Log the new item, if it was inserted on the left page. (If it was
		 * put on the right page, we don't need to explicitly WAL log it
		 * because it's included with all the other items on the right page.)
		 * Show the new item as belonging to the left page buffer, so that it
		 * is not stored if XLogInsert decides it needs a full-page image of
		 * the left page.  We always store newitemoff in the record, though.
		 *
		 * The details are sometimes slightly different for page splits that
		 * coincide with a posting list split.  If both the replacement
		 * posting list and newitem go on the right page, then we don't need
		 * to log anything extra, just like the simple !newitemonleft
		 * no-posting-split case (postingoff is set to zero in the WAL record,
		 * so recovery doesn't need to process a posting list split at all).
		 * Otherwise, we set postingoff and log orignewitem instead of
		 * newitem, despite having actually inserted newitem.  REDO routine
		 * must reconstruct nposting and newitem using _abt_swap_posting().
		 *
		 * Note: It's possible that our page split point is the point that
		 * makes the posting list lastleft and newitem firstright.  This is
		 * the only case where we log orignewitem/newitem despite newitem
		 * going on the right page.  If XLogInsert decides that it can omit
		 * orignewitem due to logging a full-page image of the left page,
		 * everything still works out, since recovery only needs to log
		 * orignewitem for items on the left page (just like the regular
		 * newitem-logged case).
		 */
		if (newitemonleft && xlrec.postingoff == 0)
			XLogRegisterBufData(0, (char *) newitem, newitemsz);
		else if (xlrec.postingoff != 0)
		{
			Assert(isleaf);
			Assert(newitemonleft || firstrightoff == newitemoff);
			Assert(newitemsz == IndexTupleSize(orignewitem));
			XLogRegisterBufData(0, (char *) orignewitem, newitemsz);
		}

		/* Log the left page's new high key */
		XLogRegisterBufData(0, (char *) lefthighkey,
							MAXALIGN(IndexTupleSize(lefthighkey)));
	
		/* 
		 * Record the update to the aggregation value of the pivot tuple
		 * that links to the split page.
		 */
		if (!isleaf && previtem_off < firstrightoff)
		{
			ItemId		ii;
			IndexTuple	itup;

			ii = PageGetItemId(origpage, previtem_off);
			itup = (IndexTuple) PageGetItem(origpage, ii);

			Assert(xlrec.postingoff == 0);
			XLogRegisterBufData(0, ABTreePivotTupleGetAggPtr(itup, agg_info),
								agg_info->agg_typlen);
		}

		/*
		 * Log the contents of the right page in the format understood by
		 * _abt_restore_page().  The whole right page will be recreated.
		 *
		 * Direct access to page is not good but faster - we should implement
		 * some new func in page API.  Note we only store the tuples
		 * themselves, knowing that they were inserted in item-number order
		 * and so the line pointers can be reconstructed.  See comments for
		 * _abt_restore_page().
		 */
		XLogRegisterBufData(1,
							(char *) rightpage + ((PageHeader) rightpage)->pd_upper,
							((PageHeader) rightpage)->pd_special - ((PageHeader) rightpage)->pd_upper);

		xlinfo = newitemonleft ? XLOG_ABTREE_SPLIT_L : XLOG_ABTREE_SPLIT_R;
		recptr = XLogInsert(RM_ABTREE_ID, xlinfo);

		PageSetLSN(origpage, recptr);
		PageSetLSN(rightpage, recptr);
		if (!isrightmost)
			PageSetLSN(spage, recptr);
		if (!isleaf)
			PageSetLSN(BufferGetPage(cbuf), recptr);
	}

	END_CRIT_SECTION();

	/* release the old right sibling */
	if (!isrightmost)
		_abt_relbuf(rel, sbuf);

	/* release the child */
	if (!isleaf)
		_abt_relbuf(rel, cbuf);

	/* be tidy */
	pfree(lefthighkey);

	/* split's done */
	return rbuf;
}

/*
 * _abt_insert_parent() -- Insert downlink into parent, completing split.
 *
 * On entry, buf and rbuf are the left and right split pages, which we
 * still hold write locks on. 
 *
 * If is_leaf == true, we expect orignewitem to be the original new item
 * inserted into the tree, and we will exclude its aggregation value from
 * the page sum of the aggregation value.
 *
 * Returns a new stack pointer if a new root is created or some page above
 * the original fast root level was updated. That is the new starting point
 * for fixing the aggregations.
 *
 * XXX Instead of the following, see above:
 * On entry, buf and rbuf are the left and right split pages, which we
 * still hold write locks on.  Both locks will be released here.  We
 * release the rbuf lock once we have a write lock on the page that we
 * intend to insert a downlink to rbuf on (i.e. buf's current parent page).
 * The lock on buf is released at the same point as the lock on the parent
 * page, since buf's INCOMPLETE_SPLIT flag must be cleared by the same
 * atomic operation that completes the split by inserting a new downlink.
 *
 * stack - stack showing how we got here.  Will be NULL when splitting true
 *			root, or during concurrent root split, where we can be inefficient
 * is_root - we split the true root
 * is_only - we split a page alone on its level (might have been fast root)
 */
static ABTStack
_abt_insert_parent(Relation rel,
				  Buffer buf,
				  Buffer rbuf,
				  ABTStack stack,
				  bool is_root,
				  bool is_only,
				  ABTCachedAggInfo agg_info /*,
				  bool is_leaf,
				  bool leafitem_in_left_subtree,
				  IndexTuple orignewitem */)
{
	Datum		lagg,
				ragg;
	uint64		lopaque_chain,
				ropaque_chain;
	ABTStack	return_stack = NULL;
	
	lagg = _abt_compute_page_aggregate_and_version_chain(
				rel, buf, &lopaque_chain, agg_info);
	ragg = _abt_compute_page_aggregate_and_version_chain(
				rel, rbuf, &ropaque_chain, agg_info);

	/*
	 * Here we have to do something Lehman and Yao don't talk about: deal with
	 * a root split and construction of a new root.  If our stack is empty
	 * then we have just split a node on what had been the root level when we
	 * descended the tree.  If it was still the root then we perform a
	 * new-root construction.  If it *wasn't* the root anymore, search to find
	 * the next higher level that someone constructed meanwhile, and find the
	 * right place to insert as for the normal case.
	 *
	 * If we have to search for the parent level, we do so by re-descending
	 * from the root.  This is not super-efficient, but it's rare enough not
	 * to matter.
	 */
	if (is_root)
	{
		Buffer		rootbuf;
		BlockNumber bknum = BufferGetBlockNumber(buf);
		BlockNumber rbknum = BufferGetBlockNumber(rbuf);

		Assert(stack == NULL);
		Assert(is_only);
		/* create a new root node and update the metapage */
		rootbuf = _abt_newroot(rel, buf, rbuf, agg_info, lagg, ragg);
	
		/* we can install the version chains now. */
		_abt_install_version_chain(rel, bknum, lopaque_chain);
		_abt_install_version_chain(rel, rbknum, ropaque_chain);

		/* 
		 * Create a new stack frame for the new root page. Usually we can just
		 * start from the new root in order to update aggregates 
		 *
		 * Setting abts_last_update_id to invalid makes sure we'll search for
		 * the correct down link to add the new item's value to instead of
		 * blindly trust the abts_offset.
		 */
		return_stack = (ABTStack) palloc(sizeof(ABTStackData));
		return_stack->abts_blkno = BufferGetBlockNumber(rootbuf);
		return_stack->abts_offset = ABTP_HIKEY; /* points to left */
		return_stack->abts_last_update_id = ABT_INVALID_LAST_UPDATE_ID;
		return_stack->abts_parent = NULL;
	
		/* release the split buffers */
		_abt_relbuf(rel, rootbuf);
		_abt_relbuf(rel, rbuf);
		_abt_relbuf(rel, buf);
	}
	else
	{
		BlockNumber bknum = BufferGetBlockNumber(buf);
		BlockNumber rbknum = BufferGetBlockNumber(rbuf);
		Page		page = BufferGetPage(buf);
		IndexTuple	new_item;
		IndexTuple	ritem;
		Buffer		pbuf;
		ABTLastUpdateId	*last_update_id_ptr;

		if (stack == NULL)
		{
			ABTPageOpaque lpageop;
			
			elog(DEBUG2, "concurrent ROOT page split");
			lpageop = (ABTPageOpaque) PageGetSpecialPointer(page);

			/*
			 * We should never reach here when a leaf page split takes place
			 * despite the insert of newitem being able to apply the fastpath
			 * optimization.  Make sure of that with an assertion.
			 *
			 * This is more of a performance issue than a correctness issue.
			 * The fastpath won't have a descent stack.  Using a phony stack
			 * here works, but never rely on that.  The fastpath should be
			 * rejected within _abt_search_insert() when the rightmost leaf
			 * page will split, since it's faster to go through _abt_search()
			 * and get a stack in the usual way.
			 */
			Assert(!(ABT_P_ISLEAF(lpageop) &&
					 BlockNumberIsValid(RelationGetTargetBlock(rel))));

			/* Find the leftmost page at the next level up */
			pbuf = _abt_get_endpoint(rel, lpageop->abtpo.level + 1, false,
									NULL);
			/* Set up a phony stack entry pointing there */

			return_stack = palloc(sizeof(ABTStackData));
			stack = return_stack;
			stack->abts_blkno = BufferGetBlockNumber(pbuf);
			stack->abts_offset = InvalidOffsetNumber;
			stack->abts_last_update_id = ABT_INVALID_LAST_UPDATE_ID;
			stack->abts_parent = NULL;
			_abt_relbuf(rel, pbuf);
		}

		/* get high key from left, a strict lower bound for new right page */
		ritem = (IndexTuple) PageGetItem(page,
										 PageGetItemId(page, ABTP_HIKEY));

		/* 
		 * form an index tuple that points at the new right page, and set
		 * the sum of the aggregation values of the right page.
		 */
		new_item = _abt_pivot_tuple_reserve_agg_space(ritem, agg_info);
		ABTreeTupleSetDownLink(new_item, rbknum);
		_abt_set_pivot_tuple_aggregation_value(new_item, ragg, agg_info);

		/* 
		 * initialize the last update id of the tuple. This is a new tuple so
		 * no one care about the initial value of it.
		 */
		last_update_id_ptr = ABTreeTupleGetLastUpdateIdPtr(new_item);
		*last_update_id_ptr = ABT_MIN_LAST_UPDATE_ID;

		/*
		 * Re-find and write lock the parent of buf.
		 *
		 * It's possible that the location of buf's downlink has changed since
		 * our initial _abt_search() descent.  _abt_getstackbuf() will detect
		 * and recover from this, updating the stack, which ensures that the
		 * new downlink will be inserted at the correct offset. Even buf's
		 * parent may have changed.
		 */
		pbuf = _abt_getstackbuf(rel, stack, bknum, agg_info);

		/* 
		 * Unlock the right child. The left child will be unblocked in
		 * _abt_insertonpg().
		 *
		 * Unlocking the right child must be delayed until here to ensure that
		 * no concurrent VACUUM operation can become confused.  Page deletion
		 * cannot be allowed to fail to re-find a downlink for the rbuf page.
		 * (Actually, this is just a vestige of how things used to work.  The
		 * page deletion code is expected to check for the INCOMPLETE_SPLIT
		 * flag on the left child.  It won't attempt deletion of the right
		 * child until the split is complete.  Despite all this, we opt to
		 * conservatively delay unlocking the right child until here.)
		 */
		_abt_relbuf(rel, rbuf);

		_abt_install_version_chain(rel, bknum, lopaque_chain);
		_abt_install_version_chain(rel, rbknum, ropaque_chain);

		if (pbuf == InvalidBuffer)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg_internal("failed to re-find parent key in index \"%s\" for split pages %u/%u",
									 RelationGetRelationName(rel), bknum, rbknum)));
	
		/* Recursively insert into the parent */
		_abt_insertonpg(rel, NULL, pbuf, buf, &stack->abts_parent,
						new_item, MAXALIGN(IndexTupleSize(new_item)),
						stack->abts_offset + 1, 0, is_only, agg_info,
						lagg);

		/* be tidy */
		pfree(new_item);
	}

	if (!agg_info->agg_byval)
	{
		pfree(DatumGetPointer(lagg));
		pfree(DatumGetPointer(ragg));
	}

	return return_stack;
}

/*
 * _abt_finish_split() -- Finish an incomplete split
 *
 * A crash or other failure can leave a split incomplete.  The insertion
 * routines won't allow to insert on a page that is incompletely split.
 * Before inserting on such a page, call _abt_finish_split().
 *
 * On entry, 'lbuf' must be locked in write-mode.  On exit, it is unlocked
 * and unpinned.
 */
void
_abt_finish_split(Relation rel, Buffer lbuf, ABTStack stack,
				  ABTCachedAggInfo agg_info)
{
	Page		lpage = BufferGetPage(lbuf);
	ABTPageOpaque lpageop = (ABTPageOpaque) PageGetSpecialPointer(lpage);
	Buffer		rbuf;
	Page		rpage;
	ABTPageOpaque rpageop;
	bool		was_root;
	bool		was_only;

	Assert(ABT_P_INCOMPLETE_SPLIT(lpageop));

	/* Lock right sibling, the one missing the downlink */
	rbuf = _abt_getbuf(rel, lpageop->abtpo_next, ABT_WRITE);
	rpage = BufferGetPage(rbuf);
	rpageop = (ABTPageOpaque) PageGetSpecialPointer(rpage);

	/* Could this be a root split? */
	if (!stack)
	{
		Buffer		metabuf;
		Page		metapg;
		ABTMetaPageData *metad;

		/* acquire lock on the metapage */
		metabuf = _abt_getbuf(rel, ABTREE_METAPAGE, ABT_WRITE);
		metapg = BufferGetPage(metabuf);
		metad = ABTPageGetMeta(metapg);

		was_root = (metad->abtm_root == BufferGetBlockNumber(lbuf));

		_abt_relbuf(rel, metabuf);
	}
	else
		was_root = false;

	/* Was this the only page on the level before split? */
	was_only = (ABT_P_LEFTMOST(lpageop) && ABT_P_RIGHTMOST(rpageop));

	elog(DEBUG1, "finishing incomplete split of %u/%u",
		 BufferGetBlockNumber(lbuf), BufferGetBlockNumber(rbuf));

	_abt_insert_parent(rel, lbuf, rbuf, stack, was_root, was_only, agg_info);
}

/*
 *	_abt_getstackbuf() -- Walk back up the tree one step, and find the pivot
 *						 tuple whose downlink points to child page.
 *
 *		Caller passes child's block number, which is used to identify
 *		associated pivot tuple in parent page using a linear search that
 *		matches on pivot's downlink/block number.  The expected location of
 *		the pivot tuple is taken from the stack one level above the child
 *		page.  This is used as a starting point.  Insertions into the
 *		parent level could cause the pivot tuple to move right; deletions
 *		could cause it to move left, but not left of the page we previously
 *		found it on.
 *
 *		Caller can use its stack to relocate the pivot tuple/downlink for
 *		any same-level page to the right of the page found by its initial
 *		descent.  This is necessary because of the possibility that caller
 *		moved right to recover from a concurrent page split.  It's also
 *		convenient for certain callers to be able to step right when there
 *		wasn't a concurrent page split, while still using their original
 *		stack.  For example, the checkingunique _abt_doinsert() case may
 *		have to step right when there are many physical duplicates, and its
 *		scantid forces an insertion to the right of the "first page the
 *		value could be on".  (This is also relied on by all of our callers
 *		when dealing with !heapkeyspace indexes.)
 *
 *		Returns write-locked parent page buffer, or InvalidBuffer if pivot
 *		tuple not found (should not happen).  Adjusts abts_blkno &
 *		abts_offset if changed.  Page split caller should insert its new
 *		pivot tuple for its new right sibling page on parent page, at the
 *		offset number abts_offset + 1.
 */
Buffer
_abt_getstackbuf(Relation rel, ABTStack stack, BlockNumber child,
				 ABTCachedAggInfo agg_info)
{
	BlockNumber blkno;
	OffsetNumber start;

	blkno = stack->abts_blkno;
	start = stack->abts_offset;

	for (;;)
	{
		Buffer		buf;
		Page		page;
		ABTPageOpaque opaque;

		buf = _abt_getbuf(rel, blkno, ABT_WRITE);
		page = BufferGetPage(buf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

		if (ABT_P_INCOMPLETE_SPLIT(opaque))
		{
			_abt_finish_split(rel, buf, stack->abts_parent, agg_info);
			continue;
		}

		if (!ABT_P_IGNORE(opaque))
		{
			OffsetNumber offnum,
						minoff,
						maxoff;
			ItemId		itemid;
			IndexTuple	item;

			minoff = ABT_P_FIRSTDATAKEY(opaque);
			maxoff = PageGetMaxOffsetNumber(page);

			/*
			 * start = InvalidOffsetNumber means "search the whole page". We
			 * need this test anyway due to possibility that page has a high
			 * key now when it didn't before.
			 */
			if (start < minoff)
				start = minoff;

			/*
			 * Need this check too, to guard against possibility that page
			 * split since we visited it originally.
			 */
			if (start > maxoff)
				start = OffsetNumberNext(maxoff);

			/*
			 * These loops will check every item on the page --- but in an
			 * order that's attuned to the probability of where it actually
			 * is.  Scan to the right first, then to the left.
			 */
			for (offnum = start;
				 offnum <= maxoff;
				 offnum = OffsetNumberNext(offnum))
			{
				itemid = PageGetItemId(page, offnum);
				item = (IndexTuple) PageGetItem(page, itemid);

				if (ABTreeTupleGetDownLink(item) == child)
				{
					/* Return accurate pointer to where link is now */
					stack->abts_blkno = blkno;
					stack->abts_offset = offnum;
					return buf;
				}
			}

			for (offnum = OffsetNumberPrev(start);
				 offnum >= minoff;
				 offnum = OffsetNumberPrev(offnum))
			{
				itemid = PageGetItemId(page, offnum);
				item = (IndexTuple) PageGetItem(page, itemid);

				if (ABTreeTupleGetDownLink(item) == child)
				{
					/* Return accurate pointer to where link is now */
					stack->abts_blkno = blkno;
					stack->abts_offset = offnum;
					return buf;
				}
			}
		}

		/*
		 * The item we're looking for moved right at least one page.
		 *
		 * Lehman and Yao couple/chain locks when moving right here, which we
		 * can avoid.  See nbtree/README.
		 */
		if (ABT_P_RIGHTMOST(opaque))
		{
			_abt_relbuf(rel, buf);
			return InvalidBuffer;
		}
		blkno = opaque->abtpo_next;
		start = InvalidOffsetNumber;
		_abt_relbuf(rel, buf);
	}
}

/*
 *	_abt_newroot() -- Create a new root page for the index.
 *
 *		We've just split the old root page and need to create a new one.
 *		In order to do this, we add a new root page to the file, then lock
 *		the metadata page and update it.  This is guaranteed to be deadlock-
 *		free, because all readers release their locks on the metadata page
 *		before trying to lock the root, and all writers lock the root before
 *		trying to lock the metadata page.  We have a write lock on the old
 *		root page, so we have not introduced any cycles into the waits-for
 *		graph.
 *
 *		On entry, lbuf (the old root) and rbuf (its new peer) are write-
 *		locked. On exit, a new root page exists with entries for the
 *		two new children, metapage is updated and unlocked/unpinned.
 *		The new root buffer is returned to caller which has to unlock/unpin
 *		lbuf, rbuf & rootbuf.
 */
static Buffer
_abt_newroot(Relation rel, Buffer lbuf, Buffer rbuf,
			 ABTCachedAggInfo agg_info, Datum lagg, Datum ragg)
{
	Buffer		rootbuf;
	Page		lpage,
				rootpage;
	BlockNumber lbkno,
				rbkno;
	BlockNumber rootblknum;
	ABTPageOpaque rootopaque;
	ABTPageOpaque lopaque;
	ItemId		itemid;
	IndexTuple	item;
	IndexTuple	left_item;
	Size		left_item_sz;
	IndexTuple	right_item;
	Size		right_item_sz;
	Buffer		metabuf;
	Page		metapg;
	ABTMetaPageData *metad;

	lbkno = BufferGetBlockNumber(lbuf);
	rbkno = BufferGetBlockNumber(rbuf);
	lpage = BufferGetPage(lbuf);
	lopaque = (ABTPageOpaque) PageGetSpecialPointer(lpage);

	/* get a new root page */
	rootbuf = _abt_getbuf(rel, P_NEW, ABT_WRITE);
	rootpage = BufferGetPage(rootbuf);
	rootblknum = BufferGetBlockNumber(rootbuf);

	/* acquire lock on the metapage */
	metabuf = _abt_getbuf(rel, ABTREE_METAPAGE, ABT_WRITE);
	metapg = BufferGetPage(metabuf);
	metad = ABTPageGetMeta(metapg);

	/*
	 * Create downlink item for left page (old root).  The key value used is
	 * "minus infinity", a sentinel value that's reliably less than any real
	 * key value that could appear in the left page.
	 *
	 */
	left_item_sz = MAXALIGN(sizeof(IndexTupleData))
					+ agg_info->extra_space_when_tid_missing;
	left_item = (IndexTuple) palloc(left_item_sz);
	left_item->t_info = left_item_sz;
	ABTreeTupleSetDownLink(left_item, lbkno);
	ABTreeTupleSetNAtts(left_item, 0, false);
	_abt_set_pivot_tuple_aggregation_value(left_item, lagg, agg_info);
	/* 
	 * Set the last update id. This is a new pivot tuple so no one knows about
	 * it yet. Any valid value would do.
	 */
	*ABTreeTupleGetLastUpdateIdPtr(left_item) = ABT_MIN_LAST_UPDATE_ID;

	/*
	 * Create downlink item for right page.  The key for it is obtained from
	 * the "high key" position in the left page.
	 */
	itemid = PageGetItemId(lpage, ABTP_HIKEY);
	item = (IndexTuple) PageGetItem(lpage, itemid);
	right_item = _abt_pivot_tuple_reserve_agg_space(item, agg_info);
	/* 
	 * get right_item_sz from the tuple as it should at least increase by the
	 * size of agg_type 
	 */
	right_item_sz = IndexTupleSize(right_item);
	ABTreeTupleSetDownLink(right_item, rbkno);
	_abt_set_pivot_tuple_aggregation_value(right_item, ragg, agg_info);
	/* 
	 * Set the last update id. Same as above
	 */
	*ABTreeTupleGetLastUpdateIdPtr(right_item) = ABT_MIN_LAST_UPDATE_ID;

	/* NO EREPORT(ERROR) from here till newroot op is logged */
	START_CRIT_SECTION();

	/* upgrade metapage if needed */
	if (metad->abtm_version < ABTREE_NOVAC_VERSION)
		_abt_upgrademetapage(metapg);

	/* set btree special data */
	rootopaque = (ABTPageOpaque) PageGetSpecialPointer(rootpage);
	rootopaque->abtpo_prev = rootopaque->abtpo_next = ABTP_NONE;
	rootopaque->abtpo_flags = ABTP_ROOT;
	rootopaque->abtpo.level =
		((ABTPageOpaque) PageGetSpecialPointer(lpage))->abtpo.level + 1;
	rootopaque->abtpo_cycleid = 0;
	rootopaque->abtpo_last_update_id = ABT_MIN_LAST_UPDATE_ID;

	/* update metapage data */
	metad->abtm_root = rootblknum;
	metad->abtm_level = rootopaque->abtpo.level;
	metad->abtm_fastroot = rootblknum;
	metad->abtm_fastlevel = rootopaque->abtpo.level;

	/*
	 * Insert the left page pointer into the new root page.  The root page is
	 * the rightmost page on its level so there is no "high key" in it; the
	 * two items will go into positions ABTP_HIKEY and ABTP_FIRSTKEY.
	 *
	 * Note: we *must* insert the two items in item-number order, for the
	 * benefit of _abt_restore_page().
	 */
	Assert(ABTreeTupleGetNAtts(left_item, rel) == 0);
	if (PageAddItem(rootpage, (Item) left_item, left_item_sz, ABTP_HIKEY,
					false, false) == InvalidOffsetNumber)
		elog(PANIC, "failed to add leftkey to new root page"
			 " while splitting block %u of index \"%s\"",
			 BufferGetBlockNumber(lbuf), RelationGetRelationName(rel));

	/*
	 * insert the right page pointer into the new root page.
	 */
	Assert(ABTreeTupleGetNAtts(right_item, rel) > 0);
	Assert(ABTreeTupleGetNAtts(right_item, rel) <=
		   IndexRelationGetNumberOfKeyAttributes(rel));
	if (PageAddItem(rootpage, (Item) right_item, right_item_sz, ABTP_FIRSTKEY,
					false, false) == InvalidOffsetNumber)
		elog(PANIC, "failed to add rightkey to new root page"
			 " while splitting block %u of index \"%s\"",
			 BufferGetBlockNumber(lbuf), RelationGetRelationName(rel));

	/* Clear the incomplete-split flag in the left child */
	Assert(ABT_P_INCOMPLETE_SPLIT(lopaque));
	lopaque->abtpo_flags &= ~ABTP_INCOMPLETE_SPLIT;
	MarkBufferDirty(lbuf);

	MarkBufferDirty(rootbuf);
	MarkBufferDirty(metabuf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_abtree_newroot xlrec;
		XLogRecPtr	recptr;
		xl_abtree_metadata md;
		ABTAggSupport agg_support;

		agg_support = ABTMetaPageGetAggSupport(metad);

		xlrec.rootblk = rootblknum;
		xlrec.level = metad->abtm_level;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfABtreeNewroot);

		XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
		XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
		XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

		Assert(metad->abtm_version >= ABTREE_NOVAC_VERSION);
		md.version = metad->abtm_version;
		md.root = rootblknum;
		md.level = metad->abtm_level;
		md.fastroot = rootblknum;
		md.fastlevel = metad->abtm_level;
		md.oldest_abtpo_xact = metad->abtm_oldest_abtpo_xact;
		md.last_cleanup_num_heap_tuples = metad->abtm_last_cleanup_num_heap_tuples;
		md.allequalimage = metad->abtm_allequalimage;

		XLogRegisterBufData(2, (char *) &md, sizeof(xl_abtree_metadata));
		XLogRegisterBufData(2, (char *) agg_support,
			ABTASGetStructSize(agg_support));

		/*
		 * Direct access to page is not good but faster - we should implement
		 * some new func in page API.
		 */
		XLogRegisterBufData(0,
							(char *) rootpage + ((PageHeader) rootpage)->pd_upper,
							((PageHeader) rootpage)->pd_special -
							((PageHeader) rootpage)->pd_upper);

		recptr = XLogInsert(RM_ABTREE_ID, XLOG_ABTREE_NEWROOT);

		PageSetLSN(lpage, recptr);
		PageSetLSN(rootpage, recptr);
		PageSetLSN(metapg, recptr);
	}

	END_CRIT_SECTION();

	/* done with metapage */
	_abt_relbuf(rel, metabuf);

	pfree(left_item);
	pfree(right_item);

	return rootbuf;
}

/*
 *	_abt_pgaddtup() -- add a data item to a particular page during split.
 *
 *		The difference between this routine and a bare PageAddItem call is
 *		that this code can deal with the first data item on an internal btree
 *		page in passing.  This data item (which is called "firstright" within
 *		_abt_split()) has a key that must be treated as minus infinity after
 *		the split.  Therefore, we truncate away all attributes when caller
 *		specifies it's the first data item on page (downlink is not changed,
 *		though).  This extra step is only needed for the right page of an
 *		internal page split.  There is no need to do this for the first data
 *		item on the existing/left page, since that will already have been
 *		truncated during an earlier page split.
 *
 *		See _abt_split() for a high level explanation of why we truncate here.
 *		Note that this routine has nothing to do with suffix truncation,
 *		despite using some of the same infrastructure.
 *
 *		However, the space still needs to be reserved for the aggregation
 *		value for the first item on an internal page.
 */
static inline bool
_abt_pgaddtup(Page page,
			 Size itemsize,
			 IndexTuple itup,
			 OffsetNumber itup_off,
			 bool newfirstdataitem,
			 ABTCachedAggInfo agg_info)
{
	IndexTuple	truncated;
	bool		result;

	if (newfirstdataitem)
	{
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

	if (unlikely(PageAddItem(page, (Item) itup, itemsize, itup_off, false,
							 false) == InvalidOffsetNumber))
	{
		result = false;
	}
	else
	{
		result = true;
	}
	
	if (newfirstdataitem)
		pfree(truncated);
	return result;
}

/*
 * _abt_vacuum_one_page - vacuum just one index page.
 *
 * Try to remove LP_DEAD items from the given page.  The passed buffer
 * must be exclusive-locked, but unlike a real VACUUM, we don't need a
 * super-exclusive "cleanup" lock (see nbtree/README).
 *
 */
static void
_abt_vacuum_one_page(Relation rel, Buffer buffer, Relation heapRel)
{
	OffsetNumber deletable[MaxIndexTuplesPerPage];
	int			ndeletable = 0;
	OffsetNumber offnum,
				minoff,
				maxoff;
	Page		page = BufferGetPage(buffer);
	ABTPageOpaque opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	Assert(ABT_P_ISLEAF(opaque));

	/*
	 * Scan over all items to see which ones need to be deleted according to
	 * LP_DEAD flags.
	 */
	minoff = ABT_P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = minoff;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemId = PageGetItemId(page, offnum);

		if (ItemIdIsDead(itemId))
		{
			deletable[ndeletable++] = offnum;
		}
	}

	if (ndeletable > 0)
	{
		_abt_delitems_delete(rel, buffer, deletable, ndeletable, heapRel);
	}

	/*
	 * Note: if we didn't find any LP_DEAD items, then the page's
	 * ABTP_HAS_GARBAGE hint bit is falsely set.  We do not bother expending a
	 * separate write to clear it, however.  We will clear it when we split
	 * the page, or when deduplication runs.
	 */
}

/*
 * Fix all the aggregations along the path leading to itup (which has been
 * inserted into the tree). The stack is reversed (top-down instead of
 * bottom-up) upon return. The caller should reverse it again if it needs to
 * use it anywhere else.
 */
static void
_abt_fix_path_aggregation_itup(Relation rel,
							   ABTScanInsert itup_key,
							   IndexTuple inserted_itup,
							   OffsetNumber newitemoff,
							   int postingoff,
							   ABTStack stack,
							   BlockNumber leaf_blkno,
							   ABTCachedAggInfo agg_info)
{
	TransactionId			xmin;
	Pointer					agg_ptr;
	Datum					dval;
	Buffer					buf,
							cbuf,
							pbuf;
	BlockNumber				blkno,
							cblkno,
							pblkno;
	Page					page,
							cpage,
							ppage;
	ABTPageOpaque			opaque,
							copaque,
							popaque;
	ItemId					itemid;
	IndexTuple				itup;
	OffsetNumber			off,
							minoff,
							maxoff,
							low,
							high;
	int						i,
							j,
							n;
	TransactionId			*xmin_ptr;
	ItemPointer				tid;
	ABTLastUpdateId			*tuple_last_update_id_ptr;
	bool					found_child_link;
	ABTStack				pstack;

	/* 
	 * xmin should be our top transaction ID and it shouldn't be older
	 * than the global xmin.
	 */
	xmin = GetTopTransactionIdIfAny();
	Assert(!TransactionIdPrecedes(xmin, RecentGlobalDataXmin));
	
	/* get tuple's heap tid and the delta value to add the aggregates */
	Assert(!ABTreeTupleIsPivot(inserted_itup));
	Assert(!ABTreeTupleIsPosting(inserted_itup));
	if (agg_info->leaf_has_agg)
	{
		agg_ptr = ABTreeNonPivotTupleGetAggPtr(inserted_itup, agg_info);
		dval = fetch_att(agg_ptr, agg_info->agg_byval, agg_info->agg_typlen);
	}
	else
	{
		dval = agg_info->agg_map_val;
	}
	
	/* 
	 * Set up search for equal keys and a scan tid. XXX that if someone uses
	 * this itup_key later, we need to recover the old values.
	 */
	Assert(!itup_key->nextkey);
	itup_key->scantid = &inserted_itup->t_tid;

	/*
	 * Start from the fast root. Read-lock it and let it be the current page.
	 *
	 * 1) If current is the leaf page, update the inserted tuple's xmin to our
	 * top xid and we're done.
	 * 2) Find the right downlink either using info in the stack (if it's not
	 * write-locked since last time) or using the itup_key (more expensive but
	 * guaranteed to be correct). Let that be the child page.
	 * 3) Drop the lock on the current page.
	 * 4) Grab a read-lock on the child page.
	 * 5) Read-lock current page again.
	 * 6) If the current page is not split, then we're good. Go ahead to add
	 * the dval to the aggregate and update the version chain. Otherwise,
	 * someone may have undone a lot of our efforts above this level during the
	 * split. Start from somewhere that we are sure updates to upper levels
	 * are still intact.
	 * 6) Drop the lock on current. Let current be the child. Go to 1).
	 *
	 */

	/* 
	 * We need to do it top-down instead of bottom-up to add a delta value.
	 */
	stack = _abt_stack_setup_child_link(stack);

start_from_root:
	/* Start from a true fast root */
	buf = _abt_getroot(rel, ABT_READ, true, agg_info);
	cbuf = InvalidBuffer;

	/* There must be a root because the true has at least one item. */
	Assert(BufferIsValid(buf));

	for (;;)
	{
		blkno = BufferGetBlockNumber(buf);
		page = BufferGetPage(buf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

		if (ABT_P_ISLEAF(opaque))
		{

			/* 
			 * We're at the leaf page. Find and update the xmin of the inserted
			 * tuple. Note here we must be at the correct page.
			 */
			Assert(!stack);

			minoff = ABT_P_FIRSTDATAKEY(opaque);
			maxoff = PageGetMaxOffsetNumber(page);
			xmin_ptr = NULL; /* not found yet */
	
			/* 
			 * newitemoff was the offset when we inserted the tuple. If we are
			 * looking at the same block and it is still valid, try to search
			 * it and its surroundings to find a matching tid. If nothing
			 * yields, just do a binary search using the key to find the item.
			 *
			 * Need to guard around offset number underflow (which is uint16)
			 * for the condition newitemoff - 10 <= maxoff.
			 */
			if (leaf_blkno == blkno && newitemoff <= maxoff + 10)
			{
				low = newitemoff;
				high = newitemoff + 1; /* at least 2 and thus must >= minoff */
				Assert(high >= minoff);

				/* 
				 * We search the surrounding 10 items (21 items in total). This
				 * is arbitrarily chosen right now. But usually there shouldn't
				 * be too much contention around one single page anyway.
				 */
				for (i = 0; i < 21; ++i)
				{
					if ((i & 1) == 0)
					{
						if (low < minoff)
							continue;
						off = low--;
					}
					else
					{
						off = high++;
					}
					if (off > maxoff)
						continue;

					itemid = PageGetItemId(page, off);
					itup = (IndexTuple) PageGetItem(page, itemid);
					
					if (ABTreeTupleIsPosting(itup))
					{
						tid = ABTreeTupleGetPosting(itup);
						n = ABTreeTupleGetNPosting(itup);
						if (off == newitemoff && postingoff != 0 &&
							postingoff < n)
						{
							if (ItemPointerEquals(itup_key->scantid,
												  &tid[postingoff]))
							{
								xmin_ptr = ABTreePostingTupleGetXminPtr(itup)
									+ postingoff;
								break;
							}
						}
						j = _abt_binsrch_posting(itup_key, page, off);
						if (j >= 0 && j < n &&
							ItemPointerEquals(itup_key->scantid,
											  &tid[j]))
						{
							xmin_ptr = ABTreePostingTupleGetXminPtr(itup) + j;
							break;
						}
					}
					else
					{
						if (ItemPointerEquals(itup_key->scantid, &itup->t_tid))
						{
							xmin_ptr = ABTreeNonPivotTupleGetXminPtr(itup);
							break;
						}
					}
				}
			}
			
			/* inserted tuple hasn't been found using tid, do a binary search */
			if (xmin_ptr == NULL)
			{
				off = _abt_binsrch(rel, itup_key, buf);
				
				/* debugging code */
				/*if (off > maxoff)
				{
					Buffer rbuf;
					OffsetNumber off2;
					Page rpage;
					ItemPointer	htid;
					ItemPointer	htid2;
					ItemId	itemid;
					IndexTuple itup;
					int postingoff = -1;
					IndexTuple	prev_hikey;
					ItemPointer	htid_hi;
					
					if (ABT_P_RIGHTMOST(opaque))
						elog(ERROR, "off > maxoff and rightmost");

					rbuf = _abt_getbuf(rel, opaque->abtpo_next, ABT_READ);
					rpage = BufferGetPage(rbuf);
					off2 = _abt_binsrch(rel, itup_key, rbuf);
					
					htid = itup_key->scantid;
					while (off2 <= PageGetMaxOffsetNumber(rpage))
					{
						itemid = PageGetItemId(rpage, off2);
						itup = (IndexTuple) PageGetItem(rpage, itemid);
						htid2 = ABTreeTupleGetHeapTID(itup);
						
						if (ABTreeTupleIsPosting(itup))
						{
							int n = ABTreeTupleGetNPosting(itup);
							postingoff = 0;
							while (postingoff < n)
							{
								if (ItemPointerEquals(&htid2[postingoff], htid))
								{
									htid = htid + postingoff;
									break;
								}
								postingoff++;
							}
							if (postingoff < n)
								break;
						}
						else
						{
							if (ItemPointerEquals(htid2, htid))
								break;
						}

						++off2;
					}

					if (off2 > PageGetMaxOffsetNumber(rpage))
					{
						htid2 = NULL;
						itup = NULL;
					}
					
					{
						ItemId itemid = PageGetItemId(page, ABTP_HIKEY);		
						prev_hikey = (IndexTuple) PageGetItem(page, itemid);
						htid_hi = ABTreeTupleGetHeapTID(prev_hikey);
					}

					Assert(prev_hikey);

					elog(ERROR, "off > maxoff: %d %d, off2 = %d, maxoff2 = %lu, "
							"postingoff = %d, "
							"prev_hikey = (%d, (%u, %u)), "
							"itup = (%d, (%u, %u)), off2 = (%d, (%u, %u))",
							off, maxoff, off2, PageGetMaxOffsetNumber(rpage),
							postingoff,
					*(int32 *)((char *) prev_hikey + 8),
					(htid_hi ? ItemPointerGetBlockNumberNoCheck(htid_hi) : InvalidBlockNumber),
					(htid_hi ? ItemPointerGetOffsetNumberNoCheck(htid_hi) : InvalidOffsetNumber),
					DatumGetInt32(itup_key->scankeys[0].sk_argument),
					ItemPointerGetBlockNumberNoCheck(htid),
					(int32) ItemPointerGetOffsetNumberNoCheck(htid),
					(itup ? (*(int32 *) ((char *) itup + 8)) : 0),
					(htid2 ? ItemPointerGetBlockNumberNoCheck(htid2) : InvalidBlockNumber),
					(htid2 ? ItemPointerGetOffsetNumberNoCheck(htid2) : InvalidOffsetNumber)
							);
				} */

				/* 
				 * There might still be multiple tuple on the leaf page with
				 * the same key. 
				 */
				for (; off <= maxoff; ++off)
				{

					itemid = PageGetItemId(page, off);
					itup = (IndexTuple) PageGetItem(page, itemid);
					
					if (ABTreeTupleIsPosting(itup))
					{
						tid = ABTreeTupleGetPosting(itup);
						n = ABTreeTupleGetNPosting(itup);
						j = _abt_binsrch_posting(itup_key, page, off);
						if (j >= 0 && j < n &&
							ItemPointerEquals(itup_key->scantid, &tid[j]))
						{
							xmin_ptr = ABTreePostingTupleGetXminPtr(itup) + j;
							break;
						}
					}
					else
					{
						if (ItemPointerEquals(itup_key->scantid, &itup->t_tid))
						{
							xmin_ptr = ABTreeNonPivotTupleGetXminPtr(itup);
							break;
						}
					}
				}

				if (xmin_ptr == NULL)
					elog(ERROR, "unable to find the inserted key");

			}
			
			/* we should have a valid xmin_ptr here. Do the update. */
			Assert(xmin_ptr);

			START_CRIT_SECTION();
			
			pg_atomic_write_u32((pg_atomic_uint32 *) xmin_ptr, xmin);
			MarkBufferDirty(buf);

			/* XLOG stuff */	
			if (RelationNeedsWAL(rel))
			{
				xl_abtree_agg_op xlrec;
				XLogRecPtr recptr;

				abt_create_agg_op(&xlrec,
								  XLOG_ABTREE_AGG_OP_ATOMIC_WRITE,
								  ((char *) xmin_ptr) - (char *) page);
				XLogBeginInsert();
				XLogRegisterData((char *) &xlrec, SizeOfABTreeAggOp);
				/* 
				 * not safe to take full page image as there might be
				 * concurrent atomic writers.
				 */
				XLogRegisterBuffer(0, buf, REGBUF_STANDARD | REGBUF_NO_IMAGE);
				XLogRegisterBufData(0, (char *) &xmin, sizeof(TransactionId));
				recptr = XLogInsert(RM_ABTREE_ID, XLOG_ABTREE_AGG_OP);

				/* 
				 * We are updating with shared lock. It only replaces LSN that
				 * is smaller than ours. If someone with a larger LSN on this
				 * page somehow managed to install the LSN before us, that is
				 * fine for redo purpose.
				 */
				PageAtomicSetLSN(page, recptr);
			}

			END_CRIT_SECTION();
			
			/* release the buffer and we're done */
			_abt_relbuf(rel, buf);
			break; /* out of the for (;;) loop */
		}

		/* 
		 * If there's no stack frame for this level. That happens when the root
		 * was split during the initial descend. Create a new stack frame for
		 * later use.
		 */
		if (!stack || stack->abts_level != opaque->abtpo.level)
		{
			ABTStack	new_stack;
			Assert(!stack || stack->abts_level < opaque->abtpo.level);
			
			new_stack = (ABTStack) palloc(sizeof(ABTStackData));
			new_stack->abts_blkno = blkno;
			new_stack->abts_offset = InvalidOffsetNumber;
			/* setting invalid update ID will force a search in the page */
			new_stack->abts_last_update_id = ABT_INVALID_LAST_UPDATE_ID;
			new_stack->abts_parent = stack ? stack->abts_parent : NULL;
			new_stack->abts_level = opaque->abtpo.level;
			new_stack->abts_child = stack;
			if (stack)
			{
				if (stack->abts_parent)
					stack->abts_parent->abts_child = new_stack;
				stack->abts_parent = new_stack;
			}
			stack = new_stack;
		}

		/* 
		 * Find the correct down link. If this is the block we saved in the
		 * current stack frame and its last update id doesn't change, we can
		 * use the cached offset. Otherwise, search for the correct key.
		 */
first_descent:
		if (stack->abts_blkno == blkno &&
			stack->abts_last_update_id == opaque->abtpo_last_update_id &&
			stack->abts_offset != InvalidOffsetNumber)
		{
			/* stack->abts_offset is already valid */
			Assert(ABT_P_RIGHTMOST(opaque) ||
					(!ABT_P_IGNORE(opaque) &&
					 _abt_compare(rel, itup_key, page, ABTP_HIKEY) <= 0));
		}
		else
		{
			/* 
			 * The original page on this level has split since the initial
			 * descend. Move right to the right place. We don't use the more
			 * complicated _abt_stepright() here because we're not inserting
			 * things.
			 */	
			buf = _abt_moveright_and_binsrch(rel, itup_key, buf, stack);
			blkno = stack->abts_blkno;
		}

		/* 
		 * off now points to the correct descend location. Find the downlink.
		 */
		page = BufferGetPage(buf);
		itemid = PageGetItemId(page, stack->abts_offset);
		itup = (IndexTuple) PageGetItem(page, itemid);
		cblkno = ABTreeTupleGetDownLink(itup);

		/* save the tuple last update id */
		//tuple_last_update_id_ptr = ABTreeTupleGetLastUpdateIdPtr(itup);
		//stack->abts_tuple_last_update_id = *tuple_last_update_id_ptr;
	
		/* sanity checks */
		/*{
			OffsetNumber next_off;

			if (!(_abt_compare(rel, itup_key, page, stack->abts_offset) > 0))
			{
				ItemPointer	htid = itup_key->scantid;
				ItemPointer	htid2 = ABTreeTupleGetHeapTID(itup);
				elog(ERROR, "itup_key <= pivot tuple on current page, (%d, (%u, %u)), (%d, (%u, %u))",
						DatumGetInt32(itup_key->scankeys[0].sk_argument),
						ItemPointerGetBlockNumberNoCheck(htid),
						(int32) ItemPointerGetOffsetNumberNoCheck(htid),
						*(int32 *) ((char *) itup + 8),
						(htid2 ? ItemPointerGetBlockNumberNoCheck(htid2) : InvalidBlockNumber),
						(htid2 ? ItemPointerGetOffsetNumberNoCheck(htid2) : InvalidOffsetNumber)
						);
			}
			
			if (stack->abts_offset == PageGetMaxOffsetNumber(page))
				next_off = ABTP_HIKEY;
			else
				next_off = stack->abts_offset + 1;

			if (next_off != ABTP_HIKEY || !ABT_P_RIGHTMOST(opaque))
			{
				if (!(_abt_compare(rel, itup_key, page, next_off) <= 0))
				{
					ItemId	itemid = PageGetItemId(page, next_off);
					IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);
					ItemPointer	htid = itup_key->scantid;
					ItemPointer	htid2 = ABTreeTupleGetHeapTID(itup);
					elog(ERROR, "itup_key > next pivot tuple on current page, (%d, (%u, %u)), (%d, (%u, %u))",
							DatumGetInt32(itup_key->scankeys[0].sk_argument),
							ItemPointerGetBlockNumberNoCheck(htid),
							(int32) ItemPointerGetOffsetNumberNoCheck(htid),
							*(int32 *) ((char *) itup + 8),
							(htid2 ? ItemPointerGetBlockNumberNoCheck(htid2) : InvalidBlockNumber),
							(htid2 ? ItemPointerGetOffsetNumberNoCheck(htid2) : InvalidOffsetNumber)
							);
				}
			}

			if (next_off != ABTP_HIKEY)
			{
				if (next_off == PageGetMaxOffsetNumber(page))
					next_off = ABTP_HIKEY;
				else
					next_off = next_off + 1;

				if (next_off != ABTP_HIKEY || !ABT_P_RIGHTMOST(opaque))
				{
					if (!(_abt_compare(rel, itup_key, page, next_off) < 0))
					{
						ItemId	itemid = PageGetItemId(page, next_off);
						IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);
						ItemPointer	htid = itup_key->scantid;
						ItemPointer	htid2 = ABTreeTupleGetHeapTID(itup);
						elog(ERROR, "itup_key >= the next after the next pivot tuple on current page, (%d, (%u, %u)), (%d, (%u, %u))",
								DatumGetInt32(itup_key->scankeys[0].sk_argument),
								ItemPointerGetBlockNumberNoCheck(htid),
								(int32) ItemPointerGetOffsetNumberNoCheck(htid),
								*(int32 *) ((char *) itup + 8),
								(htid2 ? ItemPointerGetBlockNumberNoCheck(htid2) : InvalidBlockNumber),
								(htid2 ? ItemPointerGetOffsetNumberNoCheck(htid2) : InvalidOffsetNumber)
								);
					}
				}
			}
		} */

		/* 
		 * drop the read lock on the current and grab one on the child 
		 * 
		 * Note: we can't try to grab the lock on the child without releasing
		 * the lock on the current page. _abt_getstackbuf() obtains write locks
		 * in the opposite direction and that will cause deadlock.
		 */
		cbuf = _abt_relandgetbuf(rel, buf, cblkno, ABT_READ);
		
		/* and relock the parent */
		buf = _abt_getbuf(rel, blkno, ABT_READ);
		page = BufferGetPage(buf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

		/* 
		 * oops, someone just vacuum'd, split or inserted into the current
		 * page, and thus may have vacuum'd, split or inserted into the parent
		 * page as well.  We'll need to rewind to some page that we are sure
		 * our updates on higher levels aren't undone.
		 *
		 * While it seems the initial condition looks very similar to
		 * precondition 1) for the for(;;) loop below, it is not safe to move
		 * right on the child level while holding a lock on the current page.
		 * Releaseing the lock on the current page for an extended period may
		 * admit more concurrent split on the upper level and thus undoing more
		 * of our work. Hence, the special treatment of cbuf here. If cbuf
		 * doesn't contain the correct key for descend or for finding the
		 * inserted tuple, we just try to descend from an updated current page
		 * again.
		 */
		if (stack->abts_last_update_id != opaque->abtpo_last_update_id)
		{
			/* did cbuf got split? */
			cblkno = BufferGetBlockNumber(cbuf);
			cpage = BufferGetPage(cbuf);
			copaque = (ABTPageOpaque) PageGetSpecialPointer(cpage);
			if (!ABT_P_RIGHTMOST(copaque))
			{
				if (ABT_P_IGNORE(copaque) ||
					_abt_compare(rel, itup_key, cpage, ABTP_HIKEY) > 0)
				{
					/*
					 * We're sure that the child is split and this is the wrong
					 * child to look at.
					 */
					_abt_relbuf(rel, cbuf);
					cbuf = InvalidBuffer;
				}
				else
				{
					/* 
					 * The child may still be split (just our key happens to
					 * fall on the left. Or the current page is split and that
					 * wipes out the update we have on upper levels. In any
					 * case, we may still need to rewind. See below.
					 */
				}
			}
			

			found_child_link = false;
			if (BufferIsValid(cbuf))
			{
				off = _abt_try_find_downlink(page,
											 stack->abts_offset,
											 cblkno);
				if (off != InvalidOffsetNumber)
				{
					stack->abts_offset = off;
					stack->abts_last_update_id = opaque->abtpo_last_update_id;
					found_child_link = true;
				}
			}
		
			/* 
			 * If cbuf is still valid, we didn't found the child link around
			 * the original offset. Or we know that cbuf is split and is a
			 * wrong descending point.
			 *
			 * In both cases, we want to possibly move right and find the
			 * correct descending point.
			 */
			if (!found_child_link)
			{
				buf = _abt_moveright_and_binsrch(rel, itup_key, buf, stack);
				/* 
				 * The fact that we moved right to find the index tuple means
				 * the current page is definitely split. That must have undone
				 * some of the work on upper level(s). So it's no longer safe
				 * to continue work from the current page.
				 */
				if (stack->abts_blkno != blkno)
				{
					if (BufferIsValid(cbuf))
					{
						_abt_relbuf(rel, cbuf);
						cbuf = InvalidBuffer;
					}
				}
				else
				{
					/* 
					 * However, even if we do not need to move right. We still
					 * need to consult the parent page to determine whether the
					 * current page is split. If so, give up and rewind.
					 */
				}
			}

			/* 
			 * stack->abts_offset, stack->abts_blno and
			 * stack->abts_last_update_id are all up to date at this point,
			 * while blkno, page, opaque are not updated.
			 */

			/* 
			 * cbuf is still valid, meaning it might have not been concurrently
			 * split. Check if the tuple that points to the child page still
			 * has the same last update id since we descend from the current
			 * page.
			 */
			/*if (BufferIsValid(cbuf))
			{
				page = BufferGetPage(buf);
				itemid = PageGetItemId(page, stack->abts_offset);
				itup = (IndexTuple) PageGetItem(page, itemid);

				Assert(ABTreeTupleGetDownLink(itup) == cblkno);
				tuple_last_update_id_ptr = ABTreeTupleGetLastUpdateIdPtr(itup); */

				//if (*tuple_last_update_id_ptr ==
				//	stack->abts_tuple_last_update_id)
				//{
					/* 
					 * Ok, child page didn't split. We're safe to move to the
					 * last part to complete the agg update on this level.
					 */
					//goto do_current_update;
				//}
				//else
				//{
					/* 
					 * Otherwise, cbuf is split. Check ancestors to decide
					 * where to continue. Again, there's still chance that
					 * we're safe to move forward without rewinding back to the
					 * ancestors.  But that needs us to first find the parent
					 * page. Updating the tuple last update id here doesn't have
					 * any impact on the next for loop, but it matters if 
					 * child page is not leaf and we need to further descend
					 * down the tree.
					 */
				//}
			//}

			/* 
			 * grab the parent page, and possibly move right. This is the
			 * only place where could hold 3 (read) locks at the same time.
			 *
			 * At this point, we know that one of the following preconditions
			 * is true:
			 *
			 * 1) cbuf is valid; the child page contains the correct descend
			 * location and the child page may or may not be split. We are not
			 * sure if the the pivot tuple pointing to the current page is
			 * updated or not. If so, give up and rewind. Otherwise, we are safe
			 * from here.
			 *
			 * 2) cbuf is not valid. The child page definitely was split (and
			 * our insertion point moved to the right) or the parent page was
			 * split. We don't have the correct descend location so the best
			 * we can do is go back to the step 2) first_descent. The question
			 * is where. We know that the current page had some updates on our
			 * descend location, we want to know whether the parent has updates
			 * to the pivot tuple linking to the current page.
			 *
			 * In both cases, we know the current page doesn't have any updated
			 * aggregates. So if the parent page's pivot tuple to it gets
			 * updated because of current page split or vacuum, we know the
			 * parent page won't have the correct aggregate value for that
			 * pivot tuple and thus have to rewind to there. Otherwise, it
			 * has a correct value, we're safe to continue from current.
			 *
			 * There are 3 possible scenarios about the pivot tuple on the
			 * parent level linking to the current page.
			 *
			 * a) We don't have a parent stack. So current page was a root. We
			 * do not know whether it still is. If it's not a root now, we must
			 * go back and retry from the root. But in the other case if it's
			 * still a root/fastroot, we might still be safe.  Nevertheless, we
			 * are always safe to start from root because we have not applied
			 * any updates yet. 
			 *
			 * b) The parent page didn't change at all. Then, our updates to
			 * the parent page wasn't undone.
			 *
			 * c) The parent page was updated.  We found our descend location
			 * on the same parent page or to the right. We can use the tuple
			 * last update id to decide whether the pivot tuple on the parent
			 * level was updated or not.
			 *
			 */
			pstack = stack->abts_parent;
			for (;;) {
				/* 
				 * Case a). Just go back to the start and redo everything.
				 */
				if (!pstack)
				{
					if (ABT_P_LEFTMOST(opaque) && ABT_P_RIGHTMOST(opaque))
					{
						/* 
						 * This is still a root/fastroot, we can continue from
						 * here. False negative case: some deleted page to the
						 * right of current page. But we are safe to restart
						 * from root anyway...
						 */
						if (BufferIsValid(cbuf))
						{
							/* 
							 * cbuf is still the correct page, we're safe to
							 * move on to page update on current. 
							 */ 
							goto do_current_update;
						}
						else
						{
							/*
							 * Or we should re-descend from the current page.
							 */
							goto first_descent;
						}
					}

					if (BufferIsValid(cbuf))
					{

						_abt_relbuf(rel, cbuf);
					}
					_abt_relbuf(rel, buf);
					goto start_from_root;
				}

				pblkno = pstack->abts_blkno;
				pbuf = _abt_getbuf(rel, pblkno, ABT_READ);
				ppage = BufferGetPage(pbuf);
				popaque = (ABTPageOpaque) PageGetSpecialPointer(ppage);
			
				/*
				 * Case b) We are safe at least from current level.
				 */
				if (pstack->abts_last_update_id ==
					popaque->abtpo_last_update_id)
				{
					/* 
					 *
					 * Since the parent page didn't change at all, the current
					 * page cannot have been split and we must not have moved
					 * right.
					 */
					Assert(blkno == stack->abts_blkno);
					_abt_relbuf(rel, pbuf);
					if (BufferIsValid(cbuf))
					{
						/* 
						 * cbuf is still the correct page, we're safe to
						 * move on to page update on current. 
						 */ 
						goto do_current_update;
					}
					else
					{
						/*
						 * Or we should re-descend from the current page.
						 * Fortunately, we should have the correct offset
						 * of the child link now.
						 */
						goto first_descent;
					}
				}

				/* 
				 * parent was split or had insertion. Find the descend location
				 * in the parent level, by possibly moving right.
				 */
				pbuf = _abt_moveright_and_binsrch(rel,
												  itup_key,
												  pbuf,
												  pstack);
				ppage = BufferGetPage(pbuf);
				itemid = PageGetItemId(ppage, pstack->abts_offset);
				itup = (IndexTuple) PageGetItem(ppage, itemid);
				tuple_last_update_id_ptr = ABTreeTupleGetLastUpdateIdPtr(itup);
				Assert(stack->abts_blkno == ABTreeTupleGetDownLink(itup));
				
				/*
				 * Case c). If we did not move right and 
				 * the tuple last update id didn't change, then
				 * the parent page was changed for other reasons and our work
				 * on this pivot tuple wasn't undone. We're safe to continue
				 * from current.
				 */
				if (blkno == stack->abts_blkno &&
					*tuple_last_update_id_ptr ==
					pstack->abts_tuple_last_update_id)
				{
					_abt_relbuf(rel, pbuf);
					if (BufferIsValid(cbuf))
					{
						/* 
						 * cbuf is still the correct page, we're safe to
						 * move on to page update on current. 
						 */ 
						goto do_current_update;
					}
					else
					{
						/*
						 * Or we should re-descend from the parent page.
						 * Fortunately, we should have the correct offset
						 * of the child link now.
						 */
						goto first_descent;
					}
				}

				/* 
				 * Othersiwe in case c), the tuple last update id changed.
				 * Then our work was undone in the parent level. We have
				 * established precondition 1) and should continue the loop.
				 */
				if (BufferIsValid(cbuf))
				{
					_abt_relbuf(rel, cbuf);
				}

				cbuf = buf;
				blkno = pblkno;
				buf = pbuf;
				stack = pstack;
				pstack = stack->abts_parent;
				//stack->abts_tuple_last_update_id = *tuple_last_update_id_ptr;
			} /* end of for (;;) */
		}  /* if (stack->abts_last_update_id != opaque->abtpo_last_update_id) */

do_current_update:
		/* 
		 * great! no one could have undone our updates from the parent of the
		 * current page up to up to the root. Let's do the update to current.
		 *
		 * Here stack's all fields are valid. buf and cbuf are both
		 * read-locked.. Don't use others without set them.
		 *
		 * It's not necessary to release the read lock on cbuf right now.
		 * That just guard against concurrent split and vacuum, but not
		 * searches. It also makes our life easier for the next loop.
		 */
		page = BufferGetPage(buf);
		itemid = PageGetItemId(page, stack->abts_offset);
		itup = (IndexTuple) PageGetItem(page, itemid);

		/* check again */
		/*{
			OffsetNumber next_off,
						 maxoff;
			Page cpage;
			ABTPageOpaque	copaque;

			if (!(_abt_compare(rel, itup_key, page, stack->abts_offset) > 0))
			{
				ItemPointer	htid = itup_key->scantid;
				ItemPointer	htid2 = ABTreeTupleGetHeapTID(itup);
				elog(ERROR, "itup_key <= pivot tuple on current page after lock, (%d, (%u, %u)), (%d, (%u, %u)); src = %d",
						DatumGetInt32(itup_key->scankeys[0].sk_argument),
						ItemPointerGetBlockNumberNoCheck(htid),
						(int32) ItemPointerGetOffsetNumberNoCheck(htid),
						*(int32 *) ((char *) itup + 8),
						(htid2 ? ItemPointerGetBlockNumberNoCheck(htid2) : InvalidBlockNumber),
						(htid2 ? ItemPointerGetOffsetNumberNoCheck(htid2) : InvalidOffsetNumber),
						current_src
						);
			}
			
			if (stack->abts_offset == PageGetMaxOffsetNumber(page))
				next_off = ABTP_HIKEY;
			else
				next_off = stack->abts_offset + 1;

			if (next_off != ABTP_HIKEY || !ABT_P_RIGHTMOST(opaque))
			{
				if (!(_abt_compare(rel, itup_key, page, next_off) <= 0))
				{
					ItemId	itemid = PageGetItemId(page, next_off);
					IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);
					ItemPointer	htid = itup_key->scantid;
					ItemPointer	htid2 = ABTreeTupleGetHeapTID(itup);
					elog(ERROR, "itup_key > next pivot tuple on current page after lock, (%d, (%u, %u)), (%d, (%u, %u)); src = %d; found_child_link = %d",
						DatumGetInt32(itup_key->scankeys[0].sk_argument),
						ItemPointerGetBlockNumberNoCheck(htid),
						(int32) ItemPointerGetOffsetNumberNoCheck(htid),
						*(int32 *) ((char *) itup + 8),
						(htid2 ? ItemPointerGetBlockNumberNoCheck(htid2) : InvalidBlockNumber),
						(htid2 ? ItemPointerGetOffsetNumberNoCheck(htid2) : InvalidOffsetNumber),
						current_src,
						(int) found_child_link
							);
				}
			}

			if (next_off != ABTP_HIKEY)
			{
				if (next_off == PageGetMaxOffsetNumber(page))
					next_off = ABTP_HIKEY;
				else
					next_off = next_off + 1;

				if (next_off != ABTP_HIKEY || !ABT_P_RIGHTMOST(opaque))
				{
					if (!(_abt_compare(rel, itup_key, page, next_off) < 0))
					{
						ItemId	itemid = PageGetItemId(page, next_off);
						IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);
						ItemPointer	htid = itup_key->scantid;
						ItemPointer	htid2 = ABTreeTupleGetHeapTID(itup);
						elog(ERROR, "itup_key >= the next after the next pivot "
						"tuple on current page after lock, (%d, (%u, %u)), (%d, (%u, %u)); src =%d",

						DatumGetInt32(itup_key->scankeys[0].sk_argument),
						ItemPointerGetBlockNumberNoCheck(htid),
						(int32) ItemPointerGetOffsetNumberNoCheck(htid),
						*(int32 *) ((char *) itup + 8),
						(htid2 ? ItemPointerGetBlockNumberNoCheck(htid2) : InvalidBlockNumber),
						(htid2 ? ItemPointerGetOffsetNumberNoCheck(htid2) : InvalidOffsetNumber),
						current_src
								);
					}
				}
			}

			off = _abt_binsrch(rel, itup_key, cbuf);
			cpage = BufferGetPage(cbuf);
			copaque = (ABTPageOpaque) PageGetSpecialPointer(cpage);
			maxoff = PageGetMaxOffsetNumber(cpage);
			if (off > maxoff)
			{
				Buffer rbuf;
				OffsetNumber off2;
				Page rpage;
				ItemPointer	htid;
				ItemPointer	htid2;
				ItemId	itemid;
				IndexTuple itup;
				int postingoff = -1;
				IndexTuple	prev_hikey;
				ItemPointer	htid_hi;
				
				if (ABT_P_RIGHTMOST(copaque))
					elog(ERROR, "off > maxoff and rightmost");

				rbuf = _abt_getbuf(rel, copaque->abtpo_next, ABT_READ);
				rpage = BufferGetPage(rbuf);
				off2 = _abt_binsrch(rel, itup_key, rbuf);
				
				htid = itup_key->scantid;
				while (off2 <= PageGetMaxOffsetNumber(rpage))
				{
					itemid = PageGetItemId(rpage, off2);
					itup = (IndexTuple) PageGetItem(rpage, itemid);
					htid2 = ABTreeTupleGetHeapTID(itup);
					
					if (ABTreeTupleIsPosting(itup))
					{
						int n = ABTreeTupleGetNPosting(itup);
						postingoff = 0;
						while (postingoff < n)
						{
							if (ItemPointerEquals(&htid2[postingoff], htid))
							{
								htid = htid + postingoff;
								break;
							}
							postingoff++;
						}
						if (postingoff < n)
							break;
					}
					else
					{
						if (ItemPointerEquals(htid2, htid))
							break;
					}

					++off2;
				}

				if (off2 > PageGetMaxOffsetNumber(rpage))
				{
					htid2 = NULL;
					itup = NULL;
				}
				
				{
					ItemId itemid = PageGetItemId(cpage, ABTP_HIKEY);		
					prev_hikey = (IndexTuple) PageGetItem(cpage, itemid);
					htid_hi = ABTreeTupleGetHeapTID(prev_hikey);
				}

				Assert(prev_hikey);

				elog(ERROR, "off > maxoff: %d %d, off2 = %d, maxoff2 = %lu, "
						"postingoff = %d, "
						"prev_hikey = (%d, (%u, %u)), "
						"itup = (%d, (%u, %u)), off2 = (%d, (%u, %u))",
						off, maxoff, off2, PageGetMaxOffsetNumber(rpage),
						postingoff,
				*(int32 *)((char *) prev_hikey + 8),
				(htid_hi ? ItemPointerGetBlockNumberNoCheck(htid_hi) : InvalidBlockNumber),
				(htid_hi ? ItemPointerGetOffsetNumberNoCheck(htid_hi) : InvalidOffsetNumber),
				DatumGetInt32(itup_key->scankeys[0].sk_argument),
				ItemPointerGetBlockNumberNoCheck(htid),
				(int32) ItemPointerGetOffsetNumberNoCheck(htid),
				(itup ? (*(int32 *) ((char *) itup + 8)) : 0),
				(htid2 ? ItemPointerGetBlockNumberNoCheck(htid2) : InvalidBlockNumber),
				(htid2 ? ItemPointerGetOffsetNumberNoCheck(htid2) : InvalidOffsetNumber)
						);
			}
		} */

		
		/* 
		 * Save the tuple's last update id here. When we need to come back to
		 * the current page to check if this aggregate was updated or not. We
		 * need to see the latest value.
		 */
		tuple_last_update_id_ptr = ABTreeTupleGetLastUpdateIdPtr(itup);
		stack->abts_tuple_last_update_id = *tuple_last_update_id_ptr;
	
		/* get the pointer */
		agg_ptr = ABTreePivotTupleGetAggPtr(itup, agg_info);
		cblkno = ABTreeTupleGetDownLink(itup);

		START_CRIT_SECTION();

		/* XXX currently only support 64-bit, 32-bit and 16-bit integers 
		 * TODO Others need to use a spin lock, which can be implemented using
		 * the ABT_UNUSED_OFFSET_STATUS_BIT1 in the item pointer offset of the
		 * pivot tuple.
		 */
		if (agg_info->agg_typlen == 8)
		{
			pg_atomic_uint64	*lop = (pg_atomic_uint64 *) agg_ptr;
			int64				rop = DatumGetInt64(dval);
			(void) pg_atomic_fetch_add_u64(lop, rop);
		}
		else if (agg_info->agg_typlen == 4)
		{
			pg_atomic_uint32	*lop = (pg_atomic_uint32 *) agg_ptr;
			int32				rop = DatumGetInt32(dval);
			(void) pg_atomic_fetch_add_u32(lop, rop);
		} 
		else
		{
			pg_atomic_uint32	*lop = (pg_atomic_uint32 *) agg_ptr;
			int32				rop = DatumGetInt16(dval);
			Assert(agg_info->agg_typlen == 2);
			(void) pg_atomic_fetch_add_u32(lop, rop);
		}
		MarkBufferDirty(buf);

		if (RelationNeedsWAL(rel))
		{
			xl_abtree_agg_op	xlrec;
			XLogRecPtr			recptr;

			abt_create_agg_op(&xlrec,
							  XLOG_ABTREE_AGG_OP_FETCH_ADD,
							  agg_ptr - (Pointer) page);
			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfABTreeAggOp);
			/* 
			 * not safe to take full page image as there might be
			 * concurrent atomic writers.
			 */
			XLogRegisterBuffer(0, buf, REGBUF_STANDARD | REGBUF_NO_IMAGE);
			if (agg_info->agg_typlen == 8)
			{
				int64 rop = DatumGetInt64(dval);
				XLogRegisterBufData(0, (char *) &rop, 8);
			}
			else if (agg_info->agg_typlen == 4)
			{
				int32 rop = DatumGetInt32(dval);
				XLogRegisterBufData(0, (char *) &rop, 4);
			}
			else
			{
				int16 rop = DatumGetInt16(dval);
				XLogRegisterBufData(0, (char *) &rop, 2);
			}
			recptr = XLogInsert(RM_ABTREE_ID, XLOG_ABTREE_AGG_OP);
			PageAtomicSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		/* update the version chain */
		_abt_add_to_version(rel, cblkno, xmin, dval, agg_info);
		
		/* release the buffer on the current page */
		_abt_relbuf(rel, buf);

		/* and move to the child */
		buf = cbuf;
		stack = stack->abts_child;
	} /* end of for (;;) */

	Assert(!stack);
}

static OffsetNumber
_abt_try_find_downlink(Page page, OffsetNumber start_off, BlockNumber cblkno)
{
	ABTPageOpaque	opaque;
	OffsetNumber	minoff,
					maxoff,
					off,
					low,
					high;
	int				i;
	ItemId			itemid;
	IndexTuple		itup;

	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	/* 
	 * a very simple heuristic is to look at pivot tuple around the
	 * offset we descended, if that offset is still valid (meaning
	 * the split of the current page likely left this cbuf on its
	 * original offset).
	 */
	minoff = ABT_P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);
	
	if (start_off <= maxoff + 10)
	{
		low = start_off;
		high = start_off + 1;
		Assert(high >= minoff);

		/*
		 * We also search the surrounding 10 items (21 items in
		 * total) as in the leaf page case.
		 */
		for (i = 0; i < 21; ++i)
		{
			if ((i & 1) == 0)
			{
				if (low < minoff)
					continue;
				off = low--;
			}
			else
			{
				off = high++;
			}
			if (off > maxoff)
				continue;

			itemid = PageGetItemId(page, off);
			itup = (IndexTuple) PageGetItem(page, itemid);
			if (ABTreeTupleGetDownLink(itup) == cblkno)
			{
				return off;
			}
		}
	}

	return InvalidOffsetNumber;
}

static Buffer
_abt_moveright_and_binsrch(Relation rel,
						   ABTScanInsert itup_key,
						   Buffer buf,
						   ABTStack stack)
{
	Page			page;
	ABTPageOpaque	opaque;
	
	/* 
	 * stack and agg_info are only used if for_update == true so NULL values are
	 * fine here.
	 */
	buf = _abt_moveright(rel, itup_key, buf,
						 /* for_update = */false,
						 /* stack = */NULL,
						 ABT_READ,
						 /* snapshot = */NULL,
						 /* agg_info = */NULL);

	/* search for a key */
	stack->abts_offset = _abt_binsrch(rel, itup_key, buf); 

	/* save info into the stack */
	stack->abts_blkno = BufferGetBlockNumber(buf);
	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	stack->abts_last_update_id = opaque->abtpo_last_update_id;
	Assert(stack->abts_level == opaque->abtpo.level);

	return buf;
}

