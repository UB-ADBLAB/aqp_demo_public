/*-------------------------------------------------------------------------
 *
 * abtpage.c
 *	  Page management code for aggregate btree.
 *
 *	  Copied and adapted from src/backend/access/nbtree/nbtpage.c.
 *
 * IDENTIFICATION
 *	  src/backend/access/abtree/abtpage.c
 *
 *	NOTES
 *	   Postgres btree pages look like ordinary relation pages.  The opaque
 *	   data at high addresses includes pointers to left and right siblings
 *	   and flag data describing page state.  The first page in a btree, page
 *	   zero, is special -- it stores meta-information describing the tree.
 *	   Pages one and higher store the actual tree data.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/abtree.h"
#include "access/abtxlog.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

static ABTMetaPageData *_abt_getmeta(Relation rel, Buffer metabuf);
static void _abt_log_reuse_page(Relation rel, BlockNumber blkno,
							   TransactionId latestRemovedXid);
static TransactionId _abt_xid_horizon(Relation rel, Relation heapRel, Page page,
									 OffsetNumber *deletable, int ndeletable);
static bool _abt_mark_page_halfdead(Relation rel, Buffer leafbuf,
								   ABTStack stack, ABTCachedAggInfo agg_info);
static bool _abt_unlink_halfdead_page(Relation rel, Buffer leafbuf,
									 BlockNumber scanblkno,
									 bool *rightsib_empty,
									 TransactionId *oldestBtpoXact,
									 uint32 *ndeleted);
static bool _abt_lock_subtree_parent(Relation rel, BlockNumber child,
									ABTStack stack,
									Buffer *subtreeparent,
									OffsetNumber *poffset,
									BlockNumber *topparent,
									BlockNumber *topparentrightsib,
									ABTCachedAggInfo agg_info);

/*
 *	_abt_initmetapage() -- Fill a page buffer with a correct metapage image
 */
void
_abt_initmetapage(ABTAggSupport agg_support,
				  Page page, BlockNumber rootbknum, uint32 level,
				  bool allequalimage)
{
	ABTMetaPageData	*metad;
	ABTAggSupport	agg_support_on_page;
	ABTPageOpaque	metaopaque;
	Size			agg_support_size = ABTASGetStructSize(agg_support);

	_abt_pageinit(page, BLCKSZ);
	
	/* Check the size to make sure it fits the remaining space. */
	if (ABTASGetStructSize(agg_support) > ABTAS_MAX_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("ABTAggSupportData's size " UINT64_FORMAT
						" exceeds the maximum " UINT64_FORMAT,
						ABTASGetStructSize(agg_support), ABTAS_SIZE_MASK)));
	}

	metad = ABTPageGetMeta(page);
	metad->abtm_magic = ABTREE_MAGIC;
	metad->abtm_version = ABTREE_VERSION;
	metad->abtm_root = rootbknum;
	metad->abtm_level = level;
	metad->abtm_fastroot = rootbknum;
	metad->abtm_fastlevel = level;
	metad->abtm_oldest_abtpo_xact = InvalidTransactionId;
	metad->abtm_last_cleanup_num_heap_tuples = -1.0;
	metad->abtm_allequalimage = allequalimage;

	agg_support_on_page = ABTMetaPageGetAggSupport(metad);	
	memcpy(agg_support_on_page, agg_support, agg_support_size);
	
	metaopaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	metaopaque->abtpo_flags = ABTP_META;

	/*
	 * Set pd_lower just past the end of the agg_support.  This is essential,
	 * because without doing so, metadata will be lost if xlog.c compresses
	 * the page.
	 */
	((PageHeader) page)->pd_lower =
		((char *) agg_support_on_page + agg_support_size) - (char *) page;

	/* double check pd_lower does not go past pd_upper */
	Assert(((PageHeader) page)->pd_lower <= ((PageHeader) page)->pd_upper);
}

/*
 *	_abt_upgrademetapage() -- Upgrade a meta-page from an old format to version
 *		3, the last version that can be updated without broadly affecting
 *		on-disk compatibility.  (A REINDEX is required to upgrade to v4.)
 *
 *		This routine does purely in-memory image upgrade.  Caller is
 *		responsible for locking, WAL-logging etc.
 */
void
_abt_upgrademetapage(Page page)
{
	/* 
	 * Unfortunately, we do not know the OID or the name of the index here,
	 * but _abt_upgrademetapage() is never supposed to be called anyway.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			 errmsg("abtree cannot have a version lower than %d",
					ABTREE_VERSION)));
}

/*
 * Get metadata from share-locked buffer containing metapage, while performing
 * standard sanity checks.
 *
 * Callers that cache data returned here in local cache should note that an
 * on-the-fly upgrade using _abt_upgrademetapage() can change the version field
 * and ABTREE_NOVAC_VERSION specific fields without invalidating local cache.
 */
static ABTMetaPageData *
_abt_getmeta(Relation rel, Buffer metabuf)
{
	Page		metapg;
	ABTPageOpaque metaopaque;
	ABTMetaPageData *metad;

	metapg = BufferGetPage(metabuf);
	metaopaque = (ABTPageOpaque) PageGetSpecialPointer(metapg);
	metad = ABTPageGetMeta(metapg);

	/* sanity-check the metapage */
	if (!ABT_P_ISMETA(metaopaque) ||
		metad->abtm_magic != ABTREE_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" is not an aggregate btree",
						RelationGetRelationName(rel))));

	if (metad->abtm_version < ABTREE_MIN_VERSION ||
		metad->abtm_version > ABTREE_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("version mismatch in index \"%s\": file version %d, "
						"current version %d, minimal supported version %d",
						RelationGetRelationName(rel),
						metad->abtm_version, ABTREE_VERSION, ABTREE_MIN_VERSION)));

	return metad;
}

/*
 *	_abt_update_meta_cleanup_info() -- Update cleanup-related information in
 *									  the metapage.
 *
 *		This routine checks if provided cleanup-related information is matching
 *		to those written in the metapage.  On mismatch, metapage is overwritten.
 */
void
_abt_update_meta_cleanup_info(Relation rel, TransactionId oldestBtpoXact,
							 float8 numHeapTuples)
{
	Buffer		metabuf;
	Page		metapg;
	ABTMetaPageData *metad;
	bool		needsRewrite = false;
	XLogRecPtr	recptr;

	/* read the metapage and check if it needs rewrite */
	metabuf = _abt_getbuf(rel, ABTREE_METAPAGE, ABT_READ);
	metapg = BufferGetPage(metabuf);
	metad = ABTPageGetMeta(metapg);

	/* All aggregate btrees are newer than version 3. */
	Assert(metad->abtm_version >= ABTREE_MIN_VERSION);
	if (metad->abtm_oldest_abtpo_xact != oldestBtpoXact ||
			 metad->abtm_last_cleanup_num_heap_tuples != numHeapTuples)
		needsRewrite = true;

	if (!needsRewrite)
	{
		_abt_relbuf(rel, metabuf);
		return;
	}

	/* trade in our read lock for a write lock */
	LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
	LockBuffer(metabuf, ABT_WRITE);

	START_CRIT_SECTION();

	/* upgrade meta-page if needed */
	if (metad->abtm_version < ABTREE_NOVAC_VERSION)
		_abt_upgrademetapage(metapg);

	/* update cleanup-related information */
	metad->abtm_oldest_abtpo_xact = oldestBtpoXact;
	metad->abtm_last_cleanup_num_heap_tuples = numHeapTuples;
	MarkBufferDirty(metabuf);

	/* write wal record if needed */
	if (RelationNeedsWAL(rel))
	{
		xl_abtree_metadata md;
		ABTAggSupport agg_support;

		agg_support = ABTMetaPageGetAggSupport(metad);

		XLogBeginInsert();
		XLogRegisterBuffer(0, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

		Assert(metad->abtm_version >= ABTREE_NOVAC_VERSION);
		md.version = metad->abtm_version;
		md.root = metad->abtm_root;
		md.level = metad->abtm_level;
		md.fastroot = metad->abtm_fastroot;
		md.fastlevel = metad->abtm_fastlevel;
		md.oldest_abtpo_xact = oldestBtpoXact;
		md.last_cleanup_num_heap_tuples = numHeapTuples;
		md.allequalimage = metad->abtm_allequalimage;

		XLogRegisterBufData(0, (char *) &md, sizeof(xl_abtree_metadata));
		XLogRegisterBufData(0, (char *) agg_support,
			ABTASGetStructSize(agg_support));

		recptr = XLogInsert(RM_ABTREE_ID, XLOG_ABTREE_META_CLEANUP);

		PageSetLSN(metapg, recptr);
	}

	END_CRIT_SECTION();
	_abt_relbuf(rel, metabuf);
}

/*
 *	_abt_getroot() -- Get the root page of the btree.
 *
 *		Since the root page can move around the btree file, we have to read
 *		its location from the metadata page, and then read the root page
 *		itself.  If no root page exists yet, we have to create one.
 *
 *		The access type parameter (ABT_READ or ABT_WRITE) controls whether
 *		a new root page will be created or not.  If access = ABT_READ,
 *		and no root page exists, we just return InvalidBuffer.  For
 *		ABT_WRITE, we try to create the root page if it doesn't exist.
 *		NOTE that the returned root page will have only a read lock set
 *		on it even if access = ABT_WRITE!
 *
 *		The returned page is not necessarily the true root --- it could be
 *		a "fast root" (a page that is alone in its level due to deletions).
 *		Also, if the root page is split while we are "in flight" to it,
 *		what we will return is the old root, which is now just the leftmost
 *		page on a probably-not-very-wide level.  For most purposes this is
 *		as good as or better than the true root, so we do not bother to
 *		insist on finding the true root.  We do, however, guarantee to
 *		return a live (not deleted or half-dead) page.
 *
 *		If reject_non_fastroot == true, we retry until we have a fast root/
 *		true root. However, if we find that the page we got has an incomplete
 *		split flag, we'll try to clear that flag by finishing the split. This
 *		will prevent indefinite wait of a sampling caller after a crash.
 *		agg_info is only accessed if reject_non_fastroot == true.
 *
 *		On successful return, the root page is pinned and read-locked.
 *		The metadata page is not locked or pinned on exit.
 */
Buffer
_abt_getroot(Relation rel, int access, bool reject_non_fastroot,
			 ABTCachedAggInfo agg_info)
{
	Buffer		metabuf;
	Buffer		rootbuf;
	Page		rootpage;
	ABTPageOpaque rootopaque;
	BlockNumber rootblkno;
	uint32		rootlevel;
	ABTMetaPageData *metad;

	/*
	 * Try to use previously-cached metapage data to find the root.  This
	 * normally saves one buffer access per index search, which is a very
	 * helpful savings in bufmgr traffic and hence contention.
	 */
	if ((metad = _abt_get_cached_metapage(rel)) != NULL)
	{
		/* We shouldn't have cached it if any of these fail */
		Assert(metad->abtm_magic == ABTREE_MAGIC);
		Assert(metad->abtm_version >= ABTREE_MIN_VERSION);
		Assert(metad->abtm_version <= ABTREE_VERSION);
		Assert(metad->abtm_root != ABTP_NONE);

		rootblkno = metad->abtm_fastroot;
		Assert(rootblkno != ABTP_NONE);
		rootlevel = metad->abtm_fastlevel;

		rootbuf = _abt_getbuf(rel, rootblkno, ABT_READ);
		rootpage = BufferGetPage(rootbuf);
		rootopaque = (ABTPageOpaque) PageGetSpecialPointer(rootpage);

		/*
		 * Since the cache might be stale, we check the page more carefully
		 * here than normal.  We *must* check that it's not deleted. If it's
		 * not alone on its level, then we reject too --- this may be overly
		 * paranoid but better safe than sorry.  Note we don't check ABTP_ISROOT,
		 * because that's not set in a "fast root".
		 */
		if (!ABT_P_IGNORE(rootopaque) &&
			rootopaque->abtpo.level == rootlevel &&
			ABT_P_LEFTMOST(rootopaque) &&
			ABT_P_RIGHTMOST(rootopaque))
		{
			/* OK, accept cached page as the root */
			return rootbuf;
		}
		_abt_relbuf(rel, rootbuf);
		/* Cache is stale, throw it away */
		_abt_clear_cached_metapage(rel);
	}

retry:
	metabuf = _abt_getbuf(rel, ABTREE_METAPAGE, ABT_READ);
	metad = _abt_getmeta(rel, metabuf);

	/* if no root page initialized yet, do it */
	if (metad->abtm_root == ABTP_NONE)
	{
		Page		metapg;

		/* If access = ABT_READ, caller doesn't want us to create root yet */
		if (access == ABT_READ)
		{
			_abt_relbuf(rel, metabuf);
			return InvalidBuffer;
		}

		/* trade in our read lock for a write lock */
		LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
		LockBuffer(metabuf, ABT_WRITE);

		/*
		 * Race condition:	if someone else initialized the metadata between
		 * the time we released the read lock and acquired the write lock, we
		 * must avoid doing it again.
		 */
		if (metad->abtm_root != ABTP_NONE)
		{
			/*
			 * Metadata initialized by someone else.  In order to guarantee no
			 * deadlocks, we have to release the metadata page and start all
			 * over again.  (Is that really true? But it's hardly worth trying
			 * to optimize this case.)
			 */
			_abt_relbuf(rel, metabuf);
			goto retry;
		}

		/*
		 * Get, initialize, write, and leave a lock of the appropriate type on
		 * the new root page.  Since this is the first page in the tree, it's
		 * a leaf as well as the root.
		 */
		rootbuf = _abt_getbuf(rel, P_NEW, ABT_WRITE);
		rootblkno = BufferGetBlockNumber(rootbuf);
		rootpage = BufferGetPage(rootbuf);
		rootopaque = (ABTPageOpaque) PageGetSpecialPointer(rootpage);
		rootopaque->abtpo_prev = rootopaque->abtpo_next = ABTP_NONE;
		rootopaque->abtpo_flags = (ABTP_LEAF | ABTP_ROOT);
		rootopaque->abtpo.level = 0;
		rootopaque->abtpo_cycleid = 0;
		/* Get raw page pointer for metapage */
		metapg = BufferGetPage(metabuf);

		/* NO ELOG(ERROR) till meta is updated */
		START_CRIT_SECTION();

		metad->abtm_root = rootblkno;
		metad->abtm_level = 0;
		metad->abtm_fastroot = rootblkno;
		metad->abtm_fastlevel = 0;
		metad->abtm_oldest_abtpo_xact = InvalidTransactionId;
		metad->abtm_last_cleanup_num_heap_tuples = -1.0;

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

			XLogBeginInsert();
			XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
			XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

			Assert(metad->abtm_version >= ABTREE_NOVAC_VERSION);
			md.version = metad->abtm_version;
			md.root = rootblkno;
			md.level = 0;
			md.fastroot = rootblkno;
			md.fastlevel = 0;
			md.oldest_abtpo_xact = InvalidTransactionId;
			md.last_cleanup_num_heap_tuples = -1.0;
			md.allequalimage = metad->abtm_allequalimage;
			
			XLogRegisterBufData(2, (char *) &md, sizeof(xl_abtree_metadata));
			XLogRegisterBufData(2, (char *) agg_support,
				ABTASGetStructSize(agg_support));

			xlrec.rootblk = rootblkno;
			xlrec.level = 0;

			XLogRegisterData((char *) &xlrec, SizeOfABtreeNewroot);

			recptr = XLogInsert(RM_ABTREE_ID, XLOG_ABTREE_NEWROOT);

			PageSetLSN(rootpage, recptr);
			PageSetLSN(metapg, recptr);
		}

		END_CRIT_SECTION();
	
		/*
		 * swap root write lock for read lock.  There is no danger of anyone
		 * else accessing the new root page while it's unlocked, since no one
		 * else knows where it is yet.
		 */
		LockBuffer(rootbuf, BUFFER_LOCK_UNLOCK);
		LockBuffer(rootbuf, ABT_READ);

		/* okay, metadata is correct, release lock on it without caching */
		_abt_relbuf(rel, metabuf);
	}
	else
	{
		rootblkno = metad->abtm_fastroot;
		Assert(rootblkno != ABTP_NONE);
		rootlevel = metad->abtm_fastlevel;

		/*
		 * We are done with the metapage; arrange to release it via first
		 * _abt_relandgetbuf call
		 */
		rootbuf = metabuf;

		for (;;)
		{
			rootbuf = _abt_relandgetbuf(rel, rootbuf, rootblkno, ABT_READ);
			rootpage = BufferGetPage(rootbuf);
			rootopaque = (ABTPageOpaque) PageGetSpecialPointer(rootpage);

			if (!ABT_P_IGNORE(rootopaque))
				break;

			/* it's dead, Jim.  step right one page */
			if (ABT_P_RIGHTMOST(rootopaque))
				elog(ERROR, "no live root page found in index \"%s\"",
					 RelationGetRelationName(rel));
			rootblkno = rootopaque->abtpo_next;
		}

		/* 
		 * Everything on our left is dead. We need to check for any live page
		 * on the right, if there's anything to the right.
		 */
		if (reject_non_fastroot && !ABT_P_RIGHTMOST(rootopaque))
		{
			BlockNumber		blkno;
			Buffer			buf;
			Page			page;
			ABTPageOpaque	opaque;
			
			/* 
			 * If this is an incomplete split, we should go ahead and finish
			 * the split. Otherwise, we may enter an infinite loop waiting
			 * for someone to finish that.
			 */
			if (ABT_P_INCOMPLETE_SPLIT(rootopaque))
			{
				LockBuffer(rootbuf, BUFFER_LOCK_UNLOCK);
				LockBuffer(rootbuf, ABT_WRITE);

				rootpage = BufferGetPage(rootbuf);
				rootopaque = (ABTPageOpaque) PageGetSpecialPointer(rootpage);

				if (ABT_P_INCOMPLETE_SPLIT(rootopaque))
				{
					_abt_finish_split(rel, rootbuf, /* stack =*/NULL, agg_info);
				}
				/* 
				 * whether we did finish that split ourselves, we need to try
				 * to get the root again. 
				 */
				goto retry;
			}

	
			blkno = rootopaque->abtpo_next;
			while (blkno != ABTP_NONE)
			{
				buf = _abt_getbuf(rel, blkno, ABT_READ);
				page = BufferGetPage(buf);
				opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

				if (!ABT_P_IGNORE(opaque))
				{
					/* Found some live page on the same level, retry. */
					_abt_relbuf(rel, buf);
					_abt_relbuf(rel, rootbuf);
					goto retry;
				}

				blkno = opaque->abtpo_next;
				_abt_relbuf(rel, buf);
			}
		}

		/* Note: can't check abtpo.level on deleted pages */
		if (rootopaque->abtpo.level != rootlevel)
			elog(ERROR, "root page %u of index \"%s\" has level %u, expected %u",
				 rootblkno, RelationGetRelationName(rel),
				 rootopaque->abtpo.level, rootlevel);

		/*
		 * Seems ok, cache the metapage data for next time.
		 */
		_abt_save_metapage(rel, metad, NULL);
	}

	/*
	 * By here, we have a pin and read lock on the root page, and no lock set
	 * on the metadata page.  Return the root page's buffer.
	 */
	return rootbuf;
}

/*
 *	_abt_gettrueroot() -- Get the true root page of the btree.
 *
 *		This is the same as the ABT_READ case of _abt_getroot(), except
 *		we follow the true-root link not the fast-root link.
 *
 * By the time we acquire lock on the root page, it might have been split and
 * not be the true root anymore.  This is okay for the present uses of this
 * routine; we only really need to be able to move up at least one tree level
 * from whatever non-root page we were at.  If we ever do need to lock the
 * one true root page, we could loop here, re-reading the metapage on each
 * failure.  (Note that it wouldn't do to hold the lock on the metapage while
 * moving to the root --- that'd deadlock against any concurrent root split.)
 */
Buffer
_abt_gettrueroot(Relation rel)
{
	Buffer		metabuf;
	Page		metapg;
	ABTPageOpaque metaopaque;
	Buffer		rootbuf;
	Page		rootpage;
	ABTPageOpaque rootopaque;
	BlockNumber rootblkno;
	uint32		rootlevel;
	ABTMetaPageData *metad;

	/*
	 * We don't try to use cached metapage data here, since (a) this path is
	 * not performance-critical, and (b) if we are here it suggests our cache
	 * is out-of-date anyway.  In light of point (b), it's probably safest to
	 * actively flush any cached metapage info.
	 */
	_abt_clear_cached_metapage(rel);

	metabuf = _abt_getbuf(rel, ABTREE_METAPAGE, ABT_READ);
	metapg = BufferGetPage(metabuf);
	metaopaque = (ABTPageOpaque) PageGetSpecialPointer(metapg);
	metad = ABTPageGetMeta(metapg);

	if (!ABT_P_ISMETA(metaopaque) ||
		metad->abtm_magic != ABTREE_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" is not a btree",
						RelationGetRelationName(rel))));

	if (metad->abtm_version < ABTREE_MIN_VERSION ||
		metad->abtm_version > ABTREE_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("version mismatch in index \"%s\": file version %d, "
						"current version %d, minimal supported version %d",
						RelationGetRelationName(rel),
						metad->abtm_version, ABTREE_VERSION, ABTREE_MIN_VERSION)));

	/* if no root page initialized yet, fail */
	if (metad->abtm_root == ABTP_NONE)
	{
		_abt_relbuf(rel, metabuf);
		return InvalidBuffer;
	}

	rootblkno = metad->abtm_root;
	rootlevel = metad->abtm_level;

	/*
	 * We are done with the metapage; arrange to release it via first
	 * _abt_relandgetbuf call
	 */
	rootbuf = metabuf;

	for (;;)
	{
		rootbuf = _abt_relandgetbuf(rel, rootbuf, rootblkno, ABT_READ);
		rootpage = BufferGetPage(rootbuf);
		rootopaque = (ABTPageOpaque) PageGetSpecialPointer(rootpage);

		if (!ABT_P_IGNORE(rootopaque))
			break;

		/* it's dead, Jim.  step right one page */
		if (ABT_P_RIGHTMOST(rootopaque))
			elog(ERROR, "no live root page found in index \"%s\"",
				 RelationGetRelationName(rel));
		rootblkno = rootopaque->abtpo_next;
	}

	/* Note: can't check abtpo.level on deleted pages */
	if (rootopaque->abtpo.level != rootlevel)
		elog(ERROR, "root page %u of index \"%s\" has level %u, expected %u",
			 rootblkno, RelationGetRelationName(rel),
			 rootopaque->abtpo.level, rootlevel);

	return rootbuf;
}

/*
 *	_abt_getrootheight() -- Get the height of the btree search tree.
 *
 *		We return the level (counting from zero) of the current fast root.
 *		This represents the number of tree levels we'd have to descend through
 *		to start any btree index search.
 *
 *		This is used by the planner for cost-estimation purposes.  Since it's
 *		only an estimate, slightly-stale data is fine, hence we don't worry
 *		about updating previously cached data.
 */
int
_abt_getrootheight(Relation rel)
{
	ABTMetaPageData *metad;

	if ((metad = _abt_get_cached_metapage(rel)) == NULL)
	{
		Buffer		metabuf;

		metabuf = _abt_getbuf(rel, ABTREE_METAPAGE, ABT_READ);
		metad = _abt_getmeta(rel, metabuf);

		/*
		 * If there's no root page yet, _abt_getroot() doesn't expect a cache
		 * to be made, so just stop here and report the index height is zero.
		 * (XXX perhaps _abt_getroot() should be changed to allow this case.)
		 */
		if (metad->abtm_root == ABTP_NONE)
		{
			_abt_relbuf(rel, metabuf);
			return 0;
		}

		/*
		 * Cache the metapage data for next time
		 */
		_abt_save_metapage(rel, metad, NULL);
		_abt_relbuf(rel, metabuf);
	}

	/* Get cached page */
	metad = _abt_get_cached_metapage(rel);
	/* We shouldn't have cached it if any of these fail */
	Assert(metad->abtm_magic == ABTREE_MAGIC);
	Assert(metad->abtm_version >= ABTREE_MIN_VERSION);
	Assert(metad->abtm_version <= ABTREE_VERSION);
	Assert(metad->abtm_fastroot != ABTP_NONE);

	return metad->abtm_fastlevel;
}

/*
 *	_abt_metaversion() -- Get version/status info from metapage.
 *
 *		Sets caller's *heapkeyspace and *allequalimage arguments using data
 *		from the B-Tree metapage (could be locally-cached version).  This
 *		information needs to be stashed in insertion scankey, so we provide a
 *		single function that fetches both at once.
 *
 *		This is used to determine the rules that must be used to descend a
 *		btree.  Version 4 indexes treat heap TID as a tiebreaker attribute.
 *		pg_upgrade'd version 3 indexes need extra steps to preserve reasonable
 *		performance when inserting a new ABTScanInsert-wise duplicate tuple
 *		among many leaf pages already full of such duplicates.
 *
 *		Also sets allequalimage field, which indicates whether or not it is
 *		safe to apply deduplication.  We rely on the assumption that
 *		abtm_allequalimage will be zero'ed on heapkeyspace indexes that were
 *		pg_upgrade'd from Postgres 12.
 */
void
_abt_metaversion(Relation rel, bool *heapkeyspace, bool *allequalimage)
{
	ABTMetaPageData *metad;
	
	/* *heapkeyspace is always true because aggregate b-trees are always
	 * newer than ABTREE_NOVAC_VERSION */
	*heapkeyspace = true;
	if ((metad = _abt_get_cached_metapage(rel)) == NULL)
	{
		Buffer		metabuf;

		metabuf = _abt_getbuf(rel, ABTREE_METAPAGE, ABT_READ);
		metad = _abt_getmeta(rel, metabuf);

		/*
		 * If there's no root page yet, _abt_getroot() doesn't expect a cache
		 * to be made, so just stop here.  (XXX perhaps _abt_getroot() should
		 * be changed to allow this case.)
		 */
		if (metad->abtm_root == ABTP_NONE)
		{
			*allequalimage = metad->abtm_allequalimage;

			_abt_relbuf(rel, metabuf);
			return;
		}

		/*
		 * Cache the metapage data for next time
		 *
		 * An on-the-fly version upgrade performed by _abt_upgrademetapage()
		 * can change the nbtree version for an index without invalidating any
		 * local cache.  This is okay because it can only happen when moving
		 * from version 2 to version 3, both of which are !heapkeyspace
		 * versions.
		 */
		_abt_save_metapage(rel, metad, NULL);
		_abt_relbuf(rel, metabuf);
	}

	/* Get cached page */
	metad = _abt_get_cached_metapage(rel);
	/* We shouldn't have cached it if any of these fail */
	Assert(metad->abtm_magic == ABTREE_MAGIC);
	Assert(metad->abtm_version >= ABTREE_MIN_VERSION);
	Assert(metad->abtm_version <= ABTREE_VERSION);
	Assert(metad->abtm_fastroot != ABTP_NONE);

	*allequalimage = metad->abtm_allequalimage;
}

/*
 *	_abt_checkpage() -- Verify that a freshly-read page looks sane.
 */
void
_abt_checkpage(Relation rel, Buffer buf)
{
	Page		page = BufferGetPage(buf);

	/*
	 * ReadBuffer verifies that every newly-read page passes
	 * PageHeaderIsValid, which means it either contains a reasonably sane
	 * page header or is all-zero.  We have to defend against the all-zero
	 * case, however.
	 */
	if (PageIsNew(page))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" contains unexpected zero page at block %u",
						RelationGetRelationName(rel),
						BufferGetBlockNumber(buf)),
				 errhint("Please REINDEX it.")));

	/*
	 * Additionally check that the special area looks sane.
	 */
	if (PageGetSpecialSize(page) != MAXALIGN(sizeof(ABTPageOpaqueData)))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" contains corrupted page at block %u",
						RelationGetRelationName(rel),
						BufferGetBlockNumber(buf)),
				 errhint("Please REINDEX it.")));
}

/*
 * Log the reuse of a page from the FSM.
 */
static void
_abt_log_reuse_page(Relation rel, BlockNumber blkno, TransactionId latestRemovedXid)
{
	xl_abtree_reuse_page xlrec_reuse;

	/*
	 * Note that we don't register the buffer with the record, because this
	 * operation doesn't modify the page. This record only exists to provide a
	 * conflict point for Hot Standby.
	 */

	/* XLOG stuff */
	xlrec_reuse.node = rel->rd_node;
	xlrec_reuse.block = blkno;
	xlrec_reuse.latestRemovedXid = latestRemovedXid;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec_reuse, SizeOfABtreeReusePage);

	XLogInsert(RM_ABTREE_ID, XLOG_ABTREE_REUSE_PAGE);
}

/*
 *	_abt_getbuf() -- Get a buffer by block number for read or write.
 *
 *		blkno == P_NEW means to get an unallocated index page.  The page
 *		will be initialized before returning it.
 *
 *		When this routine returns, the appropriate lock is set on the
 *		requested buffer and its reference count has been incremented
 *		(ie, the buffer is "locked and pinned").  Also, we apply
 *		_abt_checkpage to sanity-check the page (except in P_NEW case).
 */
Buffer
_abt_getbuf(Relation rel, BlockNumber blkno, int access)
{
	Buffer		buf;

	if (blkno != P_NEW)
	{
		/* Read an existing block of the relation */
		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, access);
		_abt_checkpage(rel, buf);
	}
	else
	{
		bool		needLock;
		Page		page;

		Assert(access == ABT_WRITE);

		/*
		 * First see if the FSM knows of any free pages.
		 *
		 * We can't trust the FSM's report unreservedly; we have to check that
		 * the page is still free.  (For example, an already-free page could
		 * have been re-used between the time the last VACUUM scanned it and
		 * the time the VACUUM made its FSM updates.)
		 *
		 * In fact, it's worse than that: we can't even assume that it's safe
		 * to take a lock on the reported page.  If somebody else has a lock
		 * on it, or even worse our own caller does, we could deadlock.  (The
		 * own-caller scenario is actually not improbable. Consider an index
		 * on a serial or timestamp column.  Nearly all splits will be at the
		 * rightmost page, so it's entirely likely that _abt_split will call us
		 * while holding a lock on the page most recently acquired from FSM. A
		 * VACUUM running concurrently with the previous split could well have
		 * placed that page back in FSM.)
		 *
		 * To get around that, we ask for only a conditional lock on the
		 * reported page.  If we fail, then someone else is using the page,
		 * and we may reasonably assume it's not free.  (If we happen to be
		 * wrong, the worst consequence is the page will be lost to use till
		 * the next VACUUM, which is no big problem.)
		 */
		for (;;)
		{
			blkno = GetFreeIndexPage(rel);
			if (blkno == InvalidBlockNumber)
				break;
			buf = ReadBuffer(rel, blkno);
			if (ConditionalLockBuffer(buf))
			{
				page = BufferGetPage(buf);
				if (_abt_page_recyclable(page))
				{
					/*
					 * If we are generating WAL for Hot Standby then create a
					 * WAL record that will allow us to conflict with queries
					 * running on standby, in case they have snapshots older
					 * than abtpo.xact.  This can only apply if the page does
					 * have a valid abtpo.xact value, ie not if it's new.  (We
					 * must check that because an all-zero page has no special
					 * space.)
					 */
					if (XLogStandbyInfoActive() && RelationNeedsWAL(rel) &&
						!PageIsNew(page))
					{
						ABTPageOpaque opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

						_abt_log_reuse_page(rel, blkno, opaque->abtpo.xact);
					}

					/* Okay to use page.  Re-initialize and return it */
					_abt_pageinit(page, BufferGetPageSize(buf));
					return buf;
				}
				elog(DEBUG2, "FSM returned nonrecyclable page");
				_abt_relbuf(rel, buf);
			}
			else
			{
				elog(DEBUG2, "FSM returned nonlockable page");
				/* couldn't get lock, so just drop pin */
				ReleaseBuffer(buf);
			}
		}

		/*
		 * Extend the relation by one page.
		 *
		 * We have to use a lock to ensure no one else is extending the rel at
		 * the same time, else we will both try to initialize the same new
		 * page.  We can skip locking for new or temp relations, however,
		 * since no one else could be accessing them.
		 */
		needLock = !RELATION_IS_LOCAL(rel);

		if (needLock)
			LockRelationForExtension(rel, ExclusiveLock);

		buf = ReadBuffer(rel, P_NEW);

		/* Acquire buffer lock on new page */
		LockBuffer(buf, ABT_WRITE);

		/*
		 * Release the file-extension lock; it's now OK for someone else to
		 * extend the relation some more.  Note that we cannot release this
		 * lock before we have buffer lock on the new page, or we risk a race
		 * condition against btvacuumscan --- see comments therein.
		 */
		if (needLock)
			UnlockRelationForExtension(rel, ExclusiveLock);

		/* Initialize the new page before returning it */
		page = BufferGetPage(buf);
		Assert(PageIsNew(page));
		_abt_pageinit(page, BufferGetPageSize(buf));
	}

	/* ref count and lock type are correct */
	return buf;
}

/*
 *	_abt_relandgetbuf() -- release a locked buffer and get another one.
 *
 * This is equivalent to _abt_relbuf followed by _abt_getbuf, with the
 * exception that blkno may not be P_NEW.  Also, if obuf is InvalidBuffer
 * then it reduces to just _abt_getbuf; allowing this case simplifies some
 * callers.
 *
 * The original motivation for using this was to avoid two entries to the
 * bufmgr when one would do.  However, now it's mainly just a notational
 * convenience.  The only case where it saves work over _abt_relbuf/_abt_getbuf
 * is when the target page is the same one already in the buffer.
 */
Buffer
_abt_relandgetbuf(Relation rel, Buffer obuf, BlockNumber blkno, int access)
{
	Buffer		buf;

	Assert(blkno != P_NEW);
	if (BufferIsValid(obuf))
		LockBuffer(obuf, BUFFER_LOCK_UNLOCK);
	buf = ReleaseAndReadBuffer(obuf, rel, blkno);
	LockBuffer(buf, access);
	_abt_checkpage(rel, buf);
	return buf;
}

/*
 *	_abt_relbuf() -- release a locked buffer.
 *
 * Lock and pin (refcount) are both dropped.
 */
void
_abt_relbuf(Relation rel, Buffer buf)
{
	UnlockReleaseBuffer(buf);
}

/*
 *	_abt_pageinit() -- Initialize a new page.
 *
 * On return, the page header is initialized; data space is empty;
 * special space is zeroed out.
 */
void
_abt_pageinit(Page page, Size size)
{
	PageInit(page, size, sizeof(ABTPageOpaqueData));
}

/*
 *	_abt_page_recyclable() -- Is an existing page recyclable?
 *
 * This exists to make sure _abt_getbuf and abtvacuumscan have the same
 * policy about whether a page is safe to re-use.  But note that _abt_getbuf
 * knows enough to distinguish the PageIsNew condition from the other one.
 * At some point it might be appropriate to redesign this to have a three-way
 * result value.
 */
bool
_abt_page_recyclable(Page page)
{
	ABTPageOpaque opaque;

	/*
	 * It's possible to find an all-zeroes page in an index --- for example, a
	 * backend might successfully extend the relation one page and then crash
	 * before it is able to make a WAL entry for adding the page. If we find a
	 * zeroed page then reclaim it.
	 */
	if (PageIsNew(page))
		return true;

	/*
	 * Otherwise, recycle if deleted and too old to have any processes
	 * interested in it.
	 */
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	if (ABT_P_ISDELETED(opaque) &&
		TransactionIdPrecedes(opaque->abtpo.xact, RecentGlobalXmin))
		return true;
	return false;
}

/*
 * Delete item(s) from a btree leaf page during VACUUM.
 *
 * This routine assumes that the caller has a super-exclusive write lock on
 * the buffer.  Also, the given deletable and updatable arrays *must* be
 * sorted in ascending order.
 *
 * Routine deals with deleting TIDs when some (but not all) of the heap TIDs
 * in an existing posting list item are to be removed by VACUUM.  This works
 * by updating/overwriting an existing item with caller's new version of the
 * item (a version that lacks the TIDs that are to be deleted).
 *
 * We record VACUUMs and b-tree deletes differently in WAL.  Deletes must
 * generate their own latestRemovedXid by accessing the heap directly, whereas
 * VACUUMs rely on the initial heap scan taking care of it indirectly.  Also,
 * only VACUUM can perform granular deletes of individual TIDs in posting list
 * tuples.
 */
void
_abt_delitems_vacuum(Relation rel, Buffer buf,
					OffsetNumber *deletable, int ndeletable,
					ABTVacuumPosting *updatable, int nupdatable,
					ABTCachedAggInfo agg_info)
{
	Page		page = BufferGetPage(buf);
	ABTPageOpaque opaque;
	Size		itemsz;
	char	   *updatedbuf = NULL;
	Size		updatedbuflen = 0;
	OffsetNumber updatedoffsets[MaxIndexTuplesPerPage];

	/* Shouldn't be called unless there's something to do */
	Assert(ndeletable > 0 || nupdatable > 0);

	for (int i = 0; i < nupdatable; i++)
	{
		/* Replace work area IndexTuple with updated version */
		_abt_update_posting(updatable[i], agg_info);

		/* Maintain array of updatable page offsets for WAL record */
		updatedoffsets[i] = updatable[i]->updatedoffset;
	}

	/* XLOG stuff -- allocate and fill buffer before critical section */
	if (nupdatable > 0 && RelationNeedsWAL(rel))
	{
		Size		offset = 0;

		for (int i = 0; i < nupdatable; i++)
		{
			ABTVacuumPosting vacposting = updatable[i];

			itemsz = SizeOfABtreeUpdate +
				vacposting->ndeletedtids * sizeof(uint16);
			updatedbuflen += itemsz;
		}

		updatedbuf = palloc(updatedbuflen);
		for (int i = 0; i < nupdatable; i++)
		{
			ABTVacuumPosting vacposting = updatable[i];
			xl_abtree_update update;

			update.ndeletedtids = vacposting->ndeletedtids;
			memcpy(updatedbuf + offset, &update.ndeletedtids,
				   SizeOfABtreeUpdate);
			offset += SizeOfABtreeUpdate;

			itemsz = update.ndeletedtids * sizeof(uint16);
			memcpy(updatedbuf + offset, vacposting->deletetids, itemsz);
			offset += itemsz;
		}
	}

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	/*
	 * Handle posting tuple updates.
	 *
	 * Deliberately do this before handling simple deletes.  If we did it the
	 * other way around (i.e. WAL record order -- simple deletes before
	 * updates) then we'd have to make compensating changes to the 'updatable'
	 * array of offset numbers.
	 *
	 * PageIndexTupleOverwrite() won't unset each item's LP_DEAD bit when it
	 * happens to already be set.  Although we unset the ABTP_HAS_GARBAGE page
	 * level flag, unsetting individual LP_DEAD bits should still be avoided.
	 */
	for (int i = 0; i < nupdatable; i++)
	{
		OffsetNumber updatedoffset = updatedoffsets[i];
		IndexTuple	itup;

		itup = updatable[i]->itup;
		itemsz = MAXALIGN(IndexTupleSize(itup));
		if (!PageIndexTupleOverwrite(page, updatedoffset, (Item) itup,
									 itemsz))
			elog(PANIC, "failed to update partially dead item in block %u of index \"%s\"",
				 BufferGetBlockNumber(buf), RelationGetRelationName(rel));
	}

	/* Now handle simple deletes of entire tuples */
	if (ndeletable > 0)
		PageIndexMultiDelete(page, deletable, ndeletable);

	/*
	 * We can clear the vacuum cycle ID since this page has certainly been
	 * processed by the current vacuum scan.
	 */
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	opaque->abtpo_cycleid = 0;

	/*
	 * Mark the page as not containing any LP_DEAD items.  This is not
	 * certainly true (there might be some that have recently been marked, but
	 * weren't targeted by VACUUM's heap scan), but it will be true often
	 * enough.  VACUUM does not delete items purely because they have their
	 * LP_DEAD bit set, since doing so would necessitate explicitly logging a
	 * latestRemovedXid cutoff (this is how _abt_delitems_delete works).
	 *
	 * The consequences of falsely unsetting ABTP_HAS_GARBAGE should be fairly
	 * limited, since we never falsely unset an LP_DEAD bit.  Workloads that
	 * are particularly dependent on LP_DEAD bits being set quickly will
	 * usually manage to set the ABTP_HAS_GARBAGE flag before the page fills up
	 * again anyway.  Furthermore, attempting a deduplication pass will remove
	 * all LP_DEAD items, regardless of whether the ABTP_HAS_GARBAGE hint bit
	 * is set or not.
	 */
	opaque->abtpo_flags &= ~ABTP_HAS_GARBAGE;

	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		XLogRecPtr	recptr;
		xl_abtree_vacuum xlrec_vacuum;

		xlrec_vacuum.ndeleted = ndeletable;
		xlrec_vacuum.nupdated = nupdatable;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterData((char *) &xlrec_vacuum, SizeOfABtreeVacuum);

		if (agg_info->leaf_has_agg && nupdatable > 0)
		{
			xl_abtree_minimal_agg_info min_agg_info;
			_abt_prepare_min_agg_info(&min_agg_info, agg_info);
			XLogRegisterData((char *) &min_agg_info,
							 SizeOfABtreeMinimalAggInfo);
		}

		if (ndeletable > 0)
			XLogRegisterBufData(0, (char *) deletable,
								ndeletable * sizeof(OffsetNumber));

		if (nupdatable > 0)
		{
			XLogRegisterBufData(0, (char *) updatedoffsets,
								nupdatable * sizeof(OffsetNumber));
			XLogRegisterBufData(0, updatedbuf, updatedbuflen);
		}

		recptr = XLogInsert(RM_ABTREE_ID, XLOG_ABTREE_VACUUM);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/* can't leak memory here */
	if (updatedbuf != NULL)
		pfree(updatedbuf);
	/* free tuples generated by calling _abt_update_posting() */
	for (int i = 0; i < nupdatable; i++)
		pfree(updatable[i]->itup);
}

/*
 * Delete item(s) from a btree leaf page during single-page cleanup.
 *
 * This routine assumes that the caller has pinned and write locked the
 * buffer.  Also, the given deletable array *must* be sorted in ascending
 * order.
 *
 * This is nearly the same as _abt_delitems_vacuum as far as what it does to
 * the page, but it needs to generate its own latestRemovedXid by accessing
 * the heap.  This is used by the REDO routine to generate recovery conflicts.
 * Also, it doesn't handle posting list tuples unless the entire tuple can be
 * deleted as a whole (since there is only one LP_DEAD bit per line pointer).
 */
void
_abt_delitems_delete(Relation rel, Buffer buf,
					OffsetNumber *deletable, int ndeletable,
					Relation heapRel)
{
	Page		page = BufferGetPage(buf);
	ABTPageOpaque opaque;
	TransactionId latestRemovedXid = InvalidTransactionId;

	/* Shouldn't be called unless there's something to do */
	Assert(ndeletable > 0);

	if (XLogStandbyInfoActive() && RelationNeedsWAL(rel))
		latestRemovedXid =
			_abt_xid_horizon(rel, heapRel, page, deletable, ndeletable);

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	/* Fix the page */
	PageIndexMultiDelete(page, deletable, ndeletable);

	/*
	 * Unlike _abt_delitems_vacuum, we *must not* clear the vacuum cycle ID,
	 * because this is not called by VACUUM.  Just clear the ABTP_HAS_GARBAGE
	 * page flag, since we deleted all items with their LP_DEAD bit set.
	 *
	 * Also set ABTP_HAD_GARBAGE_REMOVED flag so that a later vacuum can pick
	 * it up and update the aggregates in the parent node.
	 */
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	opaque->abtpo_flags &= ~ABTP_HAS_GARBAGE;
	opaque->abtpo_flags |= ABTP_HAD_GARBAGE_REMOVED;

	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		XLogRecPtr	recptr;
		xl_abtree_delete xlrec_delete;

		xlrec_delete.latestRemovedXid = latestRemovedXid;
		xlrec_delete.ndeleted = ndeletable;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterData((char *) &xlrec_delete, SizeOfABtreeDelete);

		/*
		 * The deletable array is not in the buffer, but pretend that it is.
		 * When XLogInsert stores the whole buffer, the array need not be
		 * stored too.
		 */
		XLogRegisterBufData(0, (char *) deletable,
							ndeletable * sizeof(OffsetNumber));

		recptr = XLogInsert(RM_ABTREE_ID, XLOG_ABTREE_DELETE);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();
}

/*
 * Get the latestRemovedXid from the table entries pointed to by the non-pivot
 * tuples being deleted.
 *
 * This is a specialized version of index_compute_xid_horizon_for_tuples().
 * It's needed because btree tuples don't always store table TID using the
 * standard index tuple header field.
 */
static TransactionId
_abt_xid_horizon(Relation rel, Relation heapRel, Page page,
				OffsetNumber *deletable, int ndeletable)
{
	TransactionId latestRemovedXid = InvalidTransactionId;
	int			spacenhtids;
	int			nhtids;
	ItemPointer htids;

	/* Array will grow iff there are posting list tuples to consider */
	spacenhtids = ndeletable;
	nhtids = 0;
	htids = (ItemPointer) palloc(sizeof(ItemPointerData) * spacenhtids);
	for (int i = 0; i < ndeletable; i++)
	{
		ItemId		itemid;
		IndexTuple	itup;

		itemid = PageGetItemId(page, deletable[i]);
		itup = (IndexTuple) PageGetItem(page, itemid);

		Assert(ItemIdIsDead(itemid));
		Assert(!ABTreeTupleIsPivot(itup));

		if (!ABTreeTupleIsPosting(itup))
		{
			if (nhtids + 1 > spacenhtids)
			{
				spacenhtids *= 2;
				htids = (ItemPointer)
					repalloc(htids, sizeof(ItemPointerData) * spacenhtids);
			}

			Assert(ItemPointerIsValid(&itup->t_tid));
			ItemPointerCopy(&itup->t_tid, &htids[nhtids]);
			nhtids++;
		}
		else
		{
			int			nposting = ABTreeTupleGetNPosting(itup);

			if (nhtids + nposting > spacenhtids)
			{
				spacenhtids = Max(spacenhtids * 2, nhtids + nposting);
				htids = (ItemPointer)
					repalloc(htids, sizeof(ItemPointerData) * spacenhtids);
			}

			for (int j = 0; j < nposting; j++)
			{
				ItemPointer htid = ABTreeTupleGetPostingN(itup, j);

				Assert(ItemPointerIsValid(htid));
				ItemPointerCopy(htid, &htids[nhtids]);
				nhtids++;
			}
		}
	}

	Assert(nhtids >= ndeletable);

	latestRemovedXid =
		table_compute_xid_horizon_for_tuples(heapRel, htids, nhtids);

	pfree(htids);

	return latestRemovedXid;
}

/*
 * Check that leftsib page (the abtpo_prev of target page) is not marked with
 * INCOMPLETE_SPLIT flag.  Used during page deletion.
 *
 * Returning true indicates that page flag is set in leftsib (which is
 * definitely still the left sibling of target).  When that happens, the
 * target doesn't have a downlink in parent, and the page deletion algorithm
 * isn't prepared to handle that.  Deletion of the target page (or the whole
 * subtree that contains the target page) cannot take place.
 *
 * Caller should not have a lock on the target page itself, since pages on the
 * same level must always be locked left to right to avoid deadlocks.
 */
static bool
_abt_leftsib_splitflag(Relation rel, BlockNumber leftsib, BlockNumber target)
{
	Buffer		buf;
	Page		page;
	ABTPageOpaque opaque;
	bool		result;

	/* Easy case: No left sibling */
	if (leftsib == ABTP_NONE)
		return false;

	buf = _abt_getbuf(rel, leftsib, ABT_READ);
	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	/*
	 * If the left sibling was concurrently split, so that its next-pointer
	 * doesn't point to the current page anymore, the split that created
	 * target must be completed.  Caller can reasonably expect that there will
	 * be a downlink to the target page that it can relocate using its stack.
	 * (We don't allow splitting an incompletely split page again until the
	 * previous split has been completed.)
	 */
	result = (opaque->abtpo_next == target && ABT_P_INCOMPLETE_SPLIT(opaque));
	_abt_relbuf(rel, buf);

	return result;
}

/*
 * Check that leafrightsib page (the abtpo_next of target leaf page) is not
 * marked with ISHALFDEAD flag.  Used during page deletion.
 *
 * Returning true indicates that page flag is set in leafrightsib, so page
 * deletion cannot go ahead.  Our caller is not prepared to deal with the case
 * where the parent page does not have a pivot tuples whose downlink points to
 * leafrightsib (due to an earlier interrupted VACUUM operation).  It doesn't
 * seem worth going to the trouble of teaching our caller to deal with it.
 * The situation will be resolved after VACUUM finishes the deletion of the
 * half-dead page (when a future VACUUM operation reaches the target page
 * again).
 *
 * _abt_leftsib_splitflag() is called for both leaf pages and internal pages.
 * _abt_rightsib_halfdeadflag() is only called for leaf pages, though.  This is
 * okay because of the restriction on deleting pages that are the rightmost
 * page of their parent (i.e. that such deletions can only take place when the
 * entire subtree must be deleted).  The leaf level check made here will apply
 * to a right "cousin" leaf page rather than a simple right sibling leaf page
 * in cases where caller actually goes on to attempt deleting pages that are
 * above the leaf page.  The right cousin leaf page is representative of the
 * left edge of the subtree to the right of the to-be-deleted subtree as a
 * whole, which is exactly the condition that our caller cares about.
 * (Besides, internal pages are never marked half-dead, so it isn't even
 * possible to _directly_ assess if an internal page is part of some other
 * to-be-deleted subtree.)
 */
static bool
_abt_rightsib_halfdeadflag(Relation rel, BlockNumber leafrightsib)
{
	Buffer		buf;
	Page		page;
	ABTPageOpaque opaque;
	bool		result;

	Assert(leafrightsib != ABTP_NONE);

	buf = _abt_getbuf(rel, leafrightsib, ABT_READ);
	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	Assert(ABT_P_ISLEAF(opaque) && !ABT_P_ISDELETED(opaque));
	result = ABT_P_ISHALFDEAD(opaque);
	_abt_relbuf(rel, buf);

	return result;
}

/*
 * _abt_pagedel() -- Delete a leaf page from the b-tree, if legal to do so.
 *
 * This action unlinks the leaf page from the b-tree structure, removing all
 * pointers leading to it --- but not touching its own left and right links.
 * The page cannot be physically reclaimed right away, since other processes
 * may currently be trying to follow links leading to the page; they have to
 * be allowed to use its right-link to recover.  See nbtree/README.
 *
 * On entry, the target buffer must be pinned and locked (either read or write
 * lock is OK).  The page must be an empty leaf page, which may be half-dead
 * already (a half-dead page should only be passed to us when an earlier
 * VACUUM operation was interrupted, though).  Note in particular that caller
 * should never pass a buffer containing an existing deleted page here.  The
 * lock and pin on caller's buffer will be dropped before we return.
 *
 * Returns the number of pages successfully deleted (zero if page cannot
 * be deleted now; could be more than one if parent or right sibling pages
 * were deleted too).  Note that this does not include pages that we delete
 * that the btvacuumscan scan has yet to reach; they'll get counted later
 * instead.
 *
 * Maintains *oldestBtpoXact for any pages that get deleted.  Caller is
 * responsible for maintaining *oldestBtpoXact in the case of pages that were
 * deleted by a previous VACUUM.
 *
 * NOTE: this leaks memory.  Rather than trying to clean up everything
 * carefully, it's better to run it in a temp context that can be reset
 * frequently.
 */
uint32
_abt_pagedel(Relation rel, Buffer leafbuf, TransactionId *oldestBtpoXact,
			 ABTCachedAggInfo agg_info)
{
	uint32		ndeleted = 0;
	BlockNumber rightsib;
	bool		rightsib_empty;
	Page		page;
	ABTPageOpaque opaque;

	/*
	 * Save original leafbuf block number from caller.  Only deleted blocks
	 * that are <= scanblkno get counted in ndeleted return value.
	 */
	BlockNumber scanblkno = BufferGetBlockNumber(leafbuf);

	/*
	 * "stack" is a search stack leading (approximately) to the target page.
	 * It is initially NULL, but when iterating, we keep it to avoid
	 * duplicated search effort.
	 *
	 * Also, when "stack" is not NULL, we have already checked that the
	 * current page is not the right half of an incomplete split, i.e. the
	 * left sibling does not have its INCOMPLETE_SPLIT flag set, including
	 * when the current target page is to the right of caller's initial page
	 * (the scanblkno page).
	 */
	ABTStack		stack = NULL;

	for (;;)
	{
		page = BufferGetPage(leafbuf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

		/*
		 * Internal pages are never deleted directly, only as part of deleting
		 * the whole subtree all the way down to leaf level.
		 *
		 * Also check for deleted pages here.  Caller never passes us a fully
		 * deleted page.  Only VACUUM can delete pages, so there can't have
		 * been a concurrent deletion.  Assume that we reached any deleted
		 * page encountered here by following a sibling link, and that the
		 * index is corrupt.
		 */
		Assert(!ABT_P_ISDELETED(opaque));
		if (!ABT_P_ISLEAF(opaque) || ABT_P_ISDELETED(opaque))
		{
			/*
			 * Pre-9.4 page deletion only marked internal pages as half-dead,
			 * but now we only use that flag on leaf pages. The old algorithm
			 * was never supposed to leave half-dead pages in the tree, it was
			 * just a transient state, but it was nevertheless possible in
			 * error scenarios. We don't know how to deal with them here. They
			 * are harmless as far as searches are considered, but inserts
			 * into the deleted keyspace could add out-of-order downlinks in
			 * the upper levels. Log a notice, hopefully the admin will notice
			 * and reindex.
			 */
			if (ABT_P_ISHALFDEAD(opaque))
				ereport(LOG,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("index \"%s\" contains a half-dead internal page",
								RelationGetRelationName(rel)),
						 errhint("This can be caused by an interrupted VACUUM in version 9.3 or older, before upgrade. Please REINDEX it.")));

			if (ABT_P_ISDELETED(opaque))
				ereport(LOG,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg_internal("found deleted block %u while following right link from block %u in index \"%s\"",
										 BufferGetBlockNumber(leafbuf),
										 scanblkno,
										 RelationGetRelationName(rel))));

			_abt_relbuf(rel, leafbuf);
			return ndeleted;
		}

		/*
		 * We can never delete rightmost pages nor root pages.  While at it,
		 * check that page is empty, since it's possible that the leafbuf page
		 * was empty a moment ago, but has since had some inserts.
		 *
		 * To keep the algorithm simple, we also never delete an incompletely
		 * split page (they should be rare enough that this doesn't make any
		 * meaningful difference to disk usage):
		 *
		 * The INCOMPLETE_SPLIT flag on the page tells us if the page is the
		 * left half of an incomplete split, but ensuring that it's not the
		 * right half is more complicated.  For that, we have to check that
		 * the left sibling doesn't have its INCOMPLETE_SPLIT flag set using
		 * _abt_leftsib_splitflag().  On the first iteration, we temporarily
		 * release the lock on scanblkno/leafbuf, check the left sibling, and
		 * construct a search stack to scanblkno.  On subsequent iterations,
		 * we know we stepped right from a page that passed these tests, so
		 * it's OK.
		 */
		if (ABT_P_RIGHTMOST(opaque) || ABT_P_ISROOT(opaque) ||
			ABT_P_FIRSTDATAKEY(opaque) <= PageGetMaxOffsetNumber(page) ||
			ABT_P_INCOMPLETE_SPLIT(opaque))
		{
			/* Should never fail to delete a half-dead page */
			Assert(!ABT_P_ISHALFDEAD(opaque));

			_abt_relbuf(rel, leafbuf);
			return ndeleted;
		}

		/*
		 * First, remove downlink pointing to the page (or a parent of the
		 * page, if we are going to delete a taller subtree), and mark the
		 * leafbuf page half-dead
		 */
		if (!ABT_P_ISHALFDEAD(opaque))
		{
			/*
			 * We need an approximate pointer to the page's parent page.  We
			 * use a variant of the standard search mechanism to search for
			 * the page's high key; this will give us a link to either the
			 * current parent or someplace to its left (if there are multiple
			 * equal high keys, which is possible with !heapkeyspace indexes).
			 *
			 * Also check if this is the right-half of an incomplete split
			 * (see comment above).
			 */
			if (!stack)
			{
				ABTScanInsert itup_key;
				ItemId		itemid;
				IndexTuple	targetkey;
				BlockNumber leftsib,
							leafblkno;
				Buffer		sleafbuf;

				itemid = PageGetItemId(page, ABTP_HIKEY);
				targetkey = CopyIndexTuple((IndexTuple) PageGetItem(page, itemid));

				leftsib = opaque->abtpo_prev;
				leafblkno = BufferGetBlockNumber(leafbuf);

				/*
				 * To avoid deadlocks, we'd better drop the leaf page lock
				 * before going further.
				 */
				LockBuffer(leafbuf, BUFFER_LOCK_UNLOCK);

				/*
				 * Check that the left sibling of leafbuf (if any) is not
				 * marked with INCOMPLETE_SPLIT flag before proceeding
				 */
				Assert(leafblkno == scanblkno);
				if (_abt_leftsib_splitflag(rel, leftsib, leafblkno))
				{
					ReleaseBuffer(leafbuf);
					return ndeleted;
				}

				/* we need an insertion scan key for the search, so build one */
				itup_key = _abt_mkscankey(rel, targetkey);
				/* find the leftmost leaf page with matching pivot/high key */
				itup_key->pivotsearch = true;
				stack = _abt_search(rel, itup_key, &sleafbuf, ABT_READ, NULL,
									agg_info);
				/* won't need a second lock or pin on leafbuf */
				_abt_relbuf(rel, sleafbuf);

				/*
				 * Re-lock the leaf page, and start over to use our stack
				 * within _abt_mark_page_halfdead.  We must do it that way
				 * because it's possible that leafbuf can no longer be
				 * deleted.  We need to recheck.
				 *
				 * Note: We can't simply hold on to the sleafbuf lock instead,
				 * because it's barely possible that sleafbuf is not the same
				 * page as leafbuf.  This happens when leafbuf split after our
				 * original lock was dropped, but before _abt_search finished
				 * its descent.  We rely on the assumption that we'll find
				 * leafbuf isn't safe to delete anymore in this scenario.
				 * (Page deletion can cope with the stack being to the left of
				 * leafbuf, but not to the right of leafbuf.)
				 */
				LockBuffer(leafbuf, ABT_WRITE);
				continue;
			}

			/*
			 * See if it's safe to delete the leaf page, and determine how
			 * many parent/internal pages above the leaf level will be
			 * deleted.  If it's safe then _abt_mark_page_halfdead will also
			 * perform the first phase of deletion, which includes marking the
			 * leafbuf page half-dead.
			 */
			Assert(ABT_P_ISLEAF(opaque) && !ABT_P_IGNORE(opaque));
			if (!_abt_mark_page_halfdead(rel, leafbuf, stack, agg_info))
			{
				_abt_relbuf(rel, leafbuf);
				return ndeleted;
			}
		}

		/*
		 * Then unlink it from its siblings.  Each call to
		 * _abt_unlink_halfdead_page unlinks the topmost page from the subtree,
		 * making it shallower.  Iterate until the leafbuf page is deleted.
		 *
		 * _abt_unlink_halfdead_page should never fail, since we established
		 * that deletion is generally safe in _abt_mark_page_halfdead.
		 */
		rightsib_empty = false;
		Assert(ABT_P_ISLEAF(opaque) && ABT_P_ISHALFDEAD(opaque));
		while (ABT_P_ISHALFDEAD(opaque))
		{
			/* Check for interrupts in _abt_unlink_halfdead_page */
			if (!_abt_unlink_halfdead_page(rel, leafbuf, scanblkno,
										  &rightsib_empty, oldestBtpoXact,
										  &ndeleted))
			{
				/* _abt_unlink_halfdead_page failed, released buffer */
				return ndeleted;
			}
		}

		Assert(ABT_P_ISLEAF(opaque) && ABT_P_ISDELETED(opaque));
		Assert(TransactionIdFollowsOrEquals(opaque->abtpo.xact,
											*oldestBtpoXact));

		rightsib = opaque->abtpo_next;

		_abt_relbuf(rel, leafbuf);

		/*
		 * Check here, as calling loops will have locks held, preventing
		 * interrupts from being processed.
		 */
		CHECK_FOR_INTERRUPTS();

		/*
		 * The page has now been deleted. If its right sibling is completely
		 * empty, it's possible that the reason we haven't deleted it earlier
		 * is that it was the rightmost child of the parent. Now that we
		 * removed the downlink for this page, the right sibling might now be
		 * the only child of the parent, and could be removed. It would be
		 * picked up by the next vacuum anyway, but might as well try to
		 * remove it now, so loop back to process the right sibling.
		 *
		 * Note: This relies on the assumption that _abt_getstackbuf() will be
		 * able to reuse our original descent stack with a different child
		 * block (provided that the child block is to the right of the
		 * original leaf page reached by _abt_search()). It will even update
		 * the descent stack each time we loop around, avoiding repeated work.
		 */
		if (!rightsib_empty)
			break;

		leafbuf = _abt_getbuf(rel, rightsib, ABT_WRITE);
	}

	return ndeleted;
}

/*
 * First stage of page deletion.
 *
 * Establish the height of the to-be-deleted subtree with leafbuf at its
 * lowest level, remove the downlink to the subtree, and mark leafbuf
 * half-dead.  The final to-be-deleted subtree is usually just leafbuf itself,
 * but may include additional internal pages (at most one per level of the
 * tree below the root).
 *
 * Returns 'false' if leafbuf is unsafe to delete, usually because leafbuf is
 * the rightmost child of its parent (and parent has more than one downlink).
 * Returns 'true' when the first stage of page deletion completed
 * successfully.
 */
static bool
_abt_mark_page_halfdead(Relation rel, Buffer leafbuf, ABTStack stack,
						ABTCachedAggInfo agg_info)
{
	BlockNumber leafblkno;
	BlockNumber leafrightsib;
	BlockNumber topparent;
	BlockNumber topparentrightsib;
	ItemId		itemid;
	Page		page;
	ABTPageOpaque opaque;
	Buffer		subtreeparent;
	OffsetNumber poffset;
	OffsetNumber nextoffset;
	IndexTuple	itup;
	IndexTupleData trunctuple;
	Pointer		right_agg_ptr;
	Pointer		agg_ptr;

	page = BufferGetPage(leafbuf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	Assert(!ABT_P_RIGHTMOST(opaque) && !ABT_P_ISROOT(opaque) &&
		   ABT_P_ISLEAF(opaque) && !ABT_P_IGNORE(opaque) &&
		   ABT_P_FIRSTDATAKEY(opaque) > PageGetMaxOffsetNumber(page));

	/*
	 * Save info about the leaf page.
	 */
	leafblkno = BufferGetBlockNumber(leafbuf);
	leafrightsib = opaque->abtpo_next;

	/*
	 * Before attempting to lock the parent page, check that the right sibling
	 * is not in half-dead state.  A half-dead right sibling would have no
	 * downlink in the parent, which would be highly confusing later when we
	 * delete the downlink.  It would fail the "right sibling of target page
	 * is also the next child in parent page" cross-check below.
	 */
	if (_abt_rightsib_halfdeadflag(rel, leafrightsib))
	{
		elog(DEBUG1, "could not delete page %u because its right sibling %u is half-dead",
			 leafblkno, leafrightsib);
		return false;
	}

	/*
	 * We cannot delete a page that is the rightmost child of its immediate
	 * parent, unless it is the only child --- in which case the parent has to
	 * be deleted too, and the same condition applies recursively to it. We
	 * have to check this condition all the way up before trying to delete,
	 * and lock the parent of the root of the to-be-deleted subtree (the
	 * "subtree parent").  _abt_lock_subtree_parent() locks the subtree parent
	 * for us.  We remove the downlink to the "top parent" page (subtree root
	 * page) from the subtree parent page below.
	 *
	 * Initialize topparent to be leafbuf page now.  The final to-be-deleted
	 * subtree is often a degenerate one page subtree consisting only of the
	 * leafbuf page.  When that happens, the leafbuf page is the final subtree
	 * root page/top parent page.
	 */
	topparent = leafblkno;
	topparentrightsib = leafrightsib;
	if (!_abt_lock_subtree_parent(rel, leafblkno, stack,
								 &subtreeparent, &poffset,
								 &topparent, &topparentrightsib,
								 agg_info))
		return false;

	/*
	 * Check that the parent-page index items we're about to delete/overwrite
	 * in subtree parent page contain what we expect.  This can fail if the
	 * index has become corrupt for some reason.  We want to throw any error
	 * before entering the critical section --- otherwise it'd be a PANIC.
	 */
	page = BufferGetPage(subtreeparent);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

#ifdef USE_ASSERT_CHECKING

	/*
	 * This is just an assertion because _abt_lock_subtree_parent should have
	 * guaranteed tuple has the expected contents
	 */
	itemid = PageGetItemId(page, poffset);
	itup = (IndexTuple) PageGetItem(page, itemid);
	Assert(ABTreeTupleGetDownLink(itup) == topparent);
#endif

	nextoffset = OffsetNumberNext(poffset);
	itemid = PageGetItemId(page, nextoffset);
	itup = (IndexTuple) PageGetItem(page, itemid);
	if (ABTreeTupleGetDownLink(itup) != topparentrightsib)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg_internal("right sibling %u of block %u is not next child %u of block %u in index \"%s\"",
								 topparentrightsib, topparent,
								 ABTreeTupleGetDownLink(itup),
								 BufferGetBlockNumber(subtreeparent),
								 RelationGetRelationName(rel))));

	right_agg_ptr = ABTreePivotTupleGetAggPtr(itup, agg_info);

	/*
	 * Any insert which would have gone on the leaf block will now go to its
	 * right sibling.  In other words, the key space moves right.
	 */
	PredicateLockPageCombine(rel, leafblkno, leafrightsib);

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	/*
	 * Update parent of subtree.  We want to delete the downlink to the top
	 * parent page/root of the subtree, and the *following* key.  Easiest way
	 * is to copy the right sibling's downlink over the downlink that points to
	 * top parent page, as well as overwriting its associated aggregation with
	 * the value in the right sibling's index tuple, and then delete the right
	 * sibling's original pivot tuple.
	 *
	 * Lanin and Shasha make the key space move left when deleting a page,
	 * whereas the key space moves right here.  That's why we cannot simply
	 * delete the pivot tuple with the downlink to the top parent page.  See
	 * nbtree/README.
	 */
	page = BufferGetPage(subtreeparent);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	itemid = PageGetItemId(page, poffset);
	itup = (IndexTuple) PageGetItem(page, itemid);
	ABTreeTupleSetDownLink(itup, topparentrightsib);
	agg_ptr = ABTreePivotTupleGetAggPtr(itup, agg_info);
	memcpy(agg_ptr, right_agg_ptr, agg_info->agg_typlen);

	nextoffset = OffsetNumberNext(poffset);
	PageIndexTupleDelete(page, nextoffset);

    /* 
     * Note: we do not update the last update ID here because the keys on this
     * page could not have been moved to elsewhere (since there's no merge).
     */

	/*
	 * Mark the leaf page as half-dead, and stamp it with a link to the top
	 * parent page.  When the leaf page is also the top parent page, the link
	 * is set to InvalidBlockNumber.
	 */
	page = BufferGetPage(leafbuf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	opaque->abtpo_flags |= ABTP_HALF_DEAD;

	Assert(PageGetMaxOffsetNumber(page) == ABTP_HIKEY);
	MemSet(&trunctuple, 0, sizeof(IndexTupleData));
	trunctuple.t_info = sizeof(IndexTupleData);
	if (topparent != leafblkno)
		ABTreeTupleSetTopParent(&trunctuple, topparent);
	else
		ABTreeTupleSetTopParent(&trunctuple, InvalidBlockNumber);

	if (!PageIndexTupleOverwrite(page, ABTP_HIKEY, (Item) &trunctuple,
								 IndexTupleSize(&trunctuple)))
		elog(ERROR, "could not overwrite high key in half-dead page");

	/* Must mark buffers dirty before XLogInsert */
	MarkBufferDirty(subtreeparent);
	MarkBufferDirty(leafbuf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_abtree_mark_page_halfdead xlrec;
		XLogRecPtr	recptr;

		xlrec.poffset = poffset;
		xlrec.leafblk = leafblkno;
		if (topparent != leafblkno)
			xlrec.topparent = topparent;
		else
			xlrec.topparent = InvalidBlockNumber;

		XLogBeginInsert();
		XLogRegisterBuffer(0, leafbuf, REGBUF_WILL_INIT);
		XLogRegisterBuffer(1, subtreeparent, REGBUF_STANDARD);

		page = BufferGetPage(leafbuf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		xlrec.leftblk = opaque->abtpo_prev;
		xlrec.rightblk = opaque->abtpo_next;

		XLogRegisterData((char *) &xlrec, SizeOfABtreeMarkPageHalfDead);

		recptr = XLogInsert(RM_ABTREE_ID, XLOG_ABTREE_MARK_PAGE_HALFDEAD);

		page = BufferGetPage(subtreeparent);
		PageSetLSN(page, recptr);
		page = BufferGetPage(leafbuf);
		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	_abt_relbuf(rel, subtreeparent);
	return true;
}

/*
 * Second stage of page deletion.
 *
 * Unlinks a single page (in the subtree undergoing deletion) from its
 * siblings.  Also marks the page deleted.
 *
 * To get rid of the whole subtree, including the leaf page itself, call here
 * until the leaf page is deleted.  The original "top parent" established in
 * the first stage of deletion is deleted in the first call here, while the
 * leaf page is deleted in the last call here.  Note that the leaf page itself
 * is often the initial top parent page.
 *
 * Returns 'false' if the page could not be unlinked (shouldn't happen).  If
 * the right sibling of the current target page is empty, *rightsib_empty is
 * set to true, allowing caller to delete the target's right sibling page in
 * passing.  Note that *rightsib_empty is only actually used by caller when
 * target page is leafbuf, following last call here for leafbuf/the subtree
 * containing leafbuf.  (We always set *rightsib_empty for caller, just to be
 * consistent.)
 *
 * We maintain *oldestBtpoXact for pages that are deleted by the current
 * VACUUM operation here.  This must be handled here because we conservatively
 * assume that there needs to be a new call to ReadNewTransactionId() each
 * time a page gets deleted.  See comments about the underlying assumption
 * below.
 *
 * Must hold pin and lock on leafbuf at entry (read or write doesn't matter).
 * On success exit, we'll be holding pin and write lock.  On failure exit,
 * we'll release both pin and lock before returning (we define it that way
 * to avoid having to reacquire a lock we already released).
 */
static bool
_abt_unlink_halfdead_page(Relation rel, Buffer leafbuf, BlockNumber scanblkno,
						 bool *rightsib_empty, TransactionId *oldestBtpoXact,
						 uint32 *ndeleted)
{
	BlockNumber leafblkno = BufferGetBlockNumber(leafbuf);
	BlockNumber leafleftsib;
	BlockNumber leafrightsib;
	BlockNumber target;
	BlockNumber leftsib;
	BlockNumber rightsib;
	Buffer		lbuf = InvalidBuffer;
	Buffer		buf;
	Buffer		rbuf;
	Buffer		metabuf = InvalidBuffer;
	Page		metapg = NULL;
	ABTMetaPageData *metad = NULL;
	ItemId		itemid;
	Page		page;
	ABTPageOpaque opaque;
	bool		rightsib_is_rightmost;
	int			targetlevel;
	IndexTuple	leafhikey;
	BlockNumber nextchild;

	page = BufferGetPage(leafbuf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	Assert(ABT_P_ISLEAF(opaque) && !ABT_P_ISDELETED(opaque) && ABT_P_ISHALFDEAD(opaque));

	/*
	 * Remember some information about the leaf page.
	 */
	itemid = PageGetItemId(page, ABTP_HIKEY);
	leafhikey = (IndexTuple) PageGetItem(page, itemid);
	target = ABTreeTupleGetTopParent(leafhikey);
	leafleftsib = opaque->abtpo_prev;
	leafrightsib = opaque->abtpo_next;

	LockBuffer(leafbuf, BUFFER_LOCK_UNLOCK);

	/*
	 * Check here, as calling loops will have locks held, preventing
	 * interrupts from being processed.
	 */
	CHECK_FOR_INTERRUPTS();

	/* Unlink the current top parent of the subtree */
	if (!BlockNumberIsValid(target))
	{
		/* Target is leaf page (or leaf page is top parent, if you prefer) */
		target = leafblkno;

		buf = leafbuf;
		leftsib = leafleftsib;
		targetlevel = 0;
	}
	else
	{
		/* Target is the internal page taken from leaf's top parent link */
		Assert(target != leafblkno);

		/* Fetch the block number of the target's left sibling */
		buf = _abt_getbuf(rel, target, ABT_READ);
		page = BufferGetPage(buf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		leftsib = opaque->abtpo_prev;
		targetlevel = opaque->abtpo.level;
		Assert(targetlevel > 0);

		/*
		 * To avoid deadlocks, we'd better drop the target page lock before
		 * going further.
		 */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	}

	/*
	 * We have to lock the pages we need to modify in the standard order:
	 * moving right, then up.  Else we will deadlock against other writers.
	 *
	 * So, first lock the leaf page, if it's not the target.  Then find and
	 * write-lock the current left sibling of the target page.  The sibling
	 * that was current a moment ago could have split, so we may have to move
	 * right.  This search could fail if either the sibling or the target page
	 * was deleted by someone else meanwhile; if so, give up.  (Right now,
	 * that should never happen, since page deletion is only done in VACUUM
	 * and there shouldn't be multiple VACUUMs concurrently on the same
	 * table.)
	 */
	if (target != leafblkno)
		LockBuffer(leafbuf, ABT_WRITE);
	if (leftsib != ABTP_NONE)
	{
		lbuf = _abt_getbuf(rel, leftsib, ABT_WRITE);
		page = BufferGetPage(lbuf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		while (ABT_P_ISDELETED(opaque) || opaque->abtpo_next != target)
		{
			/* step right one page */
			leftsib = opaque->abtpo_next;
			_abt_relbuf(rel, lbuf);

			/*
			 * It'd be good to check for interrupts here, but it's not easy to
			 * do so because a lock is always held. This block isn't
			 * frequently reached, so hopefully the consequences of not
			 * checking interrupts aren't too bad.
			 */

			if (leftsib == ABTP_NONE)
			{
				elog(LOG, "no left sibling (concurrent deletion?) of block %u in \"%s\"",
					 target,
					 RelationGetRelationName(rel));
				if (target != leafblkno)
				{
					/* we have only a pin on target, but pin+lock on leafbuf */
					ReleaseBuffer(buf);
					_abt_relbuf(rel, leafbuf);
				}
				else
				{
					/* we have only a pin on leafbuf */
					ReleaseBuffer(leafbuf);
				}
				return false;
			}
			lbuf = _abt_getbuf(rel, leftsib, ABT_WRITE);
			page = BufferGetPage(lbuf);
			opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		}
	}
	else
		lbuf = InvalidBuffer;

	/*
	 * Next write-lock the target page itself.  It's okay to take a write lock
	 * rather than a superexclusive lock, since no scan will stop on an empty
	 * page.
	 */
	LockBuffer(buf, ABT_WRITE);
	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

	/*
	 * Check page is still empty etc, else abandon deletion.  This is just for
	 * paranoia's sake; a half-dead page cannot resurrect because there can be
	 * only one vacuum process running at a time.
	 */
	if (ABT_P_RIGHTMOST(opaque) || ABT_P_ISROOT(opaque) || ABT_P_ISDELETED(opaque))
		elog(ERROR, "half-dead page changed status unexpectedly in block %u of index \"%s\"",
			 target, RelationGetRelationName(rel));

	if (opaque->abtpo_prev != leftsib)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg_internal("left link changed unexpectedly in block %u of index \"%s\"",
								 target, RelationGetRelationName(rel))));

	if (target == leafblkno)
	{
		if (ABT_P_FIRSTDATAKEY(opaque) <= PageGetMaxOffsetNumber(page) ||
			!ABT_P_ISLEAF(opaque) || !ABT_P_ISHALFDEAD(opaque))
			elog(ERROR, "half-dead page changed status unexpectedly in block %u of index \"%s\"",
				 target, RelationGetRelationName(rel));
		nextchild = InvalidBlockNumber;
	}
	else
	{
		if (ABT_P_FIRSTDATAKEY(opaque) != PageGetMaxOffsetNumber(page) ||
			ABT_P_ISLEAF(opaque))
			elog(ERROR, "half-dead page changed status unexpectedly in block %u of index \"%s\"",
				 target, RelationGetRelationName(rel));

		/* Remember the next non-leaf child down in the subtree */
		itemid = PageGetItemId(page, ABT_P_FIRSTDATAKEY(opaque));
		nextchild = ABTreeTupleGetDownLink((IndexTuple) PageGetItem(page, itemid));
		if (nextchild == leafblkno)
			nextchild = InvalidBlockNumber;
	}

	/*
	 * And next write-lock the (current) right sibling.
	 */
	rightsib = opaque->abtpo_next;
	rbuf = _abt_getbuf(rel, rightsib, ABT_WRITE);
	page = BufferGetPage(rbuf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	if (opaque->abtpo_prev != target)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg_internal("right sibling's left-link doesn't match: "
								 "block %u links to %u instead of expected %u in index \"%s\"",
								 rightsib, opaque->abtpo_prev, target,
								 RelationGetRelationName(rel))));
	rightsib_is_rightmost = ABT_P_RIGHTMOST(opaque);
	*rightsib_empty = (ABT_P_FIRSTDATAKEY(opaque) > PageGetMaxOffsetNumber(page));

	/*
	 * If we are deleting the next-to-last page on the target's level, then
	 * the rightsib is a candidate to become the new fast root. (In theory, it
	 * might be possible to push the fast root even further down, but the odds
	 * of doing so are slim, and the locking considerations daunting.)
	 *
	 * We can safely acquire a lock on the metapage here --- see comments for
	 * _abt_newroot().
	 */
	if (leftsib == ABTP_NONE && rightsib_is_rightmost)
	{
		page = BufferGetPage(rbuf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		if (ABT_P_RIGHTMOST(opaque))
		{
			/* rightsib will be the only one left on the level */
			metabuf = _abt_getbuf(rel, ABTREE_METAPAGE, ABT_WRITE);
			metapg = BufferGetPage(metabuf);
			metad = ABTPageGetMeta(metapg);

			/*
			 * The expected case here is abtm_fastlevel == targetlevel+1; if
			 * the fastlevel is <= targetlevel, something is wrong, and we
			 * choose to overwrite it to fix it.
			 */
			if (metad->abtm_fastlevel > targetlevel + 1)
			{
				/* no update wanted */
				_abt_relbuf(rel, metabuf);
				metabuf = InvalidBuffer;
			}
		}
	}

	/*
	 * Here we begin doing the deletion.
	 */

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	/*
	 * Update siblings' side-links.  Note the target page's side-links will
	 * continue to point to the siblings.  Asserts here are just rechecking
	 * things we already verified above.
	 */
	if (BufferIsValid(lbuf))
	{
		page = BufferGetPage(lbuf);
		opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
		Assert(opaque->abtpo_next == target);
		opaque->abtpo_next = rightsib;
	}
	page = BufferGetPage(rbuf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	Assert(opaque->abtpo_prev == target);
	opaque->abtpo_prev = leftsib;

	/*
	 * If we deleted a parent of the targeted leaf page, instead of the leaf
	 * itself, update the leaf to point to the next remaining child in the
	 * subtree.
	 *
	 * Note: We rely on the fact that a buffer pin on the leaf page has been
	 * held since leafhikey was initialized.  This is safe, though only
	 * because the page was already half-dead at that point.  The leaf page
	 * cannot have been modified by any other backend during the period when
	 * no lock was held.
	 */
	if (target != leafblkno)
		ABTreeTupleSetTopParent(leafhikey, nextchild);

	/*
	 * Mark the page itself deleted.  It can be recycled when all current
	 * transactions are gone.  Storing GetTopTransactionId() would work, but
	 * we're in VACUUM and would not otherwise have an XID.  Having already
	 * updated links to the target, ReadNewTransactionId() suffices as an
	 * upper bound.  Any scan having retained a now-stale link is advertising
	 * in its PGXACT an xmin less than or equal to the value we read here.  It
	 * will continue to do so, holding back RecentGlobalXmin, for the duration
	 * of that scan.
	 */
	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	Assert(ABT_P_ISHALFDEAD(opaque) || !ABT_P_ISLEAF(opaque));
	opaque->abtpo_flags &= ~ABTP_HALF_DEAD;
	opaque->abtpo_flags |= ABTP_DELETED;
	opaque->abtpo.xact = ReadNewTransactionId();

	/* And update the metapage, if needed */
	if (BufferIsValid(metabuf))
	{
		/* upgrade metapage if needed */
		if (metad->abtm_version < ABTREE_NOVAC_VERSION)
			_abt_upgrademetapage(metapg);
		metad->abtm_fastroot = rightsib;
		metad->abtm_fastlevel = targetlevel;
		MarkBufferDirty(metabuf);
	}

	/* Must mark buffers dirty before XLogInsert */
	MarkBufferDirty(rbuf);
	MarkBufferDirty(buf);
	if (BufferIsValid(lbuf))
		MarkBufferDirty(lbuf);
	if (target != leafblkno)
		MarkBufferDirty(leafbuf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_abtree_unlink_page xlrec;
		xl_abtree_metadata xlmeta;
		uint8		xlinfo;
		XLogRecPtr	recptr;

		XLogBeginInsert();

		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
		if (BufferIsValid(lbuf))
			XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
		XLogRegisterBuffer(2, rbuf, REGBUF_STANDARD);
		if (target != leafblkno)
			XLogRegisterBuffer(3, leafbuf, REGBUF_WILL_INIT);

		/* information on the unlinked block */
		xlrec.leftsib = leftsib;
		xlrec.rightsib = rightsib;
		xlrec.abtpo_xact = opaque->abtpo.xact;

		/* information needed to recreate the leaf block (if not the target) */
		xlrec.leafleftsib = leafleftsib;
		xlrec.leafrightsib = leafrightsib;
		xlrec.topparent = nextchild;

		XLogRegisterData((char *) &xlrec, SizeOfABtreeUnlinkPage);

		if (BufferIsValid(metabuf))
		{
			ABTAggSupport agg_support;
			agg_support = ABTMetaPageGetAggSupport(metad);

			XLogRegisterBuffer(4, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

			Assert(metad->abtm_version >= ABTREE_NOVAC_VERSION);
			xlmeta.version = metad->abtm_version;
			xlmeta.root = metad->abtm_root;
			xlmeta.level = metad->abtm_level;
			xlmeta.fastroot = metad->abtm_fastroot;
			xlmeta.fastlevel = metad->abtm_fastlevel;
			xlmeta.oldest_abtpo_xact = metad->abtm_oldest_abtpo_xact;
			xlmeta.last_cleanup_num_heap_tuples = metad->abtm_last_cleanup_num_heap_tuples;
			xlmeta.allequalimage = metad->abtm_allequalimage;

			XLogRegisterBufData(4, (char *) &xlmeta, sizeof(xl_abtree_metadata));
			XLogRegisterBufData(4, (char *) agg_support,
				ABTASGetStructSize(agg_support));
			xlinfo = XLOG_ABTREE_UNLINK_PAGE_META;
		}
		else
			xlinfo = XLOG_ABTREE_UNLINK_PAGE;

		recptr = XLogInsert(RM_ABTREE_ID, xlinfo);

		if (BufferIsValid(metabuf))
		{
			PageSetLSN(metapg, recptr);
		}
		page = BufferGetPage(rbuf);
		PageSetLSN(page, recptr);
		page = BufferGetPage(buf);
		PageSetLSN(page, recptr);
		if (BufferIsValid(lbuf))
		{
			page = BufferGetPage(lbuf);
			PageSetLSN(page, recptr);
		}
		if (target != leafblkno)
		{
			page = BufferGetPage(leafbuf);
			PageSetLSN(page, recptr);
		}
	}

	END_CRIT_SECTION();

	/* release metapage */
	if (BufferIsValid(metabuf))
		_abt_relbuf(rel, metabuf);

	/* release siblings */
	if (BufferIsValid(lbuf))
		_abt_relbuf(rel, lbuf);
	_abt_relbuf(rel, rbuf);

	if (!TransactionIdIsValid(*oldestBtpoXact) ||
		TransactionIdPrecedes(opaque->abtpo.xact, *oldestBtpoXact))
		*oldestBtpoXact = opaque->abtpo.xact;

	/*
	 * If btvacuumscan won't revisit this page in a future btvacuumpage call
	 * and count it as deleted then, we count it as deleted by current
	 * btvacuumpage call
	 */
	if (target <= scanblkno)
		(*ndeleted)++;

	/* If the target is not leafbuf, we're done with it now -- release it */
	if (target != leafblkno)
		_abt_relbuf(rel, buf);

	return true;
}

/*
 * Establish how tall the to-be-deleted subtree will be during the first stage
 * of page deletion.
 *
 * Caller's child argument is the block number of the page caller wants to
 * delete (this is leafbuf's block number, except when we're called
 * recursively).  stack is a search stack leading to it.  Note that we will
 * update the stack entry(s) to reflect current downlink positions --- this is
 * similar to the corresponding point in page split handling.
 *
 * If "first stage" caller cannot go ahead with deleting _any_ pages, returns
 * false.  Returns true on success, in which case caller can use certain
 * details established here to perform the first stage of deletion.  This
 * function is the last point at which page deletion may be deemed unsafe
 * (barring index corruption, or unexpected concurrent page deletions).
 *
 * We write lock the parent of the root of the to-be-deleted subtree for
 * caller on success (i.e. we leave our lock on the *subtreeparent buffer for
 * caller).  Caller will have to remove a downlink from *subtreeparent.  We
 * also set a *subtreeparent offset number in *poffset, to indicate the
 * location of the pivot tuple that contains the relevant downlink.
 *
 * The root of the to-be-deleted subtree is called the "top parent".  Note
 * that the leafbuf page is often the final "top parent" page (you can think
 * of the leafbuf page as a degenerate single page subtree when that happens).
 * Caller should initialize *topparent to the target leafbuf page block number
 * (while *topparentrightsib should be set to leafbuf's right sibling block
 * number).  We will update *topparent (and *topparentrightsib) for caller
 * here, though only when it turns out that caller will delete at least one
 * internal page (i.e. only when caller needs to store a valid link to the top
 * parent block in the leafbuf page using ABTreeTupleSetTopParent()).
 */
static bool
_abt_lock_subtree_parent(Relation rel, BlockNumber child, ABTStack stack,
						Buffer *subtreeparent, OffsetNumber *poffset,
						BlockNumber *topparent, BlockNumber *topparentrightsib,
						ABTCachedAggInfo agg_info)
{
	BlockNumber parent,
				leftsibparent;
	OffsetNumber parentoffset,
				maxoff;
	Buffer		pbuf;
	Page		page;
	ABTPageOpaque opaque;

	/*
	 * Locate the pivot tuple whose downlink points to "child".  Write lock
	 * the parent page itself.
	 */
	pbuf = _abt_getstackbuf(rel, stack, child, agg_info);
	if (pbuf == InvalidBuffer)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg_internal("failed to re-find parent key in index \"%s\" for deletion target page %u",
								 RelationGetRelationName(rel), child)));
	parent = stack->abts_blkno;
	parentoffset = stack->abts_offset;

	page = BufferGetPage(pbuf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	maxoff = PageGetMaxOffsetNumber(page);
	leftsibparent = opaque->abtpo_prev;

	/*
	 * _abt_getstackbuf() completes page splits on returned parent buffer when
	 * required.
	 *
	 * In general it's a bad idea for VACUUM to use up more disk space, which
	 * is why page deletion does not finish incomplete page splits most of the
	 * time.  We allow this limited exception because the risk is much lower,
	 * and the potential downside of not proceeding is much higher:  A single
	 * internal page with the INCOMPLETE_SPLIT flag set might otherwise
	 * prevent us from deleting hundreds of empty leaf pages from one level
	 * down.
	 */
	Assert(!ABT_P_INCOMPLETE_SPLIT(opaque));

	if (parentoffset < maxoff)
	{
		/*
		 * Child is not the rightmost child in parent, so it's safe to delete
		 * the subtree whose root/topparent is child page
		 */
		*subtreeparent = pbuf;
		*poffset = parentoffset;
		return true;
	}

	/*
	 * Child is the rightmost child of parent.
	 *
	 * Since it's the rightmost child of parent, deleting the child (or
	 * deleting the subtree whose root/topparent is the child page) is only
	 * safe when it's also possible to delete the parent.
	 */
	Assert(parentoffset == maxoff);
	if (parentoffset != ABT_P_FIRSTDATAKEY(opaque) || ABT_P_RIGHTMOST(opaque))
	{
		/*
		 * Child isn't parent's only child, or parent is rightmost on its
		 * entire level.  Definitely cannot delete any pages.
		 */
		_abt_relbuf(rel, pbuf);
		return false;
	}

	/*
	 * Now make sure that the parent deletion is itself safe by examining the
	 * child's grandparent page.  Recurse, passing the parent page as the
	 * child page (child's grandparent is the parent on the next level up). If
	 * parent deletion is unsafe, then child deletion must also be unsafe (in
	 * which case caller cannot delete any pages at all).
	 */
	*topparent = parent;
	*topparentrightsib = opaque->abtpo_next;

	/*
	 * Release lock on parent before recursing.
	 *
	 * It's OK to release page locks on parent before recursive call locks
	 * grandparent.  An internal page can only acquire an entry if the child
	 * is split, but that cannot happen as long as we still hold a lock on the
	 * leafbuf page.
	 */
	_abt_relbuf(rel, pbuf);

	/*
	 * Before recursing, check that the left sibling of parent (if any) is not
	 * marked with INCOMPLETE_SPLIT flag first (must do so after we drop the
	 * parent lock).
	 *
	 * Note: We deliberately avoid completing incomplete splits here.
	 */
	if (_abt_leftsib_splitflag(rel, leftsibparent, parent))
		return false;

	/* Recurse to examine child page's grandparent page */
	return _abt_lock_subtree_parent(rel, parent, stack->abts_parent,
								   subtreeparent, poffset,
								   topparent, topparentrightsib,
								   agg_info);
}

/*
 * Decrement the aggregations by deleted_sum from all index tuples in the
 * parent/ancestor pages that lead to the page in ``buf''. ``buf'' is
 * write-locked upon entry and will still be write-locked, to ensure there's no
 * concurrent split that may also make the deduction.
 */
void
_abt_decrement_aggs_for_vacuum(Relation rel, Buffer leafbuf, Datum deleted_sum,
                               ABTCachedAggInfo agg_info)
{
    ABTStack            stack,
                        old_stack;
    ABTScanInsert       searchkey = NULL;
    ItemId              itemid;
    IndexTuple          itup;
    Page                page;
    ABTPageOpaque       opaque,
                        copaque;
    Buffer              buf,
                        cbuf;
    BlockNumber         child_blkno,
                        blkno;
    OffsetNumber        off,
                        minoff,
                        maxoff,
                        low,
                        high;
    int                 i;
    Pointer             agg_ptr;

    page = BufferGetPage(leafbuf);
    opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

    /* The tree only has one level, so we don't have anything to decrement. */
    if (ABT_P_LEFTMOST(opaque) && ABT_P_RIGHTMOST(opaque))
        return ;

    /* 
     * Find the parent pages that lead to this leaf page using the high key,
     * or get the rightmost end point if this is the right-most leaf page.
     */
    if (ABT_P_RIGHTMOST(opaque))
    {
        buf = _abt_get_endpoint_stack(rel, 1, true, NULL, &stack);
    }
    else
    {
        itemid = PageGetItemId(page, ABTP_HIKEY);
        itup = (IndexTuple) PageGetItem(page, itemid);
        searchkey = _abt_mkscankey(rel, itup);
        searchkey->pivotsearch = true;

        stack = _abt_search_extended(rel, searchkey, &buf, ABT_READ,
                                     /* snapshot = */NULL,
                                     agg_info, /* level = */1);
    }
    
    /* 
     * Since we have sibling pages on the same level as the leaf page, there
     * must be some page above the leaf page to work on. It's not possible that
     * a concurrent backend deletes the only sibling of this leaf page without
     * first obtaining a lock on this leaf page.
     */
    Assert(BufferIsValid(buf));
    child_blkno = BufferGetBlockNumber(leafbuf);
    off = InvalidOffsetNumber;
    for (;;)
    {
        page = BufferGetPage(buf);
        opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
        minoff = ABT_P_FIRSTDATAKEY(opaque);
        maxoff = PageGetMaxOffsetNumber(page);

        /* 
         * Now buf should be the target page that contains an index tuple with
         * child_blkno: try to find that one.
         */
        if (off != InvalidOffsetNumber)
        {
            /* 
             * off is the offset number of the index tuple from which we last
             * decesended down the tree to the child page. Let's search around
             * it for the match.
             */
            low = off;
            high = off + 1;
            blkno = InvalidBlockNumber;
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

                blkno = ABTreeTupleGetDownLink(itup);
                if (blkno == child_blkno)
                {
                    /* found */
                    goto off_found;
                }
            }
        }
    
        /* 
         * off is InvalidOffsetNumber or it is not found by the above
         * if-branch.
         */
		if (searchkey)
		{
			off = _abt_binsrch(rel, searchkey, buf);
		}
		else
		{
			/* The right most leaf page must be down the right most subtree. */
			off = maxoff;
		}
		itemid = PageGetItemId(page, off);
		itup = (IndexTuple) PageGetItem(page, itemid);
    
off_found:
        /* Now we can xlog and decrement the aggregation. */
        Assert(ABTreeTupleGetDownLink(itup) == child_blkno);
        agg_ptr = ABTreePivotTupleGetAggPtr(itup, agg_info);

        START_CRIT_SECTION();
        
        if (agg_info->agg_typlen == 8)
        {
            pg_atomic_uint64    *lop = (pg_atomic_uint64 *) agg_ptr;
            int64               rop = DatumGetInt64(deleted_sum);
            (void) pg_atomic_fetch_add_u64(lop, -rop);
        }
        else if (agg_info->agg_typlen == 4)
        {
            pg_atomic_uint32    *lop = (pg_atomic_uint32 *) agg_ptr;
            int32               rop = DatumGetInt32(deleted_sum);
            (void) pg_atomic_fetch_add_u32(lop, -rop);
        }
        else
        {
            pg_atomic_uint32    *lop = (pg_atomic_uint32 *) agg_ptr;
            int32               rop = DatumGetInt16(deleted_sum);
            Assert(agg_info->agg_typlen == 2);
            (void) pg_atomic_fetch_add_u32(lop, -rop);
        }

        MarkBufferDirty(buf);

        if (RelationNeedsWAL(rel))
        {
            xl_abtree_agg_op        xlrec;
            XLogRecPtr              recptr;

            abt_create_agg_op(&xlrec,
                              XLOG_ABTREE_AGG_OP_FETCH_ADD,
                              agg_ptr - (Pointer) page);
            XLogBeginInsert();
            XLogRegisterData((char *) &xlrec, SizeOfABTreeAggOp);

            XLogRegisterBuffer(0, buf, REGBUF_STANDARD | REGBUF_NO_IMAGE);
            if (agg_info->agg_typlen == 8)
            {
                int64       rop = -DatumGetInt64(deleted_sum);
                XLogRegisterBufData(0, (char *) &rop, 8);
            }
            else if (agg_info->agg_typlen == 4)
            {
                int32       rop = -DatumGetInt32(deleted_sum);
                XLogRegisterBufData(0, (char *) &rop, 4);
            }
            else
            {
                int16       rop = -DatumGetInt16(deleted_sum);
                XLogRegisterBufData(0, (char *) &rop, 2);
            }
            recptr = XLogInsert(RM_ABTREE_ID, XLOG_ABTREE_AGG_OP);
            PageAtomicSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
        
        if (ABT_P_LEFTMOST(opaque) && ABT_P_RIGHTMOST(opaque))
        {
            /* reached the root, we're done. */
            _abt_relbuf(rel, buf);
            break;
        }

        /* Otherwise, go one level up in the stack while holding the latch. */
        cbuf = buf;
        child_blkno = BufferGetBlockNumber(cbuf);
        copaque = opaque;
    
        if (!stack)
        {
            /* 
             * We must have had a concurrent split on the fast root when we
             * reached the page in cbuf. We'll need to find its parent through
             * _abt_get_endpoint_stack or _abt_search again.
             */
            if (searchkey == NULL)
            {
                Assert(ABT_P_RIGHTMOST(copaque)); 
                buf = _abt_get_endpoint_stack(rel, copaque->abtpo.level + 1,
                                              true, NULL, &stack);
            }
            else
            {
                stack = _abt_search_extended(rel, searchkey, &buf, ABT_READ,
                                             /* snapshot = */NULL,
                                             agg_info,
                                             copaque->abtpo.level + 1);
            }

            if (!BufferIsValid(buf))
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED),
                         errmsg_internal("unable to find parent page on level "
                             "%u for aggregation maitenance in abtree vacuum "
                             "in index \"%s\"",
                             copaque->abtpo.level + 1,
                             RelationGetRelationName(rel))));
            }

            off = InvalidOffsetNumber;
        }
        else
        {
            buf = _abt_getbuf(rel, stack->abts_blkno, ABT_READ);
            page = BufferGetPage(buf);
            opaque = (ABTPageOpaque) PageGetSpecialPointer(page);

            if (opaque->abtpo_last_update_id != stack->abts_last_update_id)
            {
                /* 
                 * Some split must have happened so check if we need to move to
                 * the right.
                 */
                if (searchkey == NULL)
                {
                    while (!ABT_P_RIGHTMOST(opaque))
                    {
                        buf = _abt_relandgetbuf(rel, buf, opaque->abtpo_next,
                                                ABT_READ);
                    }
                }
                else
                {
                    /* 
                     * Pass NULL stack, snapshot and agg_info because we do not
                     * try to finish incomplete split here.
                     */
                    buf = _abt_moveright(rel, searchkey, buf,
                                         /* for_update = */false,
                                         /* stack = */NULL,
                                         ABT_READ,
                                         /* snapshot = */NULL,
                                         /* agg_info = */NULL);
                }
                
                blkno = BufferGetBlockNumber(buf);
                /* 
                 * If we move to the right, we're forced to do a binary
                 * search to find the down link as we know nothing about the
                 * new page. Otherwise, we can try around the original offset
                 * we descended down the tree, even if the page has changed.
                 */
                if (blkno != stack->abts_blkno)
                {
                    off = InvalidOffsetNumber;
                }
            }
            else
            {
                off = stack->abts_offset;
            }
            
            /* be tidy */
            old_stack = stack;
            stack = stack->abts_parent;
            pfree(old_stack);
        }
        
        /* 
         * We're good to release the latch on the child page now since we hold
         * a latch on its parent page.
         */
        _abt_relbuf(rel, cbuf);
    }
    
    /* be tidy */
    Assert(!stack);
    if (searchkey)
        pfree(searchkey);
}

