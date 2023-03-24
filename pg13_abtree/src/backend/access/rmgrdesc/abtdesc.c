/*-------------------------------------------------------------------------
 *
 * abtdesc.c
 *	  rmgr descriptor routines for access/abtree/abtxlog.c
 *
 *	  Copied and adapted from src/backend/access/rmgrdesc/nbtdesc.c.
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/abtdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/abtxlog.h"

void
abtree_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_ABTREE_INSERT_LEAF:
		case XLOG_ABTREE_INSERT_UPPER:
		case XLOG_ABTREE_INSERT_META:
		case XLOG_ABTREE_INSERT_POST:
			{
				xl_abtree_insert *xlrec = (xl_abtree_insert *) rec;

				appendStringInfo(buf, "off %u", xlrec->offnum);
				break;
			}
		case XLOG_ABTREE_SPLIT_L:
		case XLOG_ABTREE_SPLIT_R:
			{
				xl_abtree_split *xlrec = (xl_abtree_split *) rec;

				appendStringInfo(buf, "level %u, firstrightoff %d, newitemoff %d, postingoff %d",
								 xlrec->level, xlrec->firstrightoff,
								 xlrec->newitemoff, xlrec->postingoff);
				break;
			}
		case XLOG_ABTREE_DEDUP:
			{
				xl_abtree_dedup *xlrec = (xl_abtree_dedup *) rec;

				appendStringInfo(buf, "nintervals %u", xlrec->nintervals);
				break;
			}
		case XLOG_ABTREE_VACUUM:
			{
				xl_abtree_vacuum *xlrec = (xl_abtree_vacuum *) rec;

				appendStringInfo(buf, "ndeleted %u; nupdated %u",
								 xlrec->ndeleted, xlrec->nupdated);
				break;
			}
		case XLOG_ABTREE_DELETE:
			{
				xl_abtree_delete *xlrec = (xl_abtree_delete *) rec;

				appendStringInfo(buf, "latestRemovedXid %u; ndeleted %u",
								 xlrec->latestRemovedXid, xlrec->ndeleted);
				break;
			}
		case XLOG_ABTREE_MARK_PAGE_HALFDEAD:
			{
				xl_abtree_mark_page_halfdead *xlrec = (xl_abtree_mark_page_halfdead *) rec;

				appendStringInfo(buf, "topparent %u; leaf %u; left %u; right %u",
								 xlrec->topparent, xlrec->leafblk, xlrec->leftblk, xlrec->rightblk);
				break;
			}
		case XLOG_ABTREE_UNLINK_PAGE_META:
		case XLOG_ABTREE_UNLINK_PAGE:
			{
				xl_abtree_unlink_page *xlrec = (xl_abtree_unlink_page *) rec;

				appendStringInfo(buf, "left %u; right %u; abtpo_xact %u; ",
								 xlrec->leftsib, xlrec->rightsib,
								 xlrec->abtpo_xact);
				appendStringInfo(buf, "leafleft %u; leafright %u; topparent %u",
								 xlrec->leafleftsib, xlrec->leafrightsib,
								 xlrec->topparent);
				break;
			}
		case XLOG_ABTREE_NEWROOT:
			{
				xl_abtree_newroot *xlrec = (xl_abtree_newroot *) rec;

				appendStringInfo(buf, "lev %u", xlrec->level);
				break;
			}
		case XLOG_ABTREE_REUSE_PAGE:
			{
				xl_abtree_reuse_page *xlrec = (xl_abtree_reuse_page *) rec;

				appendStringInfo(buf, "rel %u/%u/%u; latestRemovedXid %u",
								 xlrec->node.spcNode, xlrec->node.dbNode,
								 xlrec->node.relNode, xlrec->latestRemovedXid);
				break;
			}
		case XLOG_ABTREE_META_CLEANUP:
			{
				xl_abtree_metadata *xlrec;

				xlrec = (xl_abtree_metadata *) XLogRecGetBlockData(record, 0,
																  NULL);
				appendStringInfo(buf, "oldest_abtpo_xact %u; last_cleanup_num_heap_tuples: %f",
								 xlrec->oldest_abtpo_xact,
								 xlrec->last_cleanup_num_heap_tuples);
				break;
			}
	}
}

const char *
abtree_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_ABTREE_INSERT_LEAF:
			id = "INSERT_LEAF";
			break;
		case XLOG_ABTREE_INSERT_UPPER:
			id = "INSERT_UPPER";
			break;
		case XLOG_ABTREE_INSERT_META:
			id = "INSERT_META";
			break;
		case XLOG_ABTREE_SPLIT_L:
			id = "SPLIT_L";
			break;
		case XLOG_ABTREE_SPLIT_R:
			id = "SPLIT_R";
			break;
		case XLOG_ABTREE_INSERT_POST:
			id = "INSERT_POST";
			break;
		case XLOG_ABTREE_DEDUP:
			id = "DEDUP";
			break;
		case XLOG_ABTREE_VACUUM:
			id = "VACUUM";
			break;
		case XLOG_ABTREE_DELETE:
			id = "DELETE";
			break;
		case XLOG_ABTREE_MARK_PAGE_HALFDEAD:
			id = "MARK_PAGE_HALFDEAD";
			break;
		case XLOG_ABTREE_UNLINK_PAGE:
			id = "UNLINK_PAGE";
			break;
		case XLOG_ABTREE_UNLINK_PAGE_META:
			id = "UNLINK_PAGE_META";
			break;
		case XLOG_ABTREE_NEWROOT:
			id = "NEWROOT";
			break;
		case XLOG_ABTREE_REUSE_PAGE:
			id = "REUSE_PAGE";
			break;
		case XLOG_ABTREE_META_CLEANUP:
			id = "META_CLEANUP";
			break;
	}

	return id;
}
