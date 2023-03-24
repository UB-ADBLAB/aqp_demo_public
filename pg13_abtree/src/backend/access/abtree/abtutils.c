/*-------------------------------------------------------------------------
 *
 * abtutils.c
 *	  Utility code for aggregate btree.
 *
 *	  Copied and adapted from src/backend/access/nbtree/nbtutils.c.
 *
 * IDENTIFICATION
 *	  src/backend/access/abtree/abtutils.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>

#include "access/abtree.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "catalog/catalog.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/progress.h"
#include "lib/qunique.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"


typedef struct ABTSortArrayContext
{
	FmgrInfo	flinfo;
	Oid			collation;
	bool		reverse;
} ABTSortArrayContext;

typedef struct ABTAMCacheHeaderData
{
	ABTMetaPageData		*metad;
	ABTCachedAggInfo	agg_info;
} ABTAMCacheHeaderData;

typedef ABTAMCacheHeaderData *ABTAMCacheHeader;

static Datum _abt_find_extreme_element(IndexScanDesc scan, ScanKey skey,
									  StrategyNumber strat,
									  Datum *elems, int nelems);
static int	_abt_sort_array_elements(IndexScanDesc scan, ScanKey skey,
									bool reverse,
									Datum *elems, int nelems);
static int	_abt_compare_array_elements(const void *a, const void *b, void *arg);
static bool _abt_compare_scankey_args(IndexScanDesc scan, ScanKey op,
									 ScanKey leftarg, ScanKey rightarg,
									 bool *result);
static bool _abt_fix_scankey_strategy(ScanKey skey, int16 *indoption);
static void _abt_mark_scankey_required(ScanKey skey);
static bool _abt_check_rowcompare(ScanKey skey,
								 IndexTuple tuple, int tupnatts, TupleDesc tupdesc,
								 ScanDirection dir, bool *continuescan);
static int	_abt_keep_natts(Relation rel, IndexTuple lastleft,
						   IndexTuple firstright, ABTScanInsert itup_key);

/*
 * _abt_mkscankey
 *		Build an insertion scan key that contains comparison data from itup
 *		as well as comparator routines appropriate to the key datatypes.
 *
 *		When itup is a non-pivot tuple, the returned insertion scan key is
 *		suitable for finding a place for it to go on the leaf level.  Pivot
 *		tuples can be used to re-find leaf page with matching high key, but
 *		then caller needs to set scan key's pivotsearch field to true.  This
 *		allows caller to search for a leaf page with a matching high key,
 *		which is usually to the left of the first leaf page a non-pivot match
 *		might appear on.
 *
 *		The result is intended for use with _abt_compare() and _abt_truncate().
 *		Callers that don't need to fill out the insertion scankey arguments
 *		(e.g. they use an ad-hoc comparison routine, or only need a scankey
 *		for _abt_truncate()) can pass a NULL index tuple.  The scankey will
 *		be initialized as if an "all truncated" pivot tuple was passed
 *		instead.
 *
 *		Note that we may occasionally have to share lock the metapage to
 *		determine whether or not the keys in the index are expected to be
 *		unique (i.e. if this is a "heapkeyspace" index).  We assume a
 *		heapkeyspace index when caller passes a NULL tuple, allowing index
 *		build callers to avoid accessing the non-existent metapage.  We
 *		also assume that the index is _not_ allequalimage when a NULL tuple
 *		is passed; CREATE INDEX callers call _abt_allequalimage() to set the
 *		field themselves.
 *
 *		key is a previously initialized key returned by
 *		_abt_mkscankey_extended(). If non-NULL, we won't allocate new memory
 *		for that.
 */
ABTScanInsert
_abt_mkscankey_extended(Relation rel, IndexTuple itup, ABTScanInsert key)
{
	ScanKey		skey;
	TupleDesc	itupdesc;
	int			indnkeyatts;
	int16	   *indoption;
	int			tupnatts;
	int			i;

	itupdesc = RelationGetDescr(rel);
	indnkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	indoption = rel->rd_indoption;
	tupnatts = itup ? ABTreeTupleGetNAtts(itup, rel) : 0;

	Assert(tupnatts <= IndexRelationGetNumberOfAttributes(rel));

	/*
	 * We'll execute search using scan key constructed on key columns.
	 * Truncated attributes and non-key attributes are omitted from the final
	 * scan key.
	 *
	 * Only initialize these fields once.
	 */
	if (!key)
	{
		key = palloc(offsetof(ABTScanInsertData, scankeys) +
					 sizeof(ScanKeyData) * indnkeyatts);
		if (itup)
			_abt_metaversion(rel, &key->heapkeyspace, &key->allequalimage);
		else
		{
			/* Utility statement callers can set these fields themselves */
			key->heapkeyspace = true;
			key->allequalimage = false;
		}
	}

	key->anynullkeys = false;	/* initial assumption */
	key->nextkey = false;
	key->pivotsearch = false;
	key->keysz = Min(indnkeyatts, tupnatts);
	key->scantid = key->heapkeyspace && itup ?
		ABTreeTupleGetHeapTID(itup) : NULL;
	skey = key->scankeys;
	for (i = 0; i < indnkeyatts; i++)
	{
		FmgrInfo   *procinfo;
		Datum		arg;
		bool		null;
		int			flags;

		/*
		 * We can use the cached (default) support procs since no cross-type
		 * comparison can be needed.
		 */
		procinfo = index_getprocinfo(rel, i + 1, ABTORDER_PROC);

		/*
		 * Key arguments built from truncated attributes (or when caller
		 * provides no tuple) are defensively represented as NULL values. They
		 * should never be used.
		 */
		if (i < tupnatts)
			arg = index_getattr(itup, i + 1, itupdesc, &null);
		else
		{
			arg = (Datum) 0;
			null = true;
		}
		flags = (null ? SK_ISNULL : 0) | (indoption[i] << SK_ABT_INDOPTION_SHIFT);
		ScanKeyEntryInitializeWithInfo(&skey[i],
									   flags,
									   (AttrNumber) (i + 1),
									   InvalidStrategy,
									   InvalidOid,
									   rel->rd_indcollation[i],
									   procinfo,
									   arg);
		/* Record if any key attribute is NULL (or truncated) */
		if (null)
			key->anynullkeys = true;
	}

	return key;
}

/*
 * free a retracement stack made by _abt_search.
 */
void
_abt_freestack(Relation rel, ABTStack stack)
{
	ABTStack		ostack;

	while (stack != NULL)
	{
		ostack = stack;
		stack = stack->abts_parent;
		pfree(ostack);
	}
}

/*
 *	_abt_preprocess_array_keys() -- Preprocess SK_SEARCHARRAY scan keys
 *
 * If there are any SK_SEARCHARRAY scan keys, deconstruct the array(s) and
 * set up ABTArrayKeyInfo info for each one that is an equality-type key.
 * Prepare modified scan keys in so->arrayKeyData, which will hold the current
 * array elements during each primitive indexscan operation.  For inequality
 * array keys, it's sufficient to find the extreme element value and replace
 * the whole array with that scalar value.
 *
 * Note: the reason we need so->arrayKeyData, rather than just scribbling
 * on scan->keyData, is that callers are permitted to call btrescan without
 * supplying a new set of scankey data.
 */
void
_abt_preprocess_array_keys(IndexScanDesc scan)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	int			numberOfKeys = scan->numberOfKeys;
	int16	   *indoption = scan->indexRelation->rd_indoption;
	int			numArrayKeys;
	ScanKey		cur;
	int			i;
	MemoryContext oldContext;

	/* Quick check to see if there are any array keys */
	numArrayKeys = 0;
	for (i = 0; i < numberOfKeys; i++)
	{
		cur = &scan->keyData[i];
		if (cur->sk_flags & SK_SEARCHARRAY)
		{
			numArrayKeys++;
			Assert(!(cur->sk_flags & (SK_ROW_HEADER | SK_SEARCHNULL | SK_SEARCHNOTNULL)));
			/* If any arrays are null as a whole, we can quit right now. */
			if (cur->sk_flags & SK_ISNULL)
			{
				so->numArrayKeys = -1;
				so->arrayKeyData = NULL;
				return;
			}
		}
	}

	/* Quit if nothing to do. */
	if (numArrayKeys == 0)
	{
		so->numArrayKeys = 0;
		so->arrayKeyData = NULL;
		return;
	}

	/*
	 * Make a scan-lifespan context to hold array-associated data, or reset it
	 * if we already have one from a previous rescan cycle.
	 */
	if (so->arrayContext == NULL)
		so->arrayContext = AllocSetContextCreate(CurrentMemoryContext,
												 "ABTree array context",
												 ALLOCSET_SMALL_SIZES);
	else
		MemoryContextReset(so->arrayContext);

	oldContext = MemoryContextSwitchTo(so->arrayContext);

	/* Create modifiable copy of scan->keyData in the workspace context */
	so->arrayKeyData = (ScanKey) palloc(scan->numberOfKeys * sizeof(ScanKeyData));
	memcpy(so->arrayKeyData,
		   scan->keyData,
		   scan->numberOfKeys * sizeof(ScanKeyData));

	/* Allocate space for per-array data in the workspace context */
	so->arrayKeys = (ABTArrayKeyInfo *) palloc0(numArrayKeys * sizeof(ABTArrayKeyInfo));

	/* Now process each array key */
	numArrayKeys = 0;
	for (i = 0; i < numberOfKeys; i++)
	{
		ArrayType  *arrayval;
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		int			num_elems;
		Datum	   *elem_values;
		bool	   *elem_nulls;
		int			num_nonnulls;
		int			j;

		cur = &so->arrayKeyData[i];
		if (!(cur->sk_flags & SK_SEARCHARRAY))
			continue;

		/*
		 * First, deconstruct the array into elements.  Anything allocated
		 * here (including a possibly detoasted array value) is in the
		 * workspace context.
		 */
		arrayval = DatumGetArrayTypeP(cur->sk_argument);
		/* We could cache this data, but not clear it's worth it */
		get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
							 &elmlen, &elmbyval, &elmalign);
		deconstruct_array(arrayval,
						  ARR_ELEMTYPE(arrayval),
						  elmlen, elmbyval, elmalign,
						  &elem_values, &elem_nulls, &num_elems);

		/*
		 * Compress out any null elements.  We can ignore them since we assume
		 * all btree operators are strict.
		 */
		num_nonnulls = 0;
		for (j = 0; j < num_elems; j++)
		{
			if (!elem_nulls[j])
				elem_values[num_nonnulls++] = elem_values[j];
		}

		/* We could pfree(elem_nulls) now, but not worth the cycles */

		/* If there's no non-nulls, the scan qual is unsatisfiable */
		if (num_nonnulls == 0)
		{
			numArrayKeys = -1;
			break;
		}

		/*
		 * If the comparison operator is not equality, then the array qual
		 * degenerates to a simple comparison against the smallest or largest
		 * non-null array element, as appropriate.
		 */
		switch (cur->sk_strategy)
		{
			case BTLessStrategyNumber:
			case BTLessEqualStrategyNumber:
				cur->sk_argument =
					_abt_find_extreme_element(scan, cur,
											 BTGreaterStrategyNumber,
											 elem_values, num_nonnulls);
				continue;
			case BTEqualStrategyNumber:
				/* proceed with rest of loop */
				break;
			case BTGreaterEqualStrategyNumber:
			case BTGreaterStrategyNumber:
				cur->sk_argument =
					_abt_find_extreme_element(scan, cur,
											 BTLessStrategyNumber,
											 elem_values, num_nonnulls);
				continue;
			default:
				elog(ERROR, "unrecognized StrategyNumber: %d",
					 (int) cur->sk_strategy);
				break;
		}

		/*
		 * Sort the non-null elements and eliminate any duplicates.  We must
		 * sort in the same ordering used by the index column, so that the
		 * successive primitive indexscans produce data in index order.
		 */
		num_elems = _abt_sort_array_elements(scan, cur,
											(indoption[cur->sk_attno - 1] & INDOPTION_DESC) != 0,
											elem_values, num_nonnulls);

		/*
		 * And set up the ABTArrayKeyInfo data.
		 */
		so->arrayKeys[numArrayKeys].scan_key = i;
		so->arrayKeys[numArrayKeys].num_elems = num_elems;
		so->arrayKeys[numArrayKeys].elem_values = elem_values;
		numArrayKeys++;
	}

	so->numArrayKeys = numArrayKeys;

	MemoryContextSwitchTo(oldContext);
}

/*
 * _abt_find_extreme_element() -- get least or greatest array element
 *
 * scan and skey identify the index column, whose opfamily determines the
 * comparison semantics.  strat should be BTLessStrategyNumber to get the
 * least element, or BTGreaterStrategyNumber to get the greatest.
 */
static Datum
_abt_find_extreme_element(IndexScanDesc scan, ScanKey skey,
						 StrategyNumber strat,
						 Datum *elems, int nelems)
{
	Relation	rel = scan->indexRelation;
	Oid			elemtype,
				cmp_op;
	RegProcedure cmp_proc;
	FmgrInfo	flinfo;
	Datum		result;
	int			i;

	/*
	 * Determine the nominal datatype of the array elements.  We have to
	 * support the convention that sk_subtype == InvalidOid means the opclass
	 * input type; this is a hack to simplify life for ScanKeyInit().
	 */
	elemtype = skey->sk_subtype;
	if (elemtype == InvalidOid)
		elemtype = rel->rd_opcintype[skey->sk_attno - 1];

	/*
	 * Look up the appropriate comparison operator in the opfamily.
	 *
	 * Note: it's possible that this would fail, if the opfamily is
	 * incomplete, but it seems quite unlikely that an opfamily would omit
	 * non-cross-type comparison operators for any datatype that it supports
	 * at all.
	 */
	cmp_op = get_opfamily_member(rel->rd_opfamily[skey->sk_attno - 1],
								 elemtype,
								 elemtype,
								 strat);
	if (!OidIsValid(cmp_op))
		elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
			 strat, elemtype, elemtype,
			 rel->rd_opfamily[skey->sk_attno - 1]);
	cmp_proc = get_opcode(cmp_op);
	if (!RegProcedureIsValid(cmp_proc))
		elog(ERROR, "missing oprcode for operator %u", cmp_op);

	fmgr_info(cmp_proc, &flinfo);

	Assert(nelems > 0);
	result = elems[0];
	for (i = 1; i < nelems; i++)
	{
		if (DatumGetBool(FunctionCall2Coll(&flinfo,
										   skey->sk_collation,
										   elems[i],
										   result)))
			result = elems[i];
	}

	return result;
}

/*
 * _abt_sort_array_elements() -- sort and de-dup array elements
 *
 * The array elements are sorted in-place, and the new number of elements
 * after duplicate removal is returned.
 *
 * scan and skey identify the index column, whose opfamily determines the
 * comparison semantics.  If reverse is true, we sort in descending order.
 */
static int
_abt_sort_array_elements(IndexScanDesc scan, ScanKey skey,
						bool reverse,
						Datum *elems, int nelems)
{
	Relation	rel = scan->indexRelation;
	Oid			elemtype;
	RegProcedure cmp_proc;
	ABTSortArrayContext cxt;

	if (nelems <= 1)
		return nelems;			/* no work to do */

	/*
	 * Determine the nominal datatype of the array elements.  We have to
	 * support the convention that sk_subtype == InvalidOid means the opclass
	 * input type; this is a hack to simplify life for ScanKeyInit().
	 */
	elemtype = skey->sk_subtype;
	if (elemtype == InvalidOid)
		elemtype = rel->rd_opcintype[skey->sk_attno - 1];

	/*
	 * Look up the appropriate comparison function in the opfamily.
	 *
	 * Note: it's possible that this would fail, if the opfamily is
	 * incomplete, but it seems quite unlikely that an opfamily would omit
	 * non-cross-type support functions for any datatype that it supports at
	 * all.
	 */
	cmp_proc = get_opfamily_proc(rel->rd_opfamily[skey->sk_attno - 1],
								 elemtype,
								 elemtype,
								 ABTORDER_PROC);
	if (!RegProcedureIsValid(cmp_proc))
		elog(ERROR, "missing support function %d(%u,%u) in opfamily %u",
			 ABTORDER_PROC, elemtype, elemtype,
			 rel->rd_opfamily[skey->sk_attno - 1]);

	/* Sort the array elements */
	fmgr_info(cmp_proc, &cxt.flinfo);
	cxt.collation = skey->sk_collation;
	cxt.reverse = reverse;
	qsort_arg((void *) elems, nelems, sizeof(Datum),
			  _abt_compare_array_elements, (void *) &cxt);

	/* Now scan the sorted elements and remove duplicates */
	return qunique_arg(elems, nelems, sizeof(Datum),
					   _abt_compare_array_elements, &cxt);
}

/*
 * qsort_arg comparator for sorting array elements
 */
static int
_abt_compare_array_elements(const void *a, const void *b, void *arg)
{
	Datum		da = *((const Datum *) a);
	Datum		db = *((const Datum *) b);
	ABTSortArrayContext *cxt = (ABTSortArrayContext *) arg;
	int32		compare;

	compare = DatumGetInt32(FunctionCall2Coll(&cxt->flinfo,
											  cxt->collation,
											  da, db));
	if (cxt->reverse)
		INVERT_COMPARE_RESULT(compare);
	return compare;
}

/*
 * _abt_start_array_keys() -- Initialize array keys at start of a scan
 *
 * Set up the cur_elem counters and fill in the first sk_argument value for
 * each array scankey.  We can't do this until we know the scan direction.
 */
void
_abt_start_array_keys(IndexScanDesc scan, ScanDirection dir)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	int			i;

	for (i = 0; i < so->numArrayKeys; i++)
	{
		ABTArrayKeyInfo *curArrayKey = &so->arrayKeys[i];
		ScanKey		skey = &so->arrayKeyData[curArrayKey->scan_key];

		Assert(curArrayKey->num_elems > 0);
		if (ScanDirectionIsBackward(dir))
			curArrayKey->cur_elem = curArrayKey->num_elems - 1;
		else
			curArrayKey->cur_elem = 0;
		skey->sk_argument = curArrayKey->elem_values[curArrayKey->cur_elem];
	}
}

/*
 * _abt_advance_array_keys() -- Advance to next set of array elements
 *
 * Returns true if there is another set of values to consider, false if not.
 * On true result, the scankeys are initialized with the next set of values.
 */
bool
_abt_advance_array_keys(IndexScanDesc scan, ScanDirection dir)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	bool		found = false;
	int			i;

	/*
	 * We must advance the last array key most quickly, since it will
	 * correspond to the lowest-order index column among the available
	 * qualifications. This is necessary to ensure correct ordering of output
	 * when there are multiple array keys.
	 */
	for (i = so->numArrayKeys - 1; i >= 0; i--)
	{
		ABTArrayKeyInfo *curArrayKey = &so->arrayKeys[i];
		ScanKey		skey = &so->arrayKeyData[curArrayKey->scan_key];
		int			cur_elem = curArrayKey->cur_elem;
		int			num_elems = curArrayKey->num_elems;

		if (ScanDirectionIsBackward(dir))
		{
			if (--cur_elem < 0)
			{
				cur_elem = num_elems - 1;
				found = false;	/* need to advance next array key */
			}
			else
				found = true;
		}
		else
		{
			if (++cur_elem >= num_elems)
			{
				cur_elem = 0;
				found = false;	/* need to advance next array key */
			}
			else
				found = true;
		}

		curArrayKey->cur_elem = cur_elem;
		skey->sk_argument = curArrayKey->elem_values[cur_elem];
		if (found)
			break;
	}

	/* 
	 * If we do end up here, we'll need to reset the preprocessed bit and
	 * the inskey cache, so that the next call to _abt_preprocess_key() in
	 * _abt_first() won't complain.
	 */
	so->preprocessed = false;
	if (so->inskey)
	{
		pfree(so->inskey);
		so->inskey = NULL;
	}

	/* advance parallel scan */
	if (scan->parallel_scan != NULL)
		_abt_parallel_advance_array_keys(scan);
	
	return found;
}

/*
 * _abt_mark_array_keys() -- Handle array keys during btmarkpos
 *
 * Save the current state of the array keys as the "mark" position.
 */
void
_abt_mark_array_keys(IndexScanDesc scan)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	int			i;

	for (i = 0; i < so->numArrayKeys; i++)
	{
		ABTArrayKeyInfo *curArrayKey = &so->arrayKeys[i];

		curArrayKey->mark_elem = curArrayKey->cur_elem;
	}
}

/*
 * _abt_restore_array_keys() -- Handle array keys during btrestrpos
 *
 * Restore the array keys to where they were when the mark was set.
 */
void
_abt_restore_array_keys(IndexScanDesc scan)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	bool		changed = false;
	int			i;

	/* Restore each array key to its position when the mark was set */
	for (i = 0; i < so->numArrayKeys; i++)
	{
		ABTArrayKeyInfo *curArrayKey = &so->arrayKeys[i];
		ScanKey		skey = &so->arrayKeyData[curArrayKey->scan_key];
		int			mark_elem = curArrayKey->mark_elem;

		if (curArrayKey->cur_elem != mark_elem)
		{
			curArrayKey->cur_elem = mark_elem;
			skey->sk_argument = curArrayKey->elem_values[mark_elem];
			changed = true;
		}
	}

	/*
	 * If we changed any keys, we must redo _abt_preprocess_keys.  That might
	 * sound like overkill, but in cases with multiple keys per index column
	 * it seems necessary to do the full set of pushups.
	 */
	if (changed)
	{
		/* 
		 * Need to reset the preprocessed bit and the inskey because the
		 * key has changed.
		 */
		so->preprocessed = false;
		if (so->inskey)
		{
			pfree(so->inskey);
			so->inskey = NULL;
		}
		_abt_preprocess_keys(scan);
		/* The mark should have been set on a consistent set of keys... */
		Assert(so->qual_ok);
	}
}


/*
 *	_abt_preprocess_keys() -- Preprocess scan keys
 *
 * The given search-type keys (in scan->keyData[] or so->arrayKeyData[])
 * are copied to so->keyData[] with possible transformation.
 * scan->numberOfKeys is the number of input keys, so->numberOfKeys gets
 * the number of output keys (possibly less, never greater).
 *
 * The output keys are marked with additional sk_flags bits beyond the
 * system-standard bits supplied by the caller.  The DESC and NULLS_FIRST
 * indoption bits for the relevant index attribute are copied into the flags.
 * Also, for a DESC column, we commute (flip) all the sk_strategy numbers
 * so that the index sorts in the desired direction.
 *
 * One key purpose of this routine is to discover which scan keys must be
 * satisfied to continue the scan.  It also attempts to eliminate redundant
 * keys and detect contradictory keys.  (If the index opfamily provides
 * incomplete sets of cross-type operators, we may fail to detect redundant
 * or contradictory keys, but we can survive that.)
 *
 * The output keys must be sorted by index attribute.  Presently we expect
 * (but verify) that the input keys are already so sorted --- this is done
 * by match_clauses_to_index() in indxpath.c.  Some reordering of the keys
 * within each attribute may be done as a byproduct of the processing here,
 * but no other code depends on that.
 *
 * The output keys are marked with flags SK_ABT_REQFWD and/or SK_ABT_REQBKWD
 * if they must be satisfied in order to continue the scan forward or backward
 * respectively.  _abt_checkkeys uses these flags.  For example, if the quals
 * are "x = 1 AND y < 4 AND z < 5", then _abt_checkkeys will reject a tuple
 * (1,2,7), but we must continue the scan in case there are tuples (1,3,z).
 * But once we reach tuples like (1,4,z) we can stop scanning because no
 * later tuples could match.  This is reflected by marking the x and y keys,
 * but not the z key, with SK_ABT_REQFWD.  In general, the keys for leading
 * attributes with "=" keys are marked both SK_ABT_REQFWD and SK_ABT_REQBKWD.
 * For the first attribute without an "=" key, any "<" and "<=" keys are
 * marked SK_ABT_REQFWD while any ">" and ">=" keys are marked SK_ABT_REQBKWD.
 * This can be seen to be correct by considering the above example.  Note
 * in particular that if there are no keys for a given attribute, the keys for
 * subsequent attributes can never be required; for instance "WHERE y = 4"
 * requires a full-index scan.
 *
 * If possible, redundant keys are eliminated: we keep only the tightest
 * >/>= bound and the tightest </<= bound, and if there's an = key then
 * that's the only one returned.  (So, we return either a single = key,
 * or one or two boundary-condition keys for each attr.)  However, if we
 * cannot compare two keys for lack of a suitable cross-type operator,
 * we cannot eliminate either.  If there are two such keys of the same
 * operator strategy, the second one is just pushed into the output array
 * without further processing here.  We may also emit both >/>= or both
 * </<= keys if we can't compare them.  The logic about required keys still
 * works if we don't eliminate redundant keys.
 *
 * Note that one reason we need direction-sensitive required-key flags is
 * precisely that we may not be able to eliminate redundant keys.  Suppose
 * we have "x > 4::int AND x > 10::bigint", and we are unable to determine
 * which key is more restrictive for lack of a suitable cross-type operator.
 * _abt_first will arbitrarily pick one of the keys to do the initial
 * positioning with.  If it picks x > 4, then the x > 10 condition will fail
 * until we reach index entries > 10; but we can't stop the scan just because
 * x > 10 is failing.  On the other hand, if we are scanning backwards, then
 * failure of either key is indeed enough to stop the scan.  (In general, when
 * inequality keys are present, the initial-positioning code only promises to
 * position before the first possible match, not exactly at the first match,
 * for a forward scan; or after the last match for a backward scan.)
 *
 * As a byproduct of this work, we can detect contradictory quals such
 * as "x = 1 AND x > 2".  If we see that, we return so->qual_ok = false,
 * indicating the scan need not be run at all since no tuples can match.
 * (In this case we do not bother completing the output key array!)
 * Again, missing cross-type operators might cause us to fail to prove the
 * quals contradictory when they really are, but the scan will work correctly.
 *
 * Row comparison keys are currently also treated without any smarts:
 * we just transfer them into the preprocessed array without any
 * editorialization.  We can treat them the same as an ordinary inequality
 * comparison on the row's first index column, for the purposes of the logic
 * about required keys.
 *
 * Note: the reason we have to copy the preprocessed scan keys into private
 * storage is that we are modifying the array based on comparisons of the
 * key argument values, which could change on a rescan or after moving to
 * new elements of array keys.  Therefore we can't overwrite the source data.
 */
void
_abt_preprocess_keys(IndexScanDesc scan)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	int			numberOfKeys = scan->numberOfKeys;
	int16	   *indoption = scan->indexRelation->rd_indoption;
	int			new_numberOfKeys;
	int			numberOfEqualCols;
	ScanKey		inkeys;
	ScanKey		outkeys;
	ScanKey		cur;
	ScanKey		xform[BTMaxStrategyNumber];
	bool		test_result;
	int			i,
				j;
	AttrNumber	attno;

	Assert(!so->preprocessed);

	/* initialize result variables */
	so->preprocessed = true;
	so->qual_ok = true;
	so->numberOfKeys = 0;

	if (numberOfKeys < 1)
		return;					/* done if qual-less scan */

	/*
	 * Read so->arrayKeyData if array keys are present, else scan->keyData
	 */
	if (so->arrayKeyData != NULL)
		inkeys = so->arrayKeyData;
	else
		inkeys = scan->keyData;

	outkeys = so->keyData;
	cur = &inkeys[0];
	/* we check that input keys are correctly ordered */
	if (cur->sk_attno < 1)
		elog(ERROR, "btree index keys must be ordered by attribute");

	/* We can short-circuit most of the work if there's just one key */
	if (numberOfKeys == 1)
	{
		/* Apply indoption to scankey (might change sk_strategy!) */
		if (!_abt_fix_scankey_strategy(cur, indoption))
			so->qual_ok = false;
		memcpy(outkeys, cur, sizeof(ScanKeyData));
		so->numberOfKeys = 1;
		/* We can mark the qual as required if it's for first index col */
		if (cur->sk_attno == 1)
			_abt_mark_scankey_required(outkeys);
		return;
	}

	/*
	 * Otherwise, do the full set of pushups.
	 */
	new_numberOfKeys = 0;
	numberOfEqualCols = 0;

	/*
	 * Initialize for processing of keys for attr 1.
	 *
	 * xform[i] points to the currently best scan key of strategy type i+1; it
	 * is NULL if we haven't yet found such a key for this attr.
	 */
	attno = 1;
	memset(xform, 0, sizeof(xform));

	/*
	 * Loop iterates from 0 to numberOfKeys inclusive; we use the last pass to
	 * handle after-last-key processing.  Actual exit from the loop is at the
	 * "break" statement below.
	 */
	for (i = 0;; cur++, i++)
	{
		if (i < numberOfKeys)
		{
			/* Apply indoption to scankey (might change sk_strategy!) */
			if (!_abt_fix_scankey_strategy(cur, indoption))
			{
				/* NULL can't be matched, so give up */
				so->qual_ok = false;
				return;
			}
		}

		/*
		 * If we are at the end of the keys for a particular attr, finish up
		 * processing and emit the cleaned-up keys.
		 */
		if (i == numberOfKeys || cur->sk_attno != attno)
		{
			int			priorNumberOfEqualCols = numberOfEqualCols;

			/* check input keys are correctly ordered */
			if (i < numberOfKeys && cur->sk_attno < attno)
				elog(ERROR, "btree index keys must be ordered by attribute");

			/*
			 * If = has been specified, all other keys can be eliminated as
			 * redundant.  If we have a case like key = 1 AND key > 2, we can
			 * set qual_ok to false and abandon further processing.
			 *
			 * We also have to deal with the case of "key IS NULL", which is
			 * unsatisfiable in combination with any other index condition. By
			 * the time we get here, that's been classified as an equality
			 * check, and we've rejected any combination of it with a regular
			 * equality condition; but not with other types of conditions.
			 */
			if (xform[BTEqualStrategyNumber - 1])
			{
				ScanKey		eq = xform[BTEqualStrategyNumber - 1];

				for (j = BTMaxStrategyNumber; --j >= 0;)
				{
					ScanKey		chk = xform[j];

					if (!chk || j == (BTEqualStrategyNumber - 1))
						continue;

					if (eq->sk_flags & SK_SEARCHNULL)
					{
						/* IS NULL is contradictory to anything else */
						so->qual_ok = false;
						return;
					}

					if (_abt_compare_scankey_args(scan, chk, eq, chk,
												 &test_result))
					{
						if (!test_result)
						{
							/* keys proven mutually contradictory */
							so->qual_ok = false;
							return;
						}
						/* else discard the redundant non-equality key */
						xform[j] = NULL;
					}
					/* else, cannot determine redundancy, keep both keys */
				}
				/* track number of attrs for which we have "=" keys */
				numberOfEqualCols++;
			}

			/* try to keep only one of <, <= */
			if (xform[BTLessStrategyNumber - 1]
				&& xform[BTLessEqualStrategyNumber - 1])
			{
				ScanKey		lt = xform[BTLessStrategyNumber - 1];
				ScanKey		le = xform[BTLessEqualStrategyNumber - 1];

				if (_abt_compare_scankey_args(scan, le, lt, le,
											 &test_result))
				{
					if (test_result)
						xform[BTLessEqualStrategyNumber - 1] = NULL;
					else
						xform[BTLessStrategyNumber - 1] = NULL;
				}
			}

			/* try to keep only one of >, >= */
			if (xform[BTGreaterStrategyNumber - 1]
				&& xform[BTGreaterEqualStrategyNumber - 1])
			{
				ScanKey		gt = xform[BTGreaterStrategyNumber - 1];
				ScanKey		ge = xform[BTGreaterEqualStrategyNumber - 1];

				if (_abt_compare_scankey_args(scan, ge, gt, ge,
											 &test_result))
				{
					if (test_result)
						xform[BTGreaterEqualStrategyNumber - 1] = NULL;
					else
						xform[BTGreaterStrategyNumber - 1] = NULL;
				}
			}

			/*
			 * Emit the cleaned-up keys into the outkeys[] array, and then
			 * mark them if they are required.  They are required (possibly
			 * only in one direction) if all attrs before this one had "=".
			 */
			for (j = BTMaxStrategyNumber; --j >= 0;)
			{
				if (xform[j])
				{
					ScanKey		outkey = &outkeys[new_numberOfKeys++];

					memcpy(outkey, xform[j], sizeof(ScanKeyData));
					if (priorNumberOfEqualCols == attno - 1)
						_abt_mark_scankey_required(outkey);
				}
			}

			/*
			 * Exit loop here if done.
			 */
			if (i == numberOfKeys)
				break;

			/* Re-initialize for new attno */
			attno = cur->sk_attno;
			memset(xform, 0, sizeof(xform));
		}

		/* check strategy this key's operator corresponds to */
		j = cur->sk_strategy - 1;

		/* if row comparison, push it directly to the output array */
		if (cur->sk_flags & SK_ROW_HEADER)
		{
			ScanKey		outkey = &outkeys[new_numberOfKeys++];

			memcpy(outkey, cur, sizeof(ScanKeyData));
			if (numberOfEqualCols == attno - 1)
				_abt_mark_scankey_required(outkey);

			/*
			 * We don't support RowCompare using equality; such a qual would
			 * mess up the numberOfEqualCols tracking.
			 */
			Assert(j != (BTEqualStrategyNumber - 1));
			continue;
		}

		/* have we seen one of these before? */
		if (xform[j] == NULL)
		{
			/* nope, so remember this scankey */
			xform[j] = cur;
		}
		else
		{
			/* yup, keep only the more restrictive key */
			if (_abt_compare_scankey_args(scan, cur, cur, xform[j],
										 &test_result))
			{
				if (test_result)
					xform[j] = cur;
				else if (j == (BTEqualStrategyNumber - 1))
				{
					/* key == a && key == b, but a != b */
					so->qual_ok = false;
					return;
				}
				/* else old key is more restrictive, keep it */
			}
			else
			{
				/*
				 * We can't determine which key is more restrictive.  Keep the
				 * previous one in xform[j] and push this one directly to the
				 * output array.
				 */
				ScanKey		outkey = &outkeys[new_numberOfKeys++];

				memcpy(outkey, cur, sizeof(ScanKeyData));
				if (numberOfEqualCols == attno - 1)
					_abt_mark_scankey_required(outkey);
			}
		}
	}

	so->numberOfKeys = new_numberOfKeys;
}

/*
 * Compare two scankey values using a specified operator.
 *
 * The test we want to perform is logically "leftarg op rightarg", where
 * leftarg and rightarg are the sk_argument values in those ScanKeys, and
 * the comparison operator is the one in the op ScanKey.  However, in
 * cross-data-type situations we may need to look up the correct operator in
 * the index's opfamily: it is the one having amopstrategy = op->sk_strategy
 * and amoplefttype/amoprighttype equal to the two argument datatypes.
 *
 * If the opfamily doesn't supply a complete set of cross-type operators we
 * may not be able to make the comparison.  If we can make the comparison
 * we store the operator result in *result and return true.  We return false
 * if the comparison could not be made.
 *
 * Note: op always points at the same ScanKey as either leftarg or rightarg.
 * Since we don't scribble on the scankeys, this aliasing should cause no
 * trouble.
 *
 * Note: this routine needs to be insensitive to any DESC option applied
 * to the index column.  For example, "x < 4" is a tighter constraint than
 * "x < 5" regardless of which way the index is sorted.
 */
static bool
_abt_compare_scankey_args(IndexScanDesc scan, ScanKey op,
						 ScanKey leftarg, ScanKey rightarg,
						 bool *result)
{
	Relation	rel = scan->indexRelation;
	Oid			lefttype,
				righttype,
				optype,
				opcintype,
				cmp_op;
	StrategyNumber strat;

	/*
	 * First, deal with cases where one or both args are NULL.  This should
	 * only happen when the scankeys represent IS NULL/NOT NULL conditions.
	 */
	if ((leftarg->sk_flags | rightarg->sk_flags) & SK_ISNULL)
	{
		bool		leftnull,
					rightnull;

		if (leftarg->sk_flags & SK_ISNULL)
		{
			Assert(leftarg->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL));
			leftnull = true;
		}
		else
			leftnull = false;
		if (rightarg->sk_flags & SK_ISNULL)
		{
			Assert(rightarg->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL));
			rightnull = true;
		}
		else
			rightnull = false;

		/*
		 * We treat NULL as either greater than or less than all other values.
		 * Since true > false, the tests below work correctly for NULLS LAST
		 * logic.  If the index is NULLS FIRST, we need to flip the strategy.
		 */
		strat = op->sk_strategy;
		if (op->sk_flags & SK_ABT_NULLS_FIRST)
			strat = ABTCommuteStrategyNumber(strat);

		switch (strat)
		{
			case BTLessStrategyNumber:
				*result = (leftnull < rightnull);
				break;
			case BTLessEqualStrategyNumber:
				*result = (leftnull <= rightnull);
				break;
			case BTEqualStrategyNumber:
				*result = (leftnull == rightnull);
				break;
			case BTGreaterEqualStrategyNumber:
				*result = (leftnull >= rightnull);
				break;
			case BTGreaterStrategyNumber:
				*result = (leftnull > rightnull);
				break;
			default:
				elog(ERROR, "unrecognized StrategyNumber: %d", (int) strat);
				*result = false;	/* keep compiler quiet */
				break;
		}
		return true;
	}

	/*
	 * The opfamily we need to worry about is identified by the index column.
	 */
	Assert(leftarg->sk_attno == rightarg->sk_attno);

	opcintype = rel->rd_opcintype[leftarg->sk_attno - 1];

	/*
	 * Determine the actual datatypes of the ScanKey arguments.  We have to
	 * support the convention that sk_subtype == InvalidOid means the opclass
	 * input type; this is a hack to simplify life for ScanKeyInit().
	 */
	lefttype = leftarg->sk_subtype;
	if (lefttype == InvalidOid)
		lefttype = opcintype;
	righttype = rightarg->sk_subtype;
	if (righttype == InvalidOid)
		righttype = opcintype;
	optype = op->sk_subtype;
	if (optype == InvalidOid)
		optype = opcintype;

	/*
	 * If leftarg and rightarg match the types expected for the "op" scankey,
	 * we can use its already-looked-up comparison function.
	 */
	if (lefttype == opcintype && righttype == optype)
	{
		*result = DatumGetBool(FunctionCall2Coll(&op->sk_func,
												 op->sk_collation,
												 leftarg->sk_argument,
												 rightarg->sk_argument));
		return true;
	}

	/*
	 * Otherwise, we need to go to the syscache to find the appropriate
	 * operator.  (This cannot result in infinite recursion, since no
	 * indexscan initiated by syscache lookup will use cross-data-type
	 * operators.)
	 *
	 * If the sk_strategy was flipped by _abt_fix_scankey_strategy, we have to
	 * un-flip it to get the correct opfamily member.
	 */
	strat = op->sk_strategy;
	if (op->sk_flags & SK_ABT_DESC)
		strat = ABTCommuteStrategyNumber(strat);

	cmp_op = get_opfamily_member(rel->rd_opfamily[leftarg->sk_attno - 1],
								 lefttype,
								 righttype,
								 strat);
	if (OidIsValid(cmp_op))
	{
		RegProcedure cmp_proc = get_opcode(cmp_op);

		if (RegProcedureIsValid(cmp_proc))
		{
			*result = DatumGetBool(OidFunctionCall2Coll(cmp_proc,
														op->sk_collation,
														leftarg->sk_argument,
														rightarg->sk_argument));
			return true;
		}
	}

	/* Can't make the comparison */
	*result = false;			/* suppress compiler warnings */
	return false;
}

/*
 * Adjust a scankey's strategy and flags setting as needed for indoptions.
 *
 * We copy the appropriate indoption value into the scankey sk_flags
 * (shifting to avoid clobbering system-defined flag bits).  Also, if
 * the DESC option is set, commute (flip) the operator strategy number.
 *
 * A secondary purpose is to check for IS NULL/NOT NULL scankeys and set up
 * the strategy field correctly for them.
 *
 * Lastly, for ordinary scankeys (not IS NULL/NOT NULL), we check for a
 * NULL comparison value.  Since all btree operators are assumed strict,
 * a NULL means that the qual cannot be satisfied.  We return true if the
 * comparison value isn't NULL, or false if the scan should be abandoned.
 *
 * This function is applied to the *input* scankey structure; therefore
 * on a rescan we will be looking at already-processed scankeys.  Hence
 * we have to be careful not to re-commute the strategy if we already did it.
 * It's a bit ugly to modify the caller's copy of the scankey but in practice
 * there shouldn't be any problem, since the index's indoptions are certainly
 * not going to change while the scankey survives.
 */
static bool
_abt_fix_scankey_strategy(ScanKey skey, int16 *indoption)
{
	int			addflags;

	addflags = indoption[skey->sk_attno - 1] << SK_ABT_INDOPTION_SHIFT;

	/*
	 * We treat all btree operators as strict (even if they're not so marked
	 * in pg_proc). This means that it is impossible for an operator condition
	 * with a NULL comparison constant to succeed, and we can reject it right
	 * away.
	 *
	 * However, we now also support "x IS NULL" clauses as search conditions,
	 * so in that case keep going. The planner has not filled in any
	 * particular strategy in this case, so set it to BTEqualStrategyNumber
	 * --- we can treat IS NULL as an equality operator for purposes of search
	 * strategy.
	 *
	 * Likewise, "x IS NOT NULL" is supported.  We treat that as either "less
	 * than NULL" in a NULLS LAST index, or "greater than NULL" in a NULLS
	 * FIRST index.
	 *
	 * Note: someday we might have to fill in sk_collation from the index
	 * column's collation.  At the moment this is a non-issue because we'll
	 * never actually call the comparison operator on a NULL.
	 */
	if (skey->sk_flags & SK_ISNULL)
	{
		/* SK_ISNULL shouldn't be set in a row header scankey */
		Assert(!(skey->sk_flags & SK_ROW_HEADER));

		/* Set indoption flags in scankey (might be done already) */
		skey->sk_flags |= addflags;

		/* Set correct strategy for IS NULL or NOT NULL search */
		if (skey->sk_flags & SK_SEARCHNULL)
		{
			skey->sk_strategy = BTEqualStrategyNumber;
			skey->sk_subtype = InvalidOid;
			skey->sk_collation = InvalidOid;
		}
		else if (skey->sk_flags & SK_SEARCHNOTNULL)
		{
			if (skey->sk_flags & SK_ABT_NULLS_FIRST)
				skey->sk_strategy = BTGreaterStrategyNumber;
			else
				skey->sk_strategy = BTLessStrategyNumber;
			skey->sk_subtype = InvalidOid;
			skey->sk_collation = InvalidOid;
		}
		else
		{
			/* regular qual, so it cannot be satisfied */
			return false;
		}

		/* Needn't do the rest */
		return true;
	}

	/* Adjust strategy for DESC, if we didn't already */
	if ((addflags & SK_ABT_DESC) && !(skey->sk_flags & SK_ABT_DESC))
		skey->sk_strategy = ABTCommuteStrategyNumber(skey->sk_strategy);
	skey->sk_flags |= addflags;

	/* If it's a row header, fix row member flags and strategies similarly */
	if (skey->sk_flags & SK_ROW_HEADER)
	{
		ScanKey		subkey = (ScanKey) DatumGetPointer(skey->sk_argument);

		for (;;)
		{
			Assert(subkey->sk_flags & SK_ROW_MEMBER);
			addflags = indoption[subkey->sk_attno - 1] << SK_ABT_INDOPTION_SHIFT;
			if ((addflags & SK_ABT_DESC) && !(subkey->sk_flags & SK_ABT_DESC))
				subkey->sk_strategy = ABTCommuteStrategyNumber(subkey->sk_strategy);
			subkey->sk_flags |= addflags;
			if (subkey->sk_flags & SK_ROW_END)
				break;
			subkey++;
		}
	}

	return true;
}

/*
 * Mark a scankey as "required to continue the scan".
 *
 * Depending on the operator type, the key may be required for both scan
 * directions or just one.  Also, if the key is a row comparison header,
 * we have to mark its first subsidiary ScanKey as required.  (Subsequent
 * subsidiary ScanKeys are normally for lower-order columns, and thus
 * cannot be required, since they're after the first non-equality scankey.)
 *
 * Note: when we set required-key flag bits in a subsidiary scankey, we are
 * scribbling on a data structure belonging to the index AM's caller, not on
 * our private copy.  This should be OK because the marking will not change
 * from scan to scan within a query, and so we'd just re-mark the same way
 * anyway on a rescan.  Something to keep an eye on though.
 */
static void
_abt_mark_scankey_required(ScanKey skey)
{
	int			addflags;

	switch (skey->sk_strategy)
	{
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			addflags = SK_ABT_REQFWD;
			break;
		case BTEqualStrategyNumber:
			addflags = SK_ABT_REQFWD | SK_ABT_REQBKWD;
			break;
		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			addflags = SK_ABT_REQBKWD;
			break;
		default:
			elog(ERROR, "unrecognized StrategyNumber: %d",
				 (int) skey->sk_strategy);
			addflags = 0;		/* keep compiler quiet */
			break;
	}

	skey->sk_flags |= addflags;

	if (skey->sk_flags & SK_ROW_HEADER)
	{
		ScanKey		subkey = (ScanKey) DatumGetPointer(skey->sk_argument);

		/* First subkey should be same column/operator as the header */
		Assert(subkey->sk_flags & SK_ROW_MEMBER);
		Assert(subkey->sk_attno == skey->sk_attno);
		Assert(subkey->sk_strategy == skey->sk_strategy);
		subkey->sk_flags |= addflags;
	}
}

/*
 * Test whether an indextuple satisfies all the scankey conditions.
 *
 * Return true if so, false if not.  If the tuple fails to pass the qual,
 * we also determine whether there's any need to continue the scan beyond
 * this tuple, and set *continuescan accordingly.  See comments for
 * _abt_preprocess_keys(), above, about how this is done.
 *
 * Forward scan callers can pass a high key tuple in the hopes of having
 * us set *continuescan to false, and avoiding an unnecessary visit to
 * the page to the right.
 *
 * scan: index scan descriptor (containing a search-type scankey)
 * tuple: index tuple to test
 * tupnatts: number of attributes in tupnatts (high key may be truncated)
 * dir: direction we are scanning in
 * continuescan: output parameter (will be set correctly in all cases)
 */
bool
_abt_checkkeys(IndexScanDesc scan, IndexTuple tuple, int tupnatts,
			  ScanDirection dir, bool *continuescan)
{
	TupleDesc	tupdesc;
	ABTScanOpaque so;
	int			keysz;
	int			ikey;
	ScanKey		key;

	Assert(ABTreeTupleGetNAtts(tuple, scan->indexRelation) == tupnatts);

	*continuescan = true;		/* default assumption */

	tupdesc = RelationGetDescr(scan->indexRelation);
	so = (ABTScanOpaque) scan->opaque;
	keysz = so->numberOfKeys;

	for (key = so->keyData, ikey = 0; ikey < keysz; key++, ikey++)
	{
		Datum		datum;
		bool		isNull;
		Datum		test;

		if (key->sk_attno > tupnatts)
		{
			/*
			 * This attribute is truncated (must be high key).  The value for
			 * this attribute in the first non-pivot tuple on the page to the
			 * right could be any possible value.  Assume that truncated
			 * attribute passes the qual.
			 */
			Assert(ScanDirectionIsForward(dir));
			Assert(ABTreeTupleIsPivot(tuple));
			continue;
		}

		/* row-comparison keys need special processing */
		if (key->sk_flags & SK_ROW_HEADER)
		{
			if (_abt_check_rowcompare(key, tuple, tupnatts, tupdesc, dir,
									 continuescan))
				continue;
			return false;
		}

		datum = index_getattr(tuple,
							  key->sk_attno,
							  tupdesc,
							  &isNull);

		if (key->sk_flags & SK_ISNULL)
		{
			/* Handle IS NULL/NOT NULL tests */
			if (key->sk_flags & SK_SEARCHNULL)
			{
				if (isNull)
					continue;	/* tuple satisfies this qual */
			}
			else
			{
				Assert(key->sk_flags & SK_SEARCHNOTNULL);
				if (!isNull)
					continue;	/* tuple satisfies this qual */
			}

			/*
			 * Tuple fails this qual.  If it's a required qual for the current
			 * scan direction, then we can conclude no further tuples will
			 * pass, either.
			 */
			if ((key->sk_flags & SK_ABT_REQFWD) &&
				ScanDirectionIsForward(dir))
				*continuescan = false;
			else if ((key->sk_flags & SK_ABT_REQBKWD) &&
					 ScanDirectionIsBackward(dir))
				*continuescan = false;

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}

		if (isNull)
		{
			if (key->sk_flags & SK_ABT_NULLS_FIRST)
			{
				/*
				 * Since NULLs are sorted before non-NULLs, we know we have
				 * reached the lower limit of the range of values for this
				 * index attr.  On a backward scan, we can stop if this qual
				 * is one of the "must match" subset.  We can stop regardless
				 * of whether the qual is > or <, so long as it's required,
				 * because it's not possible for any future tuples to pass. On
				 * a forward scan, however, we must keep going, because we may
				 * have initially positioned to the start of the index.
				 */
				if ((key->sk_flags & (SK_ABT_REQFWD | SK_ABT_REQBKWD)) &&
					ScanDirectionIsBackward(dir))
					*continuescan = false;
			}
			else
			{
				/*
				 * Since NULLs are sorted after non-NULLs, we know we have
				 * reached the upper limit of the range of values for this
				 * index attr.  On a forward scan, we can stop if this qual is
				 * one of the "must match" subset.  We can stop regardless of
				 * whether the qual is > or <, so long as it's required,
				 * because it's not possible for any future tuples to pass. On
				 * a backward scan, however, we must keep going, because we
				 * may have initially positioned to the end of the index.
				 */
				if ((key->sk_flags & (SK_ABT_REQFWD | SK_ABT_REQBKWD)) &&
					ScanDirectionIsForward(dir))
					*continuescan = false;
			}

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}

		test = FunctionCall2Coll(&key->sk_func, key->sk_collation,
								 datum, key->sk_argument);

		if (!DatumGetBool(test))
		{
			/*
			 * Tuple fails this qual.  If it's a required qual for the current
			 * scan direction, then we can conclude no further tuples will
			 * pass, either.
			 *
			 * Note: because we stop the scan as soon as any required equality
			 * qual fails, it is critical that equality quals be used for the
			 * initial positioning in _abt_first() when they are available. See
			 * comments in _abt_first().
			 */
			if ((key->sk_flags & SK_ABT_REQFWD) &&
				ScanDirectionIsForward(dir))
				*continuescan = false;
			else if ((key->sk_flags & SK_ABT_REQBKWD) &&
					 ScanDirectionIsBackward(dir))
				*continuescan = false;

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}
	}

	/* If we get here, the tuple passes all index quals. */
	return true;
}

/*
 * Test whether an indextuple satisfies a row-comparison scan condition.
 *
 * Return true if so, false if not.  If not, also clear *continuescan if
 * it's not possible for any future tuples in the current scan direction
 * to pass the qual.
 *
 * This is a subroutine for _abt_checkkeys, which see for more info.
 */
static bool
_abt_check_rowcompare(ScanKey skey, IndexTuple tuple, int tupnatts,
					 TupleDesc tupdesc, ScanDirection dir, bool *continuescan)
{
	ScanKey		subkey = (ScanKey) DatumGetPointer(skey->sk_argument);
	int32		cmpresult = 0;
	bool		result;

	/* First subkey should be same as the header says */
	Assert(subkey->sk_attno == skey->sk_attno);

	/* Loop over columns of the row condition */
	for (;;)
	{
		Datum		datum;
		bool		isNull;

		Assert(subkey->sk_flags & SK_ROW_MEMBER);

		if (subkey->sk_attno > tupnatts)
		{
			/*
			 * This attribute is truncated (must be high key).  The value for
			 * this attribute in the first non-pivot tuple on the page to the
			 * right could be any possible value.  Assume that truncated
			 * attribute passes the qual.
			 */
			Assert(ScanDirectionIsForward(dir));
			Assert(ABTreeTupleIsPivot(tuple));
			cmpresult = 0;
			if (subkey->sk_flags & SK_ROW_END)
				break;
			subkey++;
			continue;
		}

		datum = index_getattr(tuple,
							  subkey->sk_attno,
							  tupdesc,
							  &isNull);

		if (isNull)
		{
			if (subkey->sk_flags & SK_ABT_NULLS_FIRST)
			{
				/*
				 * Since NULLs are sorted before non-NULLs, we know we have
				 * reached the lower limit of the range of values for this
				 * index attr.  On a backward scan, we can stop if this qual
				 * is one of the "must match" subset.  We can stop regardless
				 * of whether the qual is > or <, so long as it's required,
				 * because it's not possible for any future tuples to pass. On
				 * a forward scan, however, we must keep going, because we may
				 * have initially positioned to the start of the index.
				 */
				if ((subkey->sk_flags & (SK_ABT_REQFWD | SK_ABT_REQBKWD)) &&
					ScanDirectionIsBackward(dir))
					*continuescan = false;
			}
			else
			{
				/*
				 * Since NULLs are sorted after non-NULLs, we know we have
				 * reached the upper limit of the range of values for this
				 * index attr.  On a forward scan, we can stop if this qual is
				 * one of the "must match" subset.  We can stop regardless of
				 * whether the qual is > or <, so long as it's required,
				 * because it's not possible for any future tuples to pass. On
				 * a backward scan, however, we must keep going, because we
				 * may have initially positioned to the end of the index.
				 */
				if ((subkey->sk_flags & (SK_ABT_REQFWD | SK_ABT_REQBKWD)) &&
					ScanDirectionIsForward(dir))
					*continuescan = false;
			}

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}

		if (subkey->sk_flags & SK_ISNULL)
		{
			/*
			 * Unlike the simple-scankey case, this isn't a disallowed case.
			 * But it can never match.  If all the earlier row comparison
			 * columns are required for the scan direction, we can stop the
			 * scan, because there can't be another tuple that will succeed.
			 */
			if (subkey != (ScanKey) DatumGetPointer(skey->sk_argument))
				subkey--;
			if ((subkey->sk_flags & SK_ABT_REQFWD) &&
				ScanDirectionIsForward(dir))
				*continuescan = false;
			else if ((subkey->sk_flags & SK_ABT_REQBKWD) &&
					 ScanDirectionIsBackward(dir))
				*continuescan = false;
			return false;
		}

		/* Perform the test --- three-way comparison not bool operator */
		cmpresult = DatumGetInt32(FunctionCall2Coll(&subkey->sk_func,
													subkey->sk_collation,
													datum,
													subkey->sk_argument));

		if (subkey->sk_flags & SK_ABT_DESC)
			INVERT_COMPARE_RESULT(cmpresult);

		/* Done comparing if unequal, else advance to next column */
		if (cmpresult != 0)
			break;

		if (subkey->sk_flags & SK_ROW_END)
			break;
		subkey++;
	}

	/*
	 * At this point cmpresult indicates the overall result of the row
	 * comparison, and subkey points to the deciding column (or the last
	 * column if the result is "=").
	 */
	switch (subkey->sk_strategy)
	{
			/* EQ and NE cases aren't allowed here */
		case BTLessStrategyNumber:
			result = (cmpresult < 0);
			break;
		case BTLessEqualStrategyNumber:
			result = (cmpresult <= 0);
			break;
		case BTGreaterEqualStrategyNumber:
			result = (cmpresult >= 0);
			break;
		case BTGreaterStrategyNumber:
			result = (cmpresult > 0);
			break;
		default:
			elog(ERROR, "unrecognized RowCompareType: %d",
				 (int) subkey->sk_strategy);
			result = 0;			/* keep compiler quiet */
			break;
	}

	if (!result)
	{
		/*
		 * Tuple fails this qual.  If it's a required qual for the current
		 * scan direction, then we can conclude no further tuples will pass,
		 * either.  Note we have to look at the deciding column, not
		 * necessarily the first or last column of the row condition.
		 */
		if ((subkey->sk_flags & SK_ABT_REQFWD) &&
			ScanDirectionIsForward(dir))
			*continuescan = false;
		else if ((subkey->sk_flags & SK_ABT_REQBKWD) &&
				 ScanDirectionIsBackward(dir))
			*continuescan = false;
	}

	return result;
}

/*
 * _abt_killitems - set LP_DEAD state for items an indexscan caller has
 * told us were killed
 *
 * scan->opaque, referenced locally through so, contains information about the
 * current page and killed tuples thereon (generally, this should only be
 * called if so->numKilled > 0).
 *
 * The caller does not have a lock on the page and may or may not have the
 * page pinned in a buffer.  Note that read-lock is sufficient for setting
 * LP_DEAD status (which is only a hint).
 *
 * We match items by heap TID before assuming they are the right ones to
 * delete.  We cope with cases where items have moved right due to insertions.
 * If an item has moved off the current page due to a split, we'll fail to
 * find it and do nothing (this is not an error case --- we assume the item
 * will eventually get marked in a future indexscan).
 *
 * Note that if we hold a pin on the target page continuously from initially
 * reading the items until applying this function, VACUUM cannot have deleted
 * any items from the page, and so there is no need to search left from the
 * recorded offset.  (This observation also guarantees that the item is still
 * the right one to delete, which might otherwise be questionable since heap
 * TIDs can get recycled.)	This holds true even if the page has been modified
 * by inserts and page splits, so there is no need to consult the LSN.
 *
 * If the pin was released after reading the page, then we re-read it.  If it
 * has been modified since we read it (as determined by the LSN), we dare not
 * flag any entries because it is possible that the old entry was vacuumed
 * away and the TID was re-used by a completely different heap tuple.
 */
void
_abt_killitems(IndexScanDesc scan)
{
	ABTScanOpaque so = (ABTScanOpaque) scan->opaque;
	Page		page;
	ABTPageOpaque opaque;
	OffsetNumber minoff;
	OffsetNumber maxoff;
	int			i;
	int			numKilled = so->numKilled;
	bool		killedsomething = false;
	bool		droppedpin PG_USED_FOR_ASSERTS_ONLY;

	Assert(ABTScanPosIsValid(so->currPos));

	/*
	 * Always reset the scan state, so we don't look for same items on other
	 * pages.
	 */
	so->numKilled = 0;

	if (ABTScanPosIsPinned(so->currPos))
	{
		/*
		 * We have held the pin on this page since we read the index tuples,
		 * so all we need to do is lock it.  The pin will have prevented
		 * re-use of any TID on the page, so there is no need to check the
		 * LSN.
		 */
		droppedpin = false;
		LockBuffer(so->currPos.buf, ABT_READ);

		page = BufferGetPage(so->currPos.buf);
	}
	else
	{
		Buffer		buf;

		droppedpin = true;
		/* Attempt to re-read the buffer, getting pin and lock. */
		buf = _abt_getbuf(scan->indexRelation, so->currPos.currPage, ABT_READ);

		page = BufferGetPage(buf);
		if (BufferGetLSNAtomic(buf) == so->currPos.lsn)
			so->currPos.buf = buf;
		else
		{
			/* Modified while not pinned means hinting is not safe. */
			_abt_relbuf(scan->indexRelation, buf);
			return;
		}
	}

	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	minoff = ABT_P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);

	for (i = 0; i < numKilled; i++)
	{
		int			itemIndex = so->killedItems[i];
		ABTScanPosItem *kitem = &so->currPos.items[itemIndex];
		OffsetNumber offnum = kitem->indexOffset;

		Assert(itemIndex >= so->currPos.firstItem &&
			   itemIndex <= so->currPos.lastItem);
		if (offnum < minoff)
			continue;			/* pure paranoia */
		while (offnum <= maxoff)
		{
			ItemId		iid = PageGetItemId(page, offnum);
			IndexTuple	ituple = (IndexTuple) PageGetItem(page, iid);
			bool		killtuple = false;

			if (ABTreeTupleIsPosting(ituple))
			{
				int			pi = i + 1;
				int			nposting = ABTreeTupleGetNPosting(ituple);
				int			j;

				/*
				 * We rely on the convention that heap TIDs in the scanpos
				 * items array are stored in ascending heap TID order for a
				 * group of TIDs that originally came from a posting list
				 * tuple.  This convention even applies during backwards
				 * scans, where returning the TIDs in descending order might
				 * seem more natural.  This is about effectiveness, not
				 * correctness.
				 *
				 * Note that the page may have been modified in almost any way
				 * since we first read it (in the !droppedpin case), so it's
				 * possible that this posting list tuple wasn't a posting list
				 * tuple when we first encountered its heap TIDs.
				 */
				for (j = 0; j < nposting; j++)
				{
					ItemPointer item = ABTreeTupleGetPostingN(ituple, j);

					if (!ItemPointerEquals(item, &kitem->heapTid))
						break;	/* out of posting list loop */

					/*
					 * kitem must have matching offnum when heap TIDs match,
					 * though only in the common case where the page can't
					 * have been concurrently modified
					 */
					Assert(kitem->indexOffset == offnum || !droppedpin);

					/*
					 * Read-ahead to later kitems here.
					 *
					 * We rely on the assumption that not advancing kitem here
					 * will prevent us from considering the posting list tuple
					 * fully dead by not matching its next heap TID in next
					 * loop iteration.
					 *
					 * If, on the other hand, this is the final heap TID in
					 * the posting list tuple, then tuple gets killed
					 * regardless (i.e. we handle the case where the last
					 * kitem is also the last heap TID in the last index tuple
					 * correctly -- posting tuple still gets killed).
					 */
					if (pi < numKilled)
						kitem = &so->currPos.items[so->killedItems[pi++]];
				}

				/*
				 * Don't bother advancing the outermost loop's int iterator to
				 * avoid processing killed items that relate to the same
				 * offnum/posting list tuple.  This micro-optimization hardly
				 * seems worth it.  (Further iterations of the outermost loop
				 * will fail to match on this same posting list's first heap
				 * TID instead, so we'll advance to the next offnum/index
				 * tuple pretty quickly.)
				 */
				if (j == nposting)
					killtuple = true;
			}
			else if (ItemPointerEquals(&ituple->t_tid, &kitem->heapTid))
				killtuple = true;

			/*
			 * Mark index item as dead, if it isn't already.  Since this
			 * happens while holding a buffer lock possibly in shared mode,
			 * it's possible that multiple processes attempt to do this
			 * simultaneously, leading to multiple full-page images being sent
			 * to WAL (if wal_log_hints or data checksums are enabled), which
			 * is undesirable.
			 */
			if (killtuple && !ItemIdIsDead(iid))
			{
				/* found the item/all posting list items */
				ItemIdMarkDead(iid);
				killedsomething = true;
				break;			/* out of inner search loop */
			}
			offnum = OffsetNumberNext(offnum);
		}
	}

	/*
	 * Since this can be redone later if needed, mark as dirty hint.
	 *
	 * Whenever we mark anything LP_DEAD, we also set the page's
	 * ABTP_HAS_GARBAGE flag, which is likewise just a hint.
	 */
	if (killedsomething)
	{
		opaque->abtpo_flags |= ABTP_HAS_GARBAGE;
		MarkBufferDirtyHint(so->currPos.buf, true);
	}

	LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
}


/*
 * The following routines manage a shared-memory area in which we track
 * assignment of "vacuum cycle IDs" to currently-active btree vacuuming
 * operations.  There is a single counter which increments each time we
 * start a vacuum to assign it a cycle ID.  Since multiple vacuums could
 * be active concurrently, we have to track the cycle ID for each active
 * vacuum; this requires at most MaxBackends entries (usually far fewer).
 * We assume at most one vacuum can be active for a given index.
 *
 * Access to the shared memory area is controlled by ABtreeVacuumLock.
 * In principle we could use a separate lmgr locktag for each index,
 * but a single LWLock is much cheaper, and given the short time that
 * the lock is ever held, the concurrency hit should be minimal.
 */

typedef struct ABTOneVacInfo
{
	LockRelId	relid;			/* global identifier of an index */
	ABTCycleId	cycleid;		/* cycle ID for its active VACUUM */
} ABTOneVacInfo;

typedef struct ABTVacInfo
{
	ABTCycleId	cycle_ctr;		/* cycle ID most recently assigned */
	int			num_vacuums;	/* number of currently active VACUUMs */
	int			max_vacuums;	/* allocated length of vacuums[] array */
	ABTOneVacInfo vacuums[FLEXIBLE_ARRAY_MEMBER];
} ABTVacInfo;

static ABTVacInfo *abtvacinfo;


/*
 * _abt_vacuum_cycleid --- get the active vacuum cycle ID for an index,
 *		or zero if there is no active VACUUM
 *
 * Note: for correct interlocking, the caller must already hold pin and
 * exclusive lock on each buffer it will store the cycle ID into.  This
 * ensures that even if a VACUUM starts immediately afterwards, it cannot
 * process those pages until the page split is complete.
 */
ABTCycleId
_abt_vacuum_cycleid(Relation rel)
{
	ABTCycleId	result = 0;
	int			i;

	/* Share lock is enough since this is a read-only operation */
	LWLockAcquire(ABtreeVacuumLock, LW_SHARED);

	for (i = 0; i < abtvacinfo->num_vacuums; i++)
	{
		ABTOneVacInfo *vac = &abtvacinfo->vacuums[i];

		if (vac->relid.relId == rel->rd_lockInfo.lockRelId.relId &&
			vac->relid.dbId == rel->rd_lockInfo.lockRelId.dbId)
		{
			result = vac->cycleid;
			break;
		}
	}

	LWLockRelease(ABtreeVacuumLock);
	return result;
}

/*
 * _abt_start_vacuum --- assign a cycle ID to a just-starting VACUUM operation
 *
 * Note: the caller must guarantee that it will eventually call
 * _abt_end_vacuum, else we'll permanently leak an array slot.  To ensure
 * that this happens even in elog(FATAL) scenarios, the appropriate coding
 * is not just a PG_TRY, but
 *		PG_ENSURE_ERROR_CLEANUP(_abt_end_vacuum_callback, PointerGetDatum(rel))
 */
ABTCycleId
_abt_start_vacuum(Relation rel)
{
	ABTCycleId	result;
	int			i;
	ABTOneVacInfo *vac;

	LWLockAcquire(ABtreeVacuumLock, LW_EXCLUSIVE);

	/*
	 * Assign the next cycle ID, being careful to avoid zero as well as the
	 * reserved high values.
	 */
	result = ++(abtvacinfo->cycle_ctr);
	if (result == 0 || result > MAX_ABT_CYCLE_ID)
		result = abtvacinfo->cycle_ctr = 1;

	/* Let's just make sure there's no entry already for this index */
	for (i = 0; i < abtvacinfo->num_vacuums; i++)
	{
		vac = &abtvacinfo->vacuums[i];
		if (vac->relid.relId == rel->rd_lockInfo.lockRelId.relId &&
			vac->relid.dbId == rel->rd_lockInfo.lockRelId.dbId)
		{
			/*
			 * Unlike most places in the backend, we have to explicitly
			 * release our LWLock before throwing an error.  This is because
			 * we expect _abt_end_vacuum() to be called before transaction
			 * abort cleanup can run to release LWLocks.
			 */
			LWLockRelease(ABtreeVacuumLock);
			elog(ERROR, "multiple active vacuums for index \"%s\"",
				 RelationGetRelationName(rel));
		}
	}

	/* OK, add an entry */
	if (abtvacinfo->num_vacuums >= abtvacinfo->max_vacuums)
	{
		LWLockRelease(ABtreeVacuumLock);
		elog(ERROR, "out of abtvacinfo slots");
	}
	vac = &abtvacinfo->vacuums[abtvacinfo->num_vacuums];
	vac->relid = rel->rd_lockInfo.lockRelId;
	vac->cycleid = result;
	abtvacinfo->num_vacuums++;

	LWLockRelease(ABtreeVacuumLock);
	return result;
}

/*
 * _abt_end_vacuum --- mark a btree VACUUM operation as done
 *
 * Note: this is deliberately coded not to complain if no entry is found;
 * this allows the caller to put PG_TRY around the start_vacuum operation.
 */
void
_abt_end_vacuum(Relation rel)
{
	int			i;

	LWLockAcquire(ABtreeVacuumLock, LW_EXCLUSIVE);

	/* Find the array entry */
	for (i = 0; i < abtvacinfo->num_vacuums; i++)
	{
		ABTOneVacInfo *vac = &abtvacinfo->vacuums[i];

		if (vac->relid.relId == rel->rd_lockInfo.lockRelId.relId &&
			vac->relid.dbId == rel->rd_lockInfo.lockRelId.dbId)
		{
			/* Remove it by shifting down the last entry */
			*vac = abtvacinfo->vacuums[abtvacinfo->num_vacuums - 1];
			abtvacinfo->num_vacuums--;
			break;
		}
	}

	LWLockRelease(ABtreeVacuumLock);
}

/*
 * _abt_end_vacuum wrapped as an on_shmem_exit callback function
 */
void
_abt_end_vacuum_callback(int code, Datum arg)
{
	_abt_end_vacuum((Relation) DatumGetPointer(arg));
}

/*
 * ABTreeShmemSize --- report amount of shared memory space needed
 */
Size
ABTreeShmemSize(void)
{
	Size		size;

	size = offsetof(ABTVacInfo, vacuums);
	size = add_size(size, mul_size(MaxBackends, sizeof(ABTOneVacInfo)));

    if (abtree_enable_metrics_collection)
    {
        size = MAXALIGN(size);
        size = add_size(size,
                        mul_size(3 * MaxBackends, sizeof(pg_atomic_uint64)));
    }
	return size;
}

/*
 * ABTreeShmemInit --- initialize this module's shared memory
 */
void
ABTreeShmemInit(void)
{
	bool		found;

	abtvacinfo = (ABTVacInfo *) ShmemInitStruct("ABTree Vacuum State",
											  ABTreeShmemSize(),
											  &found);
    if (abtree_enable_metrics_collection)
    {
        Size off = offsetof(ABTVacInfo, vacuums);
        off = add_size(off, mul_size(MaxBackends, sizeof(ABTOneVacInfo)));
        off = MAXALIGN(off);
        abtree_ninserts = (pg_atomic_uint64 *)(((char *) abtvacinfo) + off);

        off = add_size(off, mul_size(MaxBackends, sizeof(pg_atomic_uint64)));
        abtree_nsamples_accepted =
            (pg_atomic_uint64 *)(((char *) abtvacinfo) + off);

        off = add_size(off, mul_size(MaxBackends, sizeof(pg_atomic_uint64)));
        abtree_nsamples_rejected =
            (pg_atomic_uint64 *)(((char *) abtvacinfo) + off);
    }

	if (!IsUnderPostmaster)
	{
		/* Initialize shared memory area */
		Assert(!found);

		/*
		 * It doesn't really matter what the cycle counter starts at, but
		 * having it always start the same doesn't seem good.  Seed with
		 * low-order bits of time() instead.
		 */
		abtvacinfo->cycle_ctr = (ABTCycleId) time(NULL);

		abtvacinfo->num_vacuums = 0;
		abtvacinfo->max_vacuums = MaxBackends;

        if (abtree_enable_metrics_collection)
        {
            int i;

            for (i = 0; i < (int) MaxBackends; ++i)
            {
                pg_atomic_init_u64(&abtree_ninserts[i], 0);
                pg_atomic_init_u64(&abtree_nsamples_accepted[i], 0);
                pg_atomic_init_u64(&abtree_nsamples_rejected[i], 0);
            }
        }
	}
	else
		Assert(found);
}

bytea *
abtoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"fillfactor", RELOPT_TYPE_INT, offsetof(ABTOptions, fillfactor)},
		{"vacuum_cleanup_index_scale_factor", RELOPT_TYPE_REAL,
		offsetof(ABTOptions, vacuum_cleanup_index_scale_factor)},
		{"deduplicate_items", RELOPT_TYPE_BOOL,
		offsetof(ABTOptions, deduplicate_items)},

		{"aggregation_type", RELOPT_TYPE_TYP,
		offsetof(ABTOptions, aggregation_type)},
		{"agg_support", RELOPT_TYPE_PROC,
		offsetof(ABTOptions, agg_support_fn)},
	};

	ABTOptions *options =
		(ABTOptions *) build_reloptions(reloptions, validate,
										RELOPT_KIND_ABTREE,
										sizeof(ABTOptions),
										tab, lengthof(tab));

	if (validate)
	{
		HeapTuple		htup;
		Form_pg_type	agg_type;
		char			agg_type_name[NAMEDATALEN];
		Oid				agg_support_rettype;

		/* We currently require aggregation type to be a numeric type. */
		htup = SearchSysCache1(TYPEOID, options->aggregation_type.typeOid);
		if (!HeapTupleIsValid(htup))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("aggregation_type %u is not found",
							options->aggregation_type.typeOid)));
		}
		agg_type = (Form_pg_type) GETSTRUCT(htup);
		strncpy(agg_type_name, NameStr(agg_type->typname), NAMEDATALEN);

		if (agg_type->typcategory != TYPCATEGORY_NUMERIC)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("aggregation_type \"%s\" is not a numeric type",
						NameStr(agg_type->typname))));
		}

		if (agg_type->typlen < 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("aggregation_type \"%s\" is not fixed length",
						 NameStr(agg_type->typname))));
		}
	
		/* XXX only support int2, int4 and int8 right now */
		if (agg_type->oid != INT2OID && agg_type->oid != INT4OID &&
			agg_type->oid != INT8OID)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("only int2, int4, int8 are supported as "
							"abtree aggregation type right now")));
		}

		if (!agg_type->typbyval)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("int8 is not passed by value")));
		}

		ReleaseSysCache(htup);
	
		agg_support_rettype = get_func_rettype(options->agg_support_fn);
		if (agg_support_rettype != ABTREE_AGG_SUPPORTOID)
		{
			HeapTuple	htup;
			char		*typname;
			htup = SearchSysCache1(TYPEOID, agg_support_rettype);
			if (!HeapTupleIsValid(htup))
			{
				typname = palloc(17);
				snprintf(typname, 17, "oid = %u", agg_support_rettype);
			}
			else
			{
				typname = NameStr(((Form_pg_type) GETSTRUCT(htup))->typname);
			}

			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("agg_support function returns %s instead of "
						"abtree_agg_support type",
						typname)));
		}
	}

	return (bytea*) options;
}

void
abt_get_arg_info_for_combine_fn(void *options,
								int *nargs,
								Oid *argtypes) {
	ABTOptions *abt_options = (ABTOptions *) options;
	
	*nargs = 2;
	argtypes[0] = abt_options->aggregation_type.typeOid;
	argtypes[1] = abt_options->aggregation_type.typeOid;
}

/*
 *	btproperty() -- Check boolean properties of indexes.
 *
 * This is optional, but handling AMPROP_RETURNABLE here saves opening the rel
 * to call btcanreturn.
 */
bool
abtproperty(Oid index_oid, int attno,
		   IndexAMProperty prop, const char *propname,
		   bool *res, bool *isnull)
{
	switch (prop)
	{
		case AMPROP_RETURNABLE:
			/* answer only for columns, not AM or whole index */
			if (attno == 0)
				return false;
			/* otherwise, btree can always return data */
			*res = true;
			return true;

		default:
			return false;		/* punt to generic code */
	}
}

/*
 *	btbuildphasename() -- Return name of index build phase.
 */
char *
abtbuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		case PROGRESS_ABTREE_PHASE_INDEXBUILD_TABLESCAN:
			return "scanning table";
		case PROGRESS_ABTREE_PHASE_PERFORMSORT_1:
			return "sorting live tuples";
		case PROGRESS_ABTREE_PHASE_PERFORMSORT_2:
			return "sorting dead tuples";
		case PROGRESS_ABTREE_PHASE_LEAF_LOAD:
			return "loading tuples in tree";
		default:
			return NULL;
	}
}

/*
 *	_abt_truncate() -- create tuple without unneeded suffix attributes.
 *
 * Returns truncated pivot index tuple allocated in caller's memory context,
 * with key attributes copied from caller's firstright argument.  If rel is
 * an INCLUDE index, non-key attributes will definitely be truncated away,
 * since they're not part of the key space.  More aggressive suffix
 * truncation can take place when it's clear that the returned tuple does not
 * need one or more suffix key attributes.  We only need to keep firstright
 * attributes up to and including the first non-lastleft-equal attribute.
 * Caller's insertion scankey is used to compare the tuples; the scankey's
 * argument values are not considered here.
 *
 * Note that returned tuple's t_tid offset will hold the number of attributes
 * present, so the original item pointer offset is not represented.  Caller
 * should only change truncated tuple's downlink.  Note also that truncated
 * key attributes are treated as containing "minus infinity" values by
 * _abt_compare().
 *
 * In the worst case (when a heap TID must be appended to distinguish lastleft
 * from firstright), the size of the returned tuple is the size of firstright
 * plus the size of an additional MAXALIGN()'d item pointer.  This guarantee
 * is important, since callers need to stay under the 1/3 of a page
 * restriction on tuple size.  If this routine is ever taught to truncate
 * within an attribute/datum, it will need to avoid returning an enlarged
 * tuple to caller when truncation + TOAST compression ends up enlarging the
 * final datum.
 */
IndexTuple
_abt_truncate(Relation rel, IndexTuple lastleft, IndexTuple firstright,
			 ABTScanInsert itup_key, ABTCachedAggInfo agg_info)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int16		nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	int			keepnatts;
	IndexTuple	pivot;
	ItemPointer pivotheaptid;

	/*
	 * We should only ever truncate non-pivot tuples from leaf pages.  It's
	 * never okay to truncate when splitting an internal page.
	 */
	Assert(!ABTreeTupleIsPivot(lastleft) && !ABTreeTupleIsPivot(firstright));

	/* Determine how many attributes must be kept in truncated tuple */
	keepnatts = _abt_keep_natts(rel, lastleft, firstright, itup_key);

#ifdef DEBUG_NO_TRUNCATE
	/* Force truncation to be ineffective for testing purposes */
	keepnatts = nkeyatts + 1;
#endif
	
	/* 
	 * We'll have to force the truncation because the agg is not needed for
	 * high key.
	 */
	if (keepnatts < nkeyatts)
	{
		pivot = index_truncate_tuple(itupdesc, firstright, 
					Min(keepnatts, nkeyatts));
	}
	else
	{
		Size new_size;
		Size actual_size;

		if (ABTreeTupleIsPosting(firstright))
		{
			/* 
			 * Just purge everything after the posting offset, as the
			 * aggregation array, if present, follows that and will be purged
			 * as well.
			 */
			new_size = ABTreeTupleGetPostingOffset(firstright);
		}
		else
		{
			new_size = IndexTupleSize(firstright) -
				agg_info->extra_space_for_nonpivot_tuple;
		}
		
		/* The size should be already aligned. */
		Assert(MAXALIGN(new_size) == new_size);
		if (keepnatts <= nkeyatts)
		{
			actual_size = new_size;
		}
		else
		{
			/* allocate additional space for heap tid */
			actual_size = new_size + MAXALIGN(sizeof(ItemPointerData));
		}

		/* Copy the header and keys. */
		pivot = (IndexTuple) palloc(actual_size);
		memcpy(pivot, firstright, new_size);
		pivot->t_info &= ~INDEX_SIZE_MASK;
		pivot->t_info |= actual_size;
	}

	/*
	 * If there is a distinguishing key attribute within pivot tuple, we're
	 * done
	 */
	if (keepnatts <= nkeyatts)
	{
		ABTreeTupleSetNAtts(pivot, keepnatts, false);
		return pivot;
	}

	/*
	 * We have to store a heap TID in the new pivot tuple, since no non-TID
	 * key attribute value in firstright distinguishes the right side of the
	 * split from the left side.  nbtree conceptualizes this case as an
	 * inability to truncate away any key attributes, since heap TID is
	 * treated as just another key attribute (despite lacking a pg_attribute
	 * entry).
	 * 
	 * The space for that should have been reserved.  Store all of firstright's
	 * key attribute values plus a tiebreaker heap TID value in enlarged pivot
	 * tuple
	 */
	ABTreeTupleSetNAtts(pivot, nkeyatts, true);
	pivotheaptid = ABTreeTupleGetHeapTID(pivot);

	/*
	 * Lehman & Yao use lastleft as the leaf high key in all cases, but don't
	 * consider suffix truncation.  It seems like a good idea to follow that
	 * example in cases where no truncation takes place -- use lastleft's heap
	 * TID.  (This is also the closest value to negative infinity that's
	 * legally usable.)
	 */
	ItemPointerCopy(ABTreeTupleGetMaxHeapTID(lastleft), pivotheaptid);

	/*
	 * We're done.  Assert() that heap TID invariants hold before returning.
	 *
	 * Lehman and Yao require that the downlink to the right page, which is to
	 * be inserted into the parent page in the second phase of a page split be
	 * a strict lower bound on items on the right page, and a non-strict upper
	 * bound for items on the left page.  Assert that heap TIDs follow these
	 * invariants, since a heap TID value is apparently needed as a
	 * tiebreaker.
	 */
#ifndef DEBUG_NO_TRUNCATE
	Assert(ItemPointerCompare(ABTreeTupleGetMaxHeapTID(lastleft),
							  ABTreeTupleGetHeapTID(firstright)) < 0);
	Assert(ItemPointerCompare(pivotheaptid,
							  ABTreeTupleGetHeapTID(lastleft)) >= 0);
	Assert(ItemPointerCompare(pivotheaptid,
							  ABTreeTupleGetHeapTID(firstright)) < 0);
#else

	/*
	 * Those invariants aren't guaranteed to hold for lastleft + firstright
	 * heap TID attribute values when they're considered here only because
	 * DEBUG_NO_TRUNCATE is defined (a heap TID is probably not actually
	 * needed as a tiebreaker).  DEBUG_NO_TRUNCATE must therefore use a heap
	 * TID value that always works as a strict lower bound for items to the
	 * right.  In particular, it must avoid using firstright's leading key
	 * attribute values along with lastleft's heap TID value when lastleft's
	 * TID happens to be greater than firstright's TID.
	 */
	ItemPointerCopy(ABTreeTupleGetHeapTID(firstright), pivotheaptid);

	/*
	 * Pivot heap TID should never be fully equal to firstright.  Note that
	 * the pivot heap TID will still end up equal to lastleft's heap TID when
	 * that's the only usable value.
	 */
	ItemPointerSetOffsetNumber(pivotheaptid,
							   OffsetNumberPrev(ItemPointerGetOffsetNumber(pivotheaptid)));
	Assert(ItemPointerCompare(pivotheaptid,
							  ABTreeTupleGetHeapTID(firstright)) < 0);
#endif

	return pivot;
}

/*
 * _abt_keep_natts - how many key attributes to keep when truncating.
 *
 * Caller provides two tuples that enclose a split point.  Caller's insertion
 * scankey is used to compare the tuples; the scankey's argument values are
 * not considered here.
 *
 * This can return a number of attributes that is one greater than the
 * number of key attributes for the index relation.  This indicates that the
 * caller must use a heap TID as a unique-ifier in new pivot tuple.
 */
static int
_abt_keep_natts(Relation rel, IndexTuple lastleft, IndexTuple firstright,
			   ABTScanInsert itup_key)
{
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int			keepnatts;
	ScanKey		scankey;

	/*
	 * _abt_compare() treats truncated key attributes as having the value minus
	 * infinity, which would break searches within !heapkeyspace indexes.  We
	 * must still truncate away non-key attribute values, though.
	 */
	if (!itup_key->heapkeyspace)
		return nkeyatts;

	scankey = itup_key->scankeys;
	keepnatts = 1;
	for (int attnum = 1; attnum <= nkeyatts; attnum++, scankey++)
	{
		Datum		datum1,
					datum2;
		bool		isNull1,
					isNull2;

		datum1 = index_getattr(lastleft, attnum, itupdesc, &isNull1);
		datum2 = index_getattr(firstright, attnum, itupdesc, &isNull2);

		if (isNull1 != isNull2)
			break;

		if (!isNull1 &&
			DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
											scankey->sk_collation,
											datum1,
											datum2)) != 0)
			break;

		keepnatts++;
	}

	/*
	 * Assert that _abt_keep_natts_fast() agrees with us in passing.  This is
	 * expected in an allequalimage index.
	 */
	Assert(!itup_key->allequalimage ||
		   keepnatts == _abt_keep_natts_fast(rel, lastleft, firstright));

	return keepnatts;
}

/*
 * _abt_keep_natts_fast - fast bitwise variant of _abt_keep_natts.
 *
 * This is exported so that a candidate split point can have its effect on
 * suffix truncation inexpensively evaluated ahead of time when finding a
 * split location.  A naive bitwise approach to datum comparisons is used to
 * save cycles.
 *
 * The approach taken here usually provides the same answer as _abt_keep_natts
 * will (for the same pair of tuples from a heapkeyspace index), since the
 * majority of btree opclasses can never indicate that two datums are equal
 * unless they're bitwise equal after detoasting.  When an index only has
 * "equal image" columns, routine is guaranteed to give the same result as
 * _abt_keep_natts would.
 *
 * Callers can rely on the fact that attributes considered equal here are
 * definitely also equal according to _abt_keep_natts, even when the index uses
 * an opclass or collation that is not "allequalimage"/deduplication-safe.
 * This weaker guarantee is good enough for nbtsplitloc.c caller, since false
 * negatives generally only have the effect of making leaf page splits use a
 * more balanced split point.
 */
int
_abt_keep_natts_fast(Relation rel, IndexTuple lastleft, IndexTuple firstright)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int			keysz = IndexRelationGetNumberOfKeyAttributes(rel);
	int			keepnatts;

	keepnatts = 1;
	for (int attnum = 1; attnum <= keysz; attnum++)
	{
		Datum		datum1,
					datum2;
		bool		isNull1,
					isNull2;
		Form_pg_attribute att;

		datum1 = index_getattr(lastleft, attnum, itupdesc, &isNull1);
		datum2 = index_getattr(firstright, attnum, itupdesc, &isNull2);
		att = TupleDescAttr(itupdesc, attnum - 1);

		if (isNull1 != isNull2)
			break;

		if (!isNull1 &&
			!datum_image_eq(datum1, datum2, att->attbyval, att->attlen))
			break;

		keepnatts++;
	}

	return keepnatts;
}

/*
 *  _abt_check_natts() -- Verify tuple has expected number of attributes.
 *
 * Returns value indicating if the expected number of attributes were found
 * for a particular offset on page.  This can be used as a general purpose
 * sanity check.
 *
 * Testing a tuple directly with ABTreeTupleGetNAtts() should generally be
 * preferred to calling here.  That's usually more convenient, and is always
 * more explicit.  Call here instead when offnum's tuple may be a negative
 * infinity tuple that uses the pre-v11 on-disk representation, or when a low
 * context check is appropriate.  This routine is as strict as possible about
 * what is expected on each version of btree.
 */
bool
_abt_check_natts(Relation rel, bool heapkeyspace, Page page, OffsetNumber offnum)
{
	int16		natts = IndexRelationGetNumberOfAttributes(rel);
	int16		nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	ABTPageOpaque opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	IndexTuple	itup;
	int			tupnatts;

	/*
	 * We cannot reliably test a deleted or half-dead page, since they have
	 * dummy high keys
	 */
	if (ABT_P_IGNORE(opaque))
		return true;

	Assert(offnum >= FirstOffsetNumber &&
		   offnum <= PageGetMaxOffsetNumber(page));

	/*
	 * Mask allocated for number of keys in index tuple must be able to fit
	 * maximum possible number of index attributes
	 */
	StaticAssertStmt(ABT_OFFSET_MASK >= INDEX_MAX_KEYS,
					 "ABT_OFFSET_MASK can't fit INDEX_MAX_KEYS");

	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));
	tupnatts = ABTreeTupleGetNAtts(itup, rel);

	/* !heapkeyspace indexes do not support deduplication */
	if (!heapkeyspace && ABTreeTupleIsPosting(itup))
		return false;

	/* Posting list tuples should never have "pivot heap TID" bit set */
	if (ABTreeTupleIsPosting(itup) &&
		(ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) &
		 ABT_PIVOT_HEAP_TID_ATTR) != 0)
		return false;

	/* INCLUDE indexes do not support deduplication */
	if (natts != nkeyatts && ABTreeTupleIsPosting(itup))
		return false;

	if (ABT_P_ISLEAF(opaque))
	{
		if (offnum >= ABT_P_FIRSTDATAKEY(opaque))
		{
			/*
			 * Non-pivot tuple should never be explicitly marked as a pivot
			 * tuple
			 */
			if (ABTreeTupleIsPivot(itup))
				return false;

			/*
			 * Leaf tuples that are not the page high key (non-pivot tuples)
			 * should never be truncated.  (Note that tupnatts must have been
			 * inferred, even with a posting list tuple, because only pivot
			 * tuples store tupnatts directly.)
			 */
			return tupnatts == natts;
		}
		else
		{
			/*
			 * Rightmost page doesn't contain a page high key, so tuple was
			 * checked above as ordinary leaf tuple
			 */
			Assert(!ABT_P_RIGHTMOST(opaque));

			/*
			 * !heapkeyspace high key tuple contains only key attributes. Note
			 * that tupnatts will only have been explicitly represented in
			 * !heapkeyspace indexes that happen to have non-key attributes.
			 */
			if (!heapkeyspace)
				return tupnatts == nkeyatts;

			/* Use generic heapkeyspace pivot tuple handling */
		}
	}
	else						/* !ABT_P_ISLEAF(opaque) */
	{
		if (offnum == ABT_P_FIRSTDATAKEY(opaque))
		{
			/*
			 * The first tuple on any internal page (possibly the first after
			 * its high key) is its negative infinity tuple.  Negative
			 * infinity tuples are always truncated to zero attributes.  They
			 * are a particular kind of pivot tuple.
			 */
			if (heapkeyspace)
				return tupnatts == 0;

			/*
			 * The number of attributes won't be explicitly represented if the
			 * negative infinity tuple was generated during a page split that
			 * occurred with a version of Postgres before v11.  There must be
			 * a problem when there is an explicit representation that is
			 * non-zero, or when there is no explicit representation and the
			 * tuple is evidently not a pre-pg_upgrade tuple.
			 *
			 * Prior to v11, downlinks always had ABTP_HIKEY as their offset.
			 * Accept that as an alternative indication of a valid
			 * !heapkeyspace negative infinity tuple.
			 */
			return tupnatts == 0 ||
				ItemPointerGetOffsetNumber(&(itup->t_tid)) == ABTP_HIKEY;
		}
		else
		{
			/*
			 * !heapkeyspace downlink tuple with separator key contains only
			 * key attributes.  Note that tupnatts will only have been
			 * explicitly represented in !heapkeyspace indexes that happen to
			 * have non-key attributes.
			 */
			if (!heapkeyspace)
				return tupnatts == nkeyatts;

			/* Use generic heapkeyspace pivot tuple handling */
		}

	}

	/* Handle heapkeyspace pivot tuples (excluding minus infinity items) */
	Assert(heapkeyspace);

	/*
	 * Explicit representation of the number of attributes is mandatory with
	 * heapkeyspace index pivot tuples, regardless of whether or not there are
	 * non-key attributes.
	 */
	if (!ABTreeTupleIsPivot(itup))
		return false;

	/* Pivot tuple should not use posting list representation (redundant) */
	if (ABTreeTupleIsPosting(itup))
		return false;

	/*
	 * Heap TID is a tiebreaker key attribute, so it cannot be untruncated
	 * when any other key attribute is truncated
	 */
	if (ABTreeTupleGetHeapTID(itup) != NULL && tupnatts != nkeyatts)
		return false;

	/*
	 * Pivot tuple must have at least one untruncated key attribute (minus
	 * infinity pivot tuples are the only exception).  Pivot tuples can never
	 * represent that there is a value present for a key attribute that
	 * exceeds pg_index.indnkeyatts for the index.
	 */
	return tupnatts > 0 && tupnatts <= nkeyatts;
}

/*
 *
 *  _abt_check_third_page() -- check whether tuple fits on a btree page at all.
 *
 * We actually need to be able to fit three items on every page, so restrict
 * any one item to 1/3 the per-page available space.  Note that itemsz should
 * not include the ItemId overhead.
 *
 * It might be useful to apply TOAST methods rather than throw an error here.
 * Using out of line storage would break assumptions made by suffix truncation
 * and by contrib/amcheck, though.
 */
void
_abt_check_third_page(Relation rel, Relation heap, Page page, IndexTuple newtup,
		ABTCachedAggInfo agg_info)
{
	Size		itemsz;
	ABTPageOpaque opaque;

	itemsz = MAXALIGN(IndexTupleSize(newtup));

	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	if (!ABT_P_ISLEAF(opaque))
	{
		/* double check if we are above the limit as we don't need more space */
		if (itemsz <= ABTMaxItemSizeNoHeapTid(page))
			return;

		/*
		 * Internal page insertions cannot fail here, because that would mean
		 * that an earlier leaf level insertion that should have failed didn't
		 */
		elog(ERROR, "cannot insert oversized tuple of size %zu on internal "
					"page of index \"%s\"",
			 itemsz, RelationGetRelationName(rel));
	}

	/* 
	 * Double check item size against limit.
	 */
	
	if ((agg_info->leaf_has_agg ? 
			MAXALIGN(itemsz - agg_info->extra_space_when_tid_missing) :
			itemsz) <= ABTMaxItemSize(page, agg_info))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			 errmsg("index row size %zu exceeds aggregate b-tree maximum %zu for index \"%s\"",
					itemsz,
					ABTMaxItemSize(page, agg_info),
					RelationGetRelationName(rel)),
			 errdetail("Index row references tuple (%u,%u) in relation \"%s\".",
					   ItemPointerGetBlockNumber(ABTreeTupleGetHeapTID(newtup)),
					   ItemPointerGetOffsetNumber(ABTreeTupleGetHeapTID(newtup)),
					   RelationGetRelationName(heap)),
			 errhint("Values larger than 1/3 of a buffer page cannot be indexed.\n"
					 "Consider a function index of an MD5 hash of the value, "
					 "or use full text indexing."),
			 errtableconstraint(heap, RelationGetRelationName(rel))));
}

/*
 * Are all attributes in rel "equality is image equality" attributes?
 *
 * We use each attribute's ABTEQUALIMAGE_PROC opclass procedure.  If any
 * opclass either lacks a ABTEQUALIMAGE_PROC procedure or returns false, we
 * return false; otherwise we return true.
 *
 * Returned boolean value is stored in index metapage during index builds.
 * Deduplication can only be used when we return true.
 */
bool
_abt_allequalimage(Relation rel, bool debugmessage)
{
	bool		allequalimage = true;

	/* INCLUDE indexes don't support deduplication */
	if (IndexRelationGetNumberOfAttributes(rel) !=
		IndexRelationGetNumberOfKeyAttributes(rel))
		return false;

	/*
	 * There is no special reason why deduplication cannot work with system
	 * relations (i.e. with system catalog indexes and TOAST indexes).  We
	 * deem deduplication unsafe for these indexes all the same, since the
	 * alternative is to force users to always use deduplication, without
	 * being able to opt out.  (ALTER INDEX is not supported with system
	 * indexes, so users would have no way to set the deduplicate_items
	 * storage parameter to 'off'.)
	 */
	if (IsSystemRelation(rel))
		return false;

	for (int i = 0; i < IndexRelationGetNumberOfKeyAttributes(rel); i++)
	{
		Oid			opfamily = rel->rd_opfamily[i];
		Oid			opcintype = rel->rd_opcintype[i];
		Oid			collation = rel->rd_indcollation[i];
		Oid			equalimageproc;

		equalimageproc = get_opfamily_proc(opfamily, opcintype, opcintype,
										   ABTEQUALIMAGE_PROC);

		/*
		 * If there is no ABTEQUALIMAGE_PROC then deduplication is assumed to
		 * be unsafe.  Otherwise, actually call proc and see what it says.
		 */
		if (!OidIsValid(equalimageproc) ||
			!DatumGetBool(OidFunctionCall1Coll(equalimageproc, collation,
											   ObjectIdGetDatum(opcintype))))
		{
			allequalimage = false;
			break;
		}
	}

	/*
	 * Don't elog() until here to avoid reporting on a system relation index
	 * or an INCLUDE index
	 */
	if (debugmessage)
	{
		if (allequalimage)
			elog(DEBUG1, "index \"%s\" can safely use deduplication",
				 RelationGetRelationName(rel));
		else
			elog(DEBUG1, "index \"%s\" cannot use deduplication",
				 RelationGetRelationName(rel));
	}

	return allequalimage;
}

/*
 * _abt_compute_tuple_offests can be called on any agg_info with
 * agg_typlen and leaf_has_agg set.
 */
void
_abt_compute_tuple_offsets(
	ABTCachedAggInfo agg_info)
{
	Size	ext_space_wo_tid;
	Size	ext_space_w_tid;
	Size	ext_space_nonpivot;

	
	/* XXX we'll be using atomic fetch and add on the agg type (int2/int4/int8).
	 * Thus INTALIGN it guarantees we can either do 32-bit version on int2/int4,
	 * or 64-bit version on int8 without touching the tid.
	 *
	 * ext_space_wo_tid: agg value | last_update_id |
	 *					^		   ^				^
	 *					|		   |				|
	 *					|		   |				|
	 *			- ext_space_wo_tid |				|
	 *							-2 (uint16)			0
	 *				= -16	wasting 2 bytes for agg_type == int8
	 *				= -8	wasting 2 bytes for agg_type = int4
	 *				= -8	wasting 4 bytes for agg_type = int2
	 *
	 */
	ext_space_wo_tid = MAXALIGN(INTALIGN(agg_info->agg_typlen) +
		sizeof(ABTLastUpdateId));
	/* last update id should be short aligned */
	Assert(SHORTALIGN(ext_space_wo_tid - sizeof(ABTLastUpdateId)) ==
			ext_space_wo_tid - sizeof(ABTLastUpdateId));
	/* and should not overlap with agg value */
	Assert(ext_space_wo_tid - sizeof(ABTLastUpdateId) >= agg_info->agg_typlen);

	/*
	 * ext_space_w_tid: agg_value | last_update_id | heap tid |
	 *				   ^		  ^				   ^		  ^
	 *				   |		  |				   |		  |
	 *				   |		  |				   |		  |
	 *			- ext_space_w_tid |				   |		  0
	 *							 -8 (uint16)	  -6 (6-byte struct)
	 *				= -16	wasting 0 bytes for agg_type == int8
	 *				= -16	wasting 4 bytes for agg_type == int4
	 *				= -16	wasting 6 bytes for agg_type == int2
	 */
	ext_space_w_tid = MAXALIGN(INTALIGN(agg_info->agg_typlen) +
		SHORTALIGN(sizeof(ABTLastUpdateId)) +
		sizeof(ItemPointerData));

	/* the heap tid should still be properly aligned after MAXALIGN */
	Assert(SHORTALIGN(ext_space_w_tid - sizeof(ItemPointerData)) ==
			(ext_space_w_tid - sizeof(ItemPointerData)));
	/* last update id should also be short aligned. */
	Assert(SHORTALIGN(ext_space_w_tid - SHORTALIGN(sizeof(ItemPointerData)) -
				sizeof(ABTLastUpdateId)) ==
			ext_space_w_tid - SHORTALIGN(sizeof(ItemPointerData)) -
				sizeof(ABTLastUpdateId));
	/* and they shouldn't overlap with agg value */
	Assert(ext_space_w_tid - SHORTALIGN(sizeof(ItemPointerData)) -
				sizeof(ABTLastUpdateId) >= agg_info->agg_typlen);
	
	/* 
	 * now non-pivot tuple also needs to store a transaction id xmin that
	 * created this index tuple.
	 */
	StaticAssertStmt(sizeof(TransactionId) == 4, "xid is not 4 bytes");
	if (agg_info->leaf_has_agg)
	{
		/* 
		 * 16 bytes for int8,
		 * 8 bytes for int4,
		 * 8 bytes for int2
		 */
		ext_space_nonpivot = 
			MAXALIGN(INTALIGN(agg_info->agg_typlen) + sizeof(TransactionId));
	}
	else
	{
		/* 8 bytes */
		ext_space_nonpivot = MAXALIGN(sizeof(TransactionId));
	}
	
	agg_info->extra_space_for_nonpivot_tuple = ext_space_nonpivot;
	agg_info->extra_space_when_tid_missing = ext_space_wo_tid;
	agg_info->extra_space_when_tid_present = ext_space_w_tid;
}

IndexTuple
_abt_pivot_tuple_purge_agg_space(IndexTuple itup,
								 ABTCachedAggInfo agg_info)
{
	IndexTuple new_itup;

	Assert(ABTreeTupleIsPivot(itup)); 
	if (ABTPivotTupleHasHeapTID(itup)) 
	{ 
		Size new_size; 
		Size new_size_excluding_tid = IndexTupleSize(itup) -
			agg_info->extra_space_when_tid_present; 
		Assert(MAXALIGN(new_size_excluding_tid) == new_size_excluding_tid); 
		new_size = MAXALIGN(new_size_excluding_tid + sizeof(ItemPointerData)); 
		new_itup = (IndexTuple) palloc0(new_size); 
		memcpy(new_itup, itup, new_size_excluding_tid); 
		new_itup->t_info &= ~INDEX_SIZE_MASK; 
		new_itup->t_info |= new_size; 
		ItemPointerCopy(ABTreeTupleGetHeapTID(itup), 
						ABTreeTupleGetHeapTID(new_itup)); 
	} 
	else 
	{ 
		Size new_size = IndexTupleSize(itup) - 
			agg_info->extra_space_when_tid_missing; 
		Assert(MAXALIGN(new_size) == new_size); 
		new_itup = (IndexTuple) palloc0(new_size); 
		memcpy(new_itup, itup, new_size); 
		new_itup->t_info &= ~INDEX_SIZE_MASK; 
		new_itup->t_info |= new_size; 
	} 

	return new_itup;
}

IndexTuple
_abt_pivot_tuple_reserve_agg_space(IndexTuple itup,
								   ABTCachedAggInfo agg_info)
{
	IndexTuple new_itup; 

	Assert(ABTreeTupleIsPivot(itup)); 
	if (ABTPivotTupleHasHeapTID(itup)) 
	{ 
		Size base_size = MAXALIGN_DOWN(IndexTupleSize(itup)
							- sizeof(ItemPointerData));
		Size new_size;
		new_size = base_size + agg_info->extra_space_when_tid_present;
		Assert(MAXALIGN(new_size) == new_size);
		new_itup = (IndexTuple) palloc0(new_size); 
		memcpy(new_itup, itup, base_size); 
		new_itup->t_info &= ~INDEX_SIZE_MASK; 
		new_itup->t_info |= new_size; 
		ItemPointerCopy(ABTreeTupleGetHeapTID(itup), 
						ABTreeTupleGetHeapTID(new_itup)); 
	} 
	else 
	{ 
		Size new_size = IndexTupleSize(itup) +
			agg_info->extra_space_when_tid_missing; 
		Assert(MAXALIGN(new_size) == new_size); 
		new_itup = (IndexTuple) palloc0(new_size); 
		memcpy(new_itup, itup, new_size); 
		new_itup->t_info &= ~INDEX_SIZE_MASK; 
		new_itup->t_info |= new_size; 
	} 

	return new_itup;
}

void
_abt_set_pivot_tuple_aggregation_value(
	IndexTuple itup,
	Datum agg_val,
	ABTCachedAggInfo agg_info)
{
	Pointer	agg_ptr = ABTreePivotTupleGetAggPtr(itup, agg_info);
	Assert(ABTreeTupleIsPivot(itup));
		
	if (agg_info->agg_byval)
	{
		store_att_byval(
			agg_ptr,
			agg_val,
			agg_info->agg_typlen);
	}
	else
	{
		memcpy(agg_ptr,
			   DatumGetPointer(agg_val),
			   agg_info->agg_typlen);
	}
}

void
_abt_set_nonpivot_tuple_aggregation_value(
	IndexTuple itup,
	Datum agg_val,
	ABTCachedAggInfo agg_info)
{
	Pointer	agg_ptr = ABTreeNonPivotTupleGetAggPtr(itup, agg_info);
	Assert(!ABTreeTupleIsPivot(itup) && !ABTreeTupleIsPosting(itup));
		
	if (agg_info->agg_byval)
	{
		store_att_byval(
			agg_ptr,
			agg_val,
			agg_info->agg_typlen);
	}
	else
	{
		memcpy(agg_ptr,
			   DatumGetPointer(agg_val),
			   agg_info->agg_typlen);
	}
}

void
_abt_set_posting_tuple_aggregation_values(
	IndexTuple itup,
	Datum agg_val,
	ABTCachedAggInfo agg_info)
{
	int16	n;
	Pointer	agg_arr,
			agg_arr_end;
	Assert(ABTreeTupleIsPosting(itup));

	/* TODO For now, we assume only one value is passed here. */
	n = ABTreeTupleGetNPosting(itup);
	agg_arr = ABTreePostingTupleGetAggPtr(itup, agg_info);
	agg_arr_end = agg_arr + agg_info->agg_stride * n;
	while (agg_arr < agg_arr_end)
	{
		if (agg_info->agg_byval)
		{
			store_att_byval(agg_arr, agg_val, agg_info->agg_typlen);
		}
		else
		{
			memcpy(agg_arr, DatumGetPointer(agg_val),
				agg_info->agg_typlen);
		}
		agg_arr += agg_info->agg_stride;
	}
}

/*
 * Get the cached aggregation info from rel->rd_amcache.
 *
 * If the cache is not initialized and missing_ok is true, it returns NULL.
 *
 * If the cache is not initialized and missing_ok is false, it will fetch
 * the meta page and initialize the cache.
 */
ABTCachedAggInfo
_abt_get_cached_agg_info(Relation rel, bool missing_ok)
{
	ABTAMCacheHeader	cache;	

	if (!rel->rd_amcache)
	{
		if (missing_ok)
			return NULL;
		else
		{
			Buffer				metabuf;
			Page				metapg;
			ABTMetaPageData		*metad;
			
			metabuf = _abt_getbuf(rel, ABTREE_METAPAGE, ABT_READ);
			metapg = BufferGetPage(metabuf);
			metad = ABTPageGetMeta(metapg);

			if (metad->abtm_root == ABTP_NONE)
			{
				/* 
				 * We shouldn't cache the meta page if there's no root page yet,
				 * because _abt_getroot() doesn't expect one and
				 * _abt_metaversion() will complain if we do.
				 *
				 * But we should cache the agg_support whenever possible.
				 */
				_abt_save_metapage(rel, NULL, ABTMetaPageGetAggSupport(metad));
			}
			else
			{
				_abt_save_metapage(rel, metad, NULL);
			}

			_abt_relbuf(rel, metabuf);
		}
	}
	
	Assert(rel->rd_amcache);
	cache = (ABTAMCacheHeader) rel->rd_amcache;
	return cache->agg_info;
}

ABTCachedAggInfo
_abt_copy_cached_agg_info(Relation rel, MemoryContext mcxt)
{
	ABTCachedAggInfo	src_agg_info;
	ABTCachedAggInfo	dst_agg_info;
	Size				size;

	src_agg_info = _abt_get_cached_agg_info(rel, false);
	size = ABTGetCachedAggInfoSize(&src_agg_info->agg_support);
	dst_agg_info = (ABTCachedAggInfo) MemoryContextAlloc(mcxt, size);
	memcpy(dst_agg_info, src_agg_info, size);

	if (dst_agg_info->agg_support.abtas_flags & ABTAS_MAP_IS_CONST)
	{
		if (!dst_agg_info->agg_byval)
		{
			dst_agg_info->agg_map_val = ABTASGetExtraData(
				&dst_agg_info->agg_support,
				dst_agg_info->agg_support.abtas_map.off);
		}
	}

	if (dst_agg_info->int2_to_agg_type_fn_initialized)
	{
		Assert(dst_agg_info->int2_to_agg_type_fn.fn_extra == NULL);
		dst_agg_info->int2_to_agg_type_fn.fn_mcxt = mcxt;
	}

	if (dst_agg_info->agg_type_to_int2_fn_initialized)
	{
		Assert(dst_agg_info->agg_type_to_int2_fn.fn_extra == NULL);
		dst_agg_info->agg_type_to_int2_fn.fn_mcxt = mcxt;
	}
	
	if (dst_agg_info->double_to_agg_type_fn_initialized)
	{
		Assert(dst_agg_info->double_to_agg_type_fn.fn_extra == NULL);
		dst_agg_info->double_to_agg_type_fn.fn_mcxt = mcxt;
	}

	if (dst_agg_info->agg_type_to_double_fn_initialized)
	{
		Assert(dst_agg_info->agg_type_to_double_fn.fn_extra == NULL);
		dst_agg_info->agg_type_to_double_fn.fn_mcxt = mcxt;
	}

	Assert(dst_agg_info->agg_type_btcmp_fn.fn_extra == NULL);
	dst_agg_info->agg_type_btcmp_fn.fn_mcxt = mcxt;

	Assert(dst_agg_info->agg_add_fn.fn_extra == NULL);
	dst_agg_info->agg_add_fn.fn_mcxt = mcxt;

	Assert(dst_agg_info->agg_mul_fn.fn_extra == NULL);
	dst_agg_info->agg_mul_fn.fn_mcxt = mcxt;

	Assert(dst_agg_info->agg_sub_fn.fn_extra == NULL);
	dst_agg_info->agg_sub_fn.fn_mcxt = mcxt;

	return dst_agg_info;
}

/*
 * Get the cached meta page from rel->rd_amcache.
 *
 * Returns NULL if the cache is not initialized.
 */
ABTMetaPageData *
_abt_get_cached_metapage(Relation rel)
{
	if (!rel->rd_amcache)
		return NULL;

	return ((ABTAMCacheHeader) rel->rd_amcache)->metad;
}

/*
 * Save the metapage into rel->rd_amcache.
 *
 * If agg_support is NULL, we will not overwrite it if rel->rd_amcache is
 * present; otherwise, we copy the agg_support from the end of the metad.
 *
 * If metad is NULL, we will only store agg_support into into the cache.
 *
 * It shouldn't be called with both metad and agg_support being NULL.
 */
void
_abt_save_metapage(Relation rel, ABTMetaPageData *metad,
				   ABTAggSupport agg_support)
{
	Assert(metad || agg_support);

	if (!rel->rd_amcache)
	{
		ABTAMCacheHeader amcache;
		Size amcache_size = MAXALIGN(sizeof(ABTAMCacheHeaderData)) +
			MAXALIGN(sizeof(ABTMetaPageData));
		if (!agg_support)
		{
			agg_support = ABTMetaPageGetAggSupport(metad);
		}
		amcache_size += MAXALIGN(ABTGetCachedAggInfoSize(agg_support));
		amcache = (ABTAMCacheHeader) MemoryContextAlloc(rel->rd_indexcxt,
													    amcache_size);
		amcache->metad = NULL;
		amcache->agg_info = NULL;
		rel->rd_amcache = amcache;
	}
	
	if (metad)
	{
		Pointer metad_b = (Pointer) rel->rd_amcache +
			MAXALIGN(sizeof(ABTAMCacheHeaderData));
		memcpy(metad_b, metad, sizeof(ABTMetaPageData));
		((ABTAMCacheHeader) rel->rd_amcache)->metad =
			(ABTMetaPageData *) metad_b;
	}

	if (agg_support)
	{
		ABTCachedAggInfo agg_info = (ABTCachedAggInfo)(
			(Pointer) rel->rd_amcache +
			MAXALIGN(sizeof(ABTAMCacheHeaderData)) +
			MAXALIGN(sizeof(ABTMetaPageData)));
		_abt_fill_agg_info(agg_info, agg_support);
		((ABTAMCacheHeader) rel->rd_amcache)->agg_info = agg_info;
	}
}

/*
 * Clear the metad portion of the cache.
 */
void
_abt_clear_cached_metapage(Relation rel)
{
	ABTAMCacheHeader cache;

	if (!rel->rd_amcache)
		return ;

	cache = (ABTAMCacheHeader) rel->rd_amcache;
	cache->metad = NULL;
}

void
_abt_clear_cache(Relation rel)
{
	if (rel->rd_amcache)
		pfree(rel->rd_amcache);

	rel->rd_amcache = NULL;
}

void
_abt_fill_agg_info(ABTCachedAggInfo agg_info,
				   ABTAggSupport agg_support)
{
	HeapTuple			htup;
	Form_pg_type		agg_type;
	
	
	memcpy((Pointer) agg_info + offsetof(ABTCachedAggInfoData, agg_support),
			agg_support, ABTASGetStructSize(agg_support));
	agg_support = &agg_info->agg_support;
	
	/* Look up info in pg_type for the agg_type. */
	htup = SearchSysCache1(TYPEOID, agg_support->abtas_agg_type_oid);
	if (!HeapTupleIsValid(htup))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("agg type %u not found",
					agg_support->abtas_agg_type_oid)));
	}

	agg_type = (Form_pg_type) GETSTRUCT(htup);
	agg_info->agg_byval = agg_type->typbyval;
	agg_info->agg_align = agg_type->typalign;
	agg_info->agg_typlen = agg_type->typlen;
	agg_info->agg_stride = att_align_nominal(agg_type->typlen,
												 agg_type->typalign);
	if (agg_support->abtas_agg_type_oid == FLOAT8OID ||
		agg_support->abtas_agg_type_oid == FLOAT4OID)
	{
		agg_info->agg_is_floating_point = true;
	}
	else
	{
		agg_info->agg_is_floating_point = false;
	}

	/* Done with the pg_type catalog. */
	ReleaseSysCache(htup);
	
	/* Other info initialized here. */
	if (agg_support->abtas_flags & ABTAS_MAP_IS_CONST)
	{
		agg_info->leaf_has_agg = false;
		if (agg_info->agg_byval)
		{
			agg_info->agg_map_val = agg_support->abtas_map.val;
		}
		else
		{
			agg_info->agg_map_val = ABTASGetExtraData(agg_support,
										agg_support->abtas_map.off);
		}
	}
	else
	{
		agg_info->leaf_has_agg = true;
	}

	if (OidIsValid(agg_support->abtas_int2_to_agg_type_fn_oid))
	{
		agg_info->int2_to_agg_type_fn_initialized = true;
		fmgr_info(agg_support->abtas_int2_to_agg_type_fn_oid,
				  &agg_info->int2_to_agg_type_fn);
	}
	else
	{
		agg_info->int2_to_agg_type_fn_initialized = false;
	}

	if (OidIsValid(agg_support->abtas_agg_type_to_int2_fn_oid))
	{
		agg_info->agg_type_to_int2_fn_initialized = true;
		fmgr_info(agg_support->abtas_agg_type_to_int2_fn_oid,
				  &agg_info->agg_type_to_int2_fn);
	}
	else
	{
		agg_info->agg_type_to_int2_fn_initialized = false;
	}

	if (OidIsValid(agg_support->abtas_double_to_agg_type_fn_oid))
	{
		agg_info->double_to_agg_type_fn_initialized = true;
		fmgr_info(agg_support->abtas_double_to_agg_type_fn_oid,
				  &agg_info->double_to_agg_type_fn);
	}
	else
	{
		agg_info->double_to_agg_type_fn_initialized = false;
	}

	if (OidIsValid(agg_support->abtas_agg_type_to_double_fn_oid))
	{
		agg_info->agg_type_to_double_fn_initialized = true;
		fmgr_info(agg_support->abtas_agg_type_to_double_fn_oid,
				  &agg_info->agg_type_to_double_fn);
	}
	else
	{
		agg_info->agg_type_to_double_fn_initialized = false;
	}
	
	fmgr_info(agg_support->abtas_agg_type_btcmp_fn_oid,
			  &agg_info->agg_type_btcmp_fn);
	fmgr_info(agg_support->abtas_add.oid, &agg_info->agg_add_fn);
	fmgr_info(agg_support->abtas_mul.oid, &agg_info->agg_mul_fn);
	fmgr_info(agg_support->abtas_sub.oid, &agg_info->agg_sub_fn);

	_abt_compute_tuple_offsets(agg_info);
}

/*
 * _abt_accum_value_nofree() -- add value_to_add to acc
 *
 * When is_first == true, return value_to_add (possibly making a copy if the
 * type is passed by reference).
 *
 * This is almost identical to _abt_accum_value() but it does not deallocate
 * the old accumulator ``acc''.
 *
 */
Datum
_abt_accum_value_nofree(ABTCachedAggInfo agg_info,
						Datum acc,
						Datum value_to_add,
						bool is_first)
{
	if (is_first)
	{
		Pointer buf;

		if (agg_info->agg_byval)
			return value_to_add;

		buf = (Pointer) palloc(agg_info->agg_typlen);
		memcpy(buf, DatumGetPointer(value_to_add), agg_info->agg_typlen);
		return PointerGetDatum(buf);
	}
	return FunctionCall2(&agg_info->agg_add_fn, acc, value_to_add);
}

void
_abt_accum_value(ABTCachedAggInfo agg_info, Datum *acc, Datum value_to_add,
				 bool is_first)
{
	if (agg_info->agg_byval)
	{
		if (is_first)
		{
			*acc = value_to_add;
		}
		else
		{
			*acc = FunctionCall2(&agg_info->agg_add_fn, *acc, value_to_add);
		}
	}
	else
	{
		if (is_first)
		{
			Pointer buf = (Pointer) palloc(agg_info->agg_typlen);
			memcpy(buf, DatumGetPointer(value_to_add), agg_info->agg_typlen);
			*acc = PointerGetDatum(buf);
		}
		else
		{
			Datum new_agg;
			new_agg = FunctionCall2(&agg_info->agg_add_fn, *acc, value_to_add);
			pfree(DatumGetPointer(*acc));
			*acc = new_agg;
		}
	}
}

void
_abt_exclude_value(ABTCachedAggInfo agg_info,
				   Datum *acc, Datum value_to_subtract)
{
	if (agg_info->agg_byval)
	{
		*acc = FunctionCall2(&agg_info->agg_sub_fn, *acc, value_to_subtract);
	}
	else
	{
		Datum new_agg;
		new_agg = FunctionCall2(&agg_info->agg_sub_fn, *acc, value_to_subtract);
		pfree(DatumGetPointer(*acc));
		*acc = new_agg;
	}
}

void
_abt_accum_index_tuple_value(ABTCachedAggInfo agg_info,
							 Datum *agg,
							 IndexTuple itup,
							 bool is_leaf,
							 bool is_first)
{
	if (is_leaf && !agg_info->leaf_has_agg)
	{
		/* The value is a const not stored with the nonpivot tuple. */
		if (ABTreeTupleIsPosting(itup))
		{
			int16 n = ABTreeTupleGetNPosting(itup);
			Datum val_to_add = ABTreeComputeProductOfAggAndInt2(
				agg_info, agg_info->agg_map_val, n);
			_abt_accum_value(agg_info, agg, val_to_add,
				is_first);
			if (!agg_info->agg_byval)
				pfree(DatumGetPointer(val_to_add));
		}
		else
		{
			_abt_accum_value(agg_info, agg, agg_info->agg_map_val, is_first);
		}
	}
	else
	{
		if (ABTreeTupleIsPosting(itup))
		{
			int16 n = ABTreeTupleGetNPosting(itup);
			Pointer agg_arr = ABTreePostingTupleGetAggPtr(itup, agg_info);
			Pointer agg_arr_end = agg_arr + agg_info->agg_stride * n;

			_abt_accum_value(agg_info, agg,
				fetch_att(agg_arr, agg_info->agg_byval, agg_info->agg_typlen),
				is_first);
			agg_arr += agg_info->agg_stride;

			/* posting size is at least 2 */
			Assert(agg_arr < agg_arr_end);
			while (agg_arr < agg_arr_end)
			{
				_abt_accum_value(agg_info, agg,
					fetch_att(agg_arr, agg_info->agg_byval,
							  agg_info->agg_typlen),
					false);
				agg_arr += agg_info->agg_stride;
			}
		}
		else
		{
			Pointer data_ptr = is_leaf ? 
				ABTreeNonPivotTupleGetAggPtr(itup, agg_info) :
				ABTreePivotTupleGetAggPtr(itup, agg_info);
			_abt_accum_value(agg_info, agg,
				fetch_att(data_ptr, agg_info->agg_byval, agg_info->agg_typlen),
				is_first);
		}
	}
}

void
_abt_exclude_index_tuple_value(ABTCachedAggInfo agg_info,
							   Datum *agg,
							   IndexTuple itup,
							   bool is_leaf)
{
	if (is_leaf && !agg_info->leaf_has_agg)
	{
		/* The value is a const not stored with the nonpivot tuple. */
		if (ABTreeTupleIsPosting(itup))
		{
			int16 n = ABTreeTupleGetNPosting(itup);
			Datum val_to_subtract = ABTreeComputeProductOfAggAndInt2(
				agg_info, agg_info->agg_map_val, n);
			_abt_exclude_value(agg_info, agg, val_to_subtract);
			if (!agg_info->agg_byval)
				pfree(DatumGetPointer(val_to_subtract));
		}
		else
		{
			_abt_exclude_value(agg_info, agg, agg_info->agg_map_val);
		}
	}
	else
	{
		if (ABTreeTupleIsPosting(itup))
		{
			int16 n = ABTreeTupleGetNPosting(itup);
			Pointer agg_arr = ABTreePostingTupleGetAggPtr(itup, agg_info);
			Pointer agg_arr_end = agg_arr + agg_info->agg_stride * n;

			while (agg_arr < agg_arr_end)
			{
				_abt_exclude_value(agg_info, agg,
					fetch_att(agg_arr, agg_info->agg_byval,
							  agg_info->agg_typlen));
				agg_arr += agg_info->agg_stride;
			}
		}
		else
		{
			Pointer data_ptr = is_leaf ?
				ABTreeNonPivotTupleGetAggPtr(itup, agg_info) :
				ABTreePivotTupleGetAggPtr(itup, agg_info);
			_abt_exclude_value(agg_info, agg,
				fetch_att(data_ptr, agg_info->agg_byval, agg_info->agg_typlen));
		}
	}
}

Datum
_abt_accum_page_value(ABTCachedAggInfo agg_info, Buffer buf)
{
	Datum			agg;
	Page			page;
	ABTPageOpaque	opaque;
	OffsetNumber	off,
					maxoff;
	ItemId			itemid;
	IndexTuple		itup;
	bool			is_leaf;
	
	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	off = ABT_P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);
	is_leaf = ABT_P_ISLEAF(opaque);
	
	/* The page should not be empty. */
	Assert(off < maxoff);
	itemid = PageGetItemId(page, off);
	itup = (IndexTuple) PageGetItem(page, itemid);
	_abt_accum_index_tuple_value(agg_info, &agg, itup, is_leaf, true);

	for (++off; off <= maxoff; ++off)
	{
		itemid = PageGetItemId(page, off);
		itup = (IndexTuple) PageGetItem(page, itemid);
		_abt_accum_index_tuple_value(agg_info, &agg, itup, is_leaf, false);
	}

	return agg;
}

ABTStack
_abt_inverse_stack(ABTStack stack)
{
	ABTStack prev;

	if (!stack) return NULL;
	prev = NULL;

	while (stack)
	{
		ABTStack t;
		t = stack->abts_parent;
		stack->abts_parent = prev;
		prev = stack;
		stack = t;
	}

	return prev;
}

/*
 * Set up abts_child links in the stack and returns the bottom item in the
 * stack.
 */
ABTStack
_abt_stack_setup_child_link(ABTStack stack)
{
	ABTStack prev;
	uint32 level = 1;

	prev = NULL;
	while (stack)
	{
		stack->abts_child = prev;
		stack->abts_level = level++;
		prev = stack;
		stack = stack->abts_parent;
	}

	return prev;
}

