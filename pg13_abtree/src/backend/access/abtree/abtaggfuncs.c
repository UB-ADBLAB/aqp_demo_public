/*-------------------------------------------------------------------------
 *
 * abtaggfuncs.c
 *	  Support functions for aggregate btrees.
 *
 * IDENTIFICATION
 *	  src/backend/access/abtree/abtaggfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "access/abtree.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "commands/defrem.h"
#include "parser/parse_oper.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static Oid _abt_find_operator(char *oprname, Oid left_arg, Oid right_arg);

static Oid
_abt_find_operator(char *oprname, Oid left_arg, Oid right_arg)
{
	Oid		opno;
	Value	*oprname_val = makeString(oprname);
	List	*qualified_oprname = list_make1(oprname_val);

	opno = LookupOperName(NULL, qualified_oprname, left_arg, right_arg,
						  true, -1);
	list_free(qualified_oprname);
	pfree(oprname_val);
	return opno;
}

Datum
abt_count_support(PG_FUNCTION_ARGS)
{
	Oid				agg_type_oid = PG_GETARG_OID(0);

	HeapTuple		htup;
	Form_pg_type	agg_type;
	Oid				int2_to_agg_type_fn_oid = InvalidOid;
	Oid				agg_type_to_int2_fn_oid = InvalidOid;
	char			castmethod;
	FmgrInfo		int2_to_agg_type_fmgrinfo;
	Datum			one;
	ABTAggSupport	supp;
	Size			one_offset,
					supp_size;
	Oid				add_op,
					mul_op,
					sub_op,
					opclass,
					opfamily;

	htup = SearchSysCache1(TYPEOID, agg_type_oid);
	if (!HeapTupleIsValid(htup))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("aggregation type %u does not exist. This shouldn't "
						"happen because we should have validated the option.",
						agg_type_oid)));
	}
	agg_type = (Form_pg_type) GETSTRUCT(htup);
	
	/* We need to cast int2 to and from agg type if they are not the same. */
	if (agg_type_oid != INT2OID)
	{
		int2_to_agg_type_fn_oid = get_cast_fn_oid(INT2OID, agg_type_oid,
												  &castmethod);
		if (!OidIsValid(int2_to_agg_type_fn_oid) && castmethod != 'b')
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("cast from int2 (short) to %s is missing "
							"or is based on I/O functions. Hint: "
							"aggregate btree requires non-int2 aggregation "
							"type to be able to cast to and from int2 with "
							"a cast function or through binary coercion.",
							NameStr(agg_type->typname))));
		}

		agg_type_to_int2_fn_oid = get_cast_fn_oid(agg_type_oid, INT2OID,
												  &castmethod);
		if (!OidIsValid(agg_type_to_int2_fn_oid) && castmethod != 'b')
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("cast from %s to int2 (short) is missing "
							"or is based on I/O functions. Hint: "
							"aggregate btree requires non-int2 aggregation "
							"type to be able to cast to and from int2 with "
							"a cast function or through binary coercion.",
							NameStr(agg_type->typname))));
		}
	}
	
	/* Now, create the 1 value in agg_type. */
	if (OidIsValid(int2_to_agg_type_fn_oid))
	{
		/* We have a regular cast function from int2 to agg_type to apply. */
		fmgr_info(int2_to_agg_type_fn_oid, &int2_to_agg_type_fmgrinfo);
		one = FunctionCall1(&int2_to_agg_type_fmgrinfo,
							Int16GetDatum(1));
	}
	else
	{
		/* Binary coercion is fine. */
		one = Int16GetDatum(1);
	}
	
	
	/* MAXALIGN shouldn't be necessary but just to be extra safe. */
	supp_size = MAXALIGN(sizeof(ABTAggSupportData));

	/* Unless agg_type is passed by ref, we don't need to allocate
	 * additional space for that. */
	if (!agg_type->typbyval)
	{
		if (agg_type->typlen > 0) {
			one_offset = supp_size;
			supp_size = MAXALIGN(supp_size + agg_type->typlen);
		} else {
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("aggregation_type must be of fixed length")));
		}
	}

	if (supp_size > ABTAS_MAX_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("ABTAggSupportData's size " UINT64_FORMAT
						" exceeds the maximum " UINT64_FORMAT,
						supp_size, ABTAS_SIZE_MASK)));
	}

	supp = (ABTAggSupport) palloc(supp_size);
	supp->abtas_flags = supp_size |
						ABTAS_MAP_IS_CONST |
						ABTAS_ADD_IS_REGPROC |
						ABTAS_MUL_IS_REGPROC |
						ABTAS_SUB_IS_REGPROC;
	supp->abtas_agg_type_oid = agg_type_oid;
	supp->abtas_int2_to_agg_type_fn_oid = int2_to_agg_type_fn_oid;
	supp->abtas_agg_type_to_int2_fn_oid = agg_type_to_int2_fn_oid;
	if (agg_type->typbyval) {
		supp->abtas_flags |= ABTAS_AGG_TYPE_IS_BYVAL;
		supp->abtas_map.val = one;
	} else {
		supp->abtas_map.off = one_offset;
		memcpy(((Pointer) supp) + one_offset, DatumGetPointer(one),
				agg_type->typlen);
	
		/* We're done with these temporary datum structs. */
		pfree(DatumGetPointer(one));
	}

	/* find add fn */
	add_op = _abt_find_operator("+", agg_type_oid, agg_type_oid);
	if (OidIsValid(add_op))
	{
		HeapTuple			htup2;
		Form_pg_operator	add_op_row;

		htup2 = SearchSysCache1(OPEROID, add_op);
		Assert(HeapTupleIsValid(htup2));
		add_op_row = (Form_pg_operator) GETSTRUCT(htup2);

		if (add_op_row->oprresult != agg_type_oid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("operator \"+\" is expected to be closed in the "
							"aggregation type")));
		}
		supp->abtas_add.oid = add_op_row->oprcode;
		ReleaseSysCache(htup2);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("operator \"+\" is not found for aggregation type %s",
						NameStr(agg_type->typname))));
	}
			

	/* find mul fn. agg_type * int2 is preferred if available. */
	mul_op = _abt_find_operator("*", agg_type_oid, INT2OID);
	if (OidIsValid(mul_op))
	{
		HeapTuple			htup2;
		Form_pg_operator	mul_op_row;

		htup2 = SearchSysCache1(OPEROID, mul_op);
		Assert(HeapTupleIsValid(htup2));
		mul_op_row = (Form_pg_operator) GETSTRUCT(htup2);

		if (mul_op_row->oprresult != agg_type_oid)
		{
			/* The operator returns a different type than agg_type.
			 * This happens either because the implementation is wrong or
			 * agg_type is narrower than int2. In this case, fall back to
			 * using a cast.
			 */
			mul_op = InvalidOid;
		}
		else
		{
			supp->abtas_mul.oid = mul_op_row->oprcode;
		}
		ReleaseSysCache(htup2);
	}
	
	/* 
	 * Now search for agg_type * agg_type if we haven't found the preferred
	 * op.
	 */
	if (!OidIsValid(mul_op))
	{
		HeapTuple			htup2;
		Form_pg_operator	mul_op_row;

		mul_op = _abt_find_operator("*", agg_type_oid, INT2OID);		
		if (!OidIsValid(mul_op))
		{
			/* still not found. Report the error here. */
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("operator \"*\" is not found for aggregation type "
							"%s",
							NameStr(agg_type->typname))));
		}

		htup2 = SearchSysCache1(OPEROID, mul_op);
		Assert(HeapTupleIsValid(htup2));
		mul_op_row = (Form_pg_operator) GETSTRUCT(htup2);

		if (mul_op_row->oprresult != agg_type_oid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("operator \"*\" is expected to be closed in the "
							"aggregation type")));
		}
		else
		{
			supp->abtas_mul.oid = mul_op_row->oprcode;
			supp->abtas_flags |= ABTAS_MUL_REQ_CAST_2ND_ARG;
		}
		ReleaseSysCache(htup2);
	}
	
	/* Find the - op. */
	sub_op = _abt_find_operator("-", agg_type_oid, agg_type_oid);
	if (OidIsValid(sub_op))
	{
		HeapTuple			htup2;
		Form_pg_operator	sub_op_row;

		htup2 = SearchSysCache1(OPEROID, sub_op);
		Assert(HeapTupleIsValid(htup2));
		sub_op_row = (Form_pg_operator) GETSTRUCT(htup2);

		if (sub_op_row->oprresult != agg_type_oid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("operator \"-\" is expected to be closed in the "
							"aggregation type")));
		}
		supp->abtas_sub.oid = sub_op_row->oprcode;
		ReleaseSysCache(htup2);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("operator \"-\" is not found for aggregation type %s",
						NameStr(agg_type->typname))));
	}
	
	/* Find the cast functions to and from float8 */
	if (agg_type_oid != FLOAT8OID)
	{
		supp->abtas_double_to_agg_type_fn_oid =
			get_cast_fn_oid(FLOAT8OID, agg_type_oid, &castmethod);
		if (!OidIsValid(supp->abtas_double_to_agg_type_fn_oid) &&
			castmethod != 'b')
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("cast from float8 (double) to %s is missing "
							"or is based on I/O functions. Hint: "
							"aggregate btree requires non-double aggregation "
							"type to be able to cast to and from double with "
							"a cast function or through binary coercion.",
							NameStr(agg_type->typname))));
		}

		supp->abtas_agg_type_to_double_fn_oid =
			get_cast_fn_oid(agg_type_oid, FLOAT8OID, &castmethod);
		if (!OidIsValid(supp->abtas_agg_type_to_double_fn_oid) &&
			castmethod != 'b')
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("cast from %s to float8 (double) is missing "
							"or is based on I/O functions. Hint: "
							"aggregate btree requires non-double aggregation "
							"type to be able to cast to and from double with "
							"a cast function or through binary coercion.",
							NameStr(agg_type->typname))));
		}
	}
	else
	{
		supp->abtas_double_to_agg_type_fn_oid = InvalidOid;
		supp->abtas_agg_type_to_double_fn_oid = InvalidOid;
	}
	
	/* We find the btree default opclass for the comparison proc. */
	opclass = GetDefaultOpClass(agg_type_oid, BTREE_AM_OID);
	if (!OidIsValid(opclass))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("default opclass for %s using btree is missing",
						NameStr(agg_type->typname))));
	}
	opfamily = get_opclass_family(opclass);
	if (!OidIsValid(opfamily))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("cannot find the opfamily of the opclass %d",
						opclass)));
	}
	supp->abtas_agg_type_btcmp_fn_oid = get_opfamily_proc(opfamily,
														  agg_type_oid,
														  agg_type_oid,
														  ABTORDER_PROC);
	if (!OidIsValid(supp->abtas_agg_type_btcmp_fn_oid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("cannot find the btree comparison operator for the "
						"aggregation type %s",
						NameStr(agg_type->typname))));
	}


	ReleaseSysCache(htup);
	return PointerGetDatum(supp);
}

