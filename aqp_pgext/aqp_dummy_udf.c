#include "aqp.h"

#include <utils/lsyscache.h>


PG_FUNCTION_INFO_V1(aqp_dummy_func);
Datum
aqp_dummy_func(PG_FUNCTION_ARGS)
{
    Oid fn_oid = fcinfo->flinfo->fn_oid;
    char *fn_name = get_func_name(fn_oid);
    if (!fn_name) {
        StaticAssertDecl(sizeof(Oid) == 4, "assuming Oid is 4 bytes");
        fn_name = palloc(11); // 4294967295\0
        snprintf(fn_name, 11, "%u", fn_oid);
    }
    ereport(ERROR,
            errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("%s in a query plan should have been rewritten by AQP "
                   "extension", fn_name));
}

