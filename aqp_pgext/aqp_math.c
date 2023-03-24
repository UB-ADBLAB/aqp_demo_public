#include "aqp.h"

#include <catalog/pg_type.h>
#include <utils/array.h>
#include <utils/float.h>

#include "aqp_math.h"

PG_FUNCTION_INFO_V1(aqp_erf_inv_pg);
Datum
aqp_erf_inv_pg(PG_FUNCTION_ARGS)
{
    double y = PG_GETARG_FLOAT8(0);
    double x = aqp_erf_inv(y);
    PG_RETURN_FLOAT8(x);
}

PG_FUNCTION_INFO_V1(aqp_clt_half_ci_finalfunc);
Datum
aqp_clt_half_ci_finalfunc(PG_FUNCTION_ARGS)
{
    ArrayType   *transarray = PG_GETARG_ARRAYTYPE_P(0);
    float8      *transvalue;
    float8      confidence = PG_GETARG_FLOAT8(1);
    float8      nsamples = PG_GETARG_FLOAT8(2);
    float8      Sxx;
    float8      ci;
    
    if (ARR_NDIM(transarray) != 1 ||
        ARR_DIMS(transarray)[0] != 3 ||
        ARR_HASNULL(transarray) ||
        ARR_ELEMTYPE(transarray) != FLOAT8OID)
    {
        elog(ERROR, "aqp_clt_half_ci_finalfunc: expected 3-element float8 array");
    }
    transvalue = (float8 *) ARR_DATA_PTR(transarray);
    
    /* 
     * N = transvalue[0] = num_accepted_samples
     * N' = nsamples
     * Sx = transvalue[1] = \sum_{i = 1}^N Xi = \sum_{i = 1}^N' Xi
     * Sxx = transvalue[2] = \sum_{i = 1}^N (Xi - Sx/N)^2
     *
     * The following is essentially calling float8_combine() over the
     * transarray and another transarray with values {N' - N, 0, 0} where the
     * second transition array corresponds to the aggregated values of the
     * rejected samples. (see backend/utils/adt/float.c).
     */
    if (transvalue[0] == 0.0)
    {
        Sxx = 0.0;
    }
    else
    {
        float8 N2 = nsamples - transvalue[0];
        if (N2 <= 0.0)
        {
            Sxx = transvalue[2];
        }
        else
        {
            float8 tmp;

            /* Sxx1 + Sxx2 + N1 * N2 * (Sx1 / N1 - Sx2 / N2)^2 / (N1 + N2) */
            tmp = transvalue[1] / transvalue[0];
            Sxx = transvalue[2]
                + transvalue[0] * (nsamples - transvalue[0]) / nsamples
                    * tmp * tmp;
            if (unlikely(isinf(Sxx)))
                float_overflow_error();
        }
    }

    if (nsamples <= 1)
        PG_RETURN_NULL();
    
    ci = aqp_erf_inv(confidence) *
        sqrt(2 * Sxx / (((float8) nsamples) * (nsamples - 1)));
    PG_RETURN_FLOAT8(ci); 
}

