LOAD 'MODULE_PATHNAME';

CREATE FUNCTION sample_prob() RETURNS FLOAT8
LANGUAGE 'c' VOLATILE STRICT PARALLEL SAFE AS 'MODULE_PATHNAME', 'aqp_dummy_func';

CREATE FUNCTION inv_sample_prob() RETURNS FLOAT8
LANGUAGE 'c' VOLATILE STRICT PARALLEL SAFE AS 'MODULE_PATHNAME', 'aqp_dummy_func';

CREATE FUNCTION erf_inv(FLOAT8) RETURNS FLOAT8
LANGUAGE 'c' IMMUTABLE STRICT PARALLEL SAFE AS 'MODULE_PATHNAME', 'aqp_erf_inv_pg';

CREATE FUNCTION swr(INTERNAL) RETURNS TSM_HANDLER
LANGUAGE 'c' VOLATILE AS 'MODULE_PATHNAME', 'aqp_tablesample_swr_tsm_handler';

CREATE FUNCTION dummy_f8_func_f8(FLOAT8) RETURNS FLOAT8
LANGUAGE 'c' PARALLEL SAFE AS 'MODULE_PATHNAME', 'aqp_dummy_func';

CREATE FUNCTION dummy_f8_func_f8_f8(FLOAT8, FLOAT8) RETURNS FLOAT8
LANGUAGE 'c' PARALLEL SAFE AS 'MODULE_PATHNAME', 'aqp_dummy_func';

CREATE FUNCTION dummy_f8_func_f8_any(FLOAT8, "any") RETURNS FLOAT8
LANGUAGE 'c' PARALLEL SAFE AS 'MODULE_PATHNAME', 'aqp_dummy_func';

CREATE FUNCTION dummy_f8_func_f8_any_f8(FLOAT8, "any", FLOAT8) RETURNS FLOAT8
LANGUAGE 'c' PARALLEL SAFE AS 'MODULE_PATHNAME', 'aqp_dummy_func';

CREATE AGGREGATE float8_accum(FLOAT8) (
    SFUNC = float8_accum,
    STYPE = FLOAT8[],
    INITCOND = '{0, 0, 0}'
);

CREATE FUNCTION clt_half_ci_finalfunc(FLOAT8[], FLOAT8, FLOAT8)
RETURNS FLOAT8
LANGUAGE 'c' IMMUTABLE STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'aqp_clt_half_ci_finalfunc';

CREATE AGGREGATE approx_sum("any") (
    SFUNC = dummy_f8_func_f8_any,
    STYPE = FLOAT8
);

CREATE AGGREGATE approx_sum_half_ci("any", FLOAT8) (
    SFUNC = dummy_f8_func_f8_any_f8,
    STYPE = FLOAT8
);

CREATE AGGREGATE approx_count(*) (
    SFUNC = dummy_f8_func_f8,
    STYPE = FLOAT8
);

CREATE AGGREGATE approx_count_star_half_ci(FLOAT8) (
    SFUNC = dummy_f8_func_f8_f8,
    STYPE = FLOAT8
);

CREATE AGGREGATE approx_count("any") (
    SFUNC = dummy_f8_func_f8_any,
    STYPE = FLOAT8
);

CREATE AGGREGATE approx_count_half_ci("any", FLOAT8) (
    SFUNC = dummy_f8_func_f8_any_f8,
    STYPE = FLOAT8
);

