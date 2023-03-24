EXTENSION = aqp
MODULE_big = aqp
DATA = aqp--0.0.1.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

OBJS = aqp.o aqp_planner.o aqp_dummy_udf.o aqp_samplepath.o pg_indxpath.o \
	   aqp_swrscan.o aqp_sample_scan_state.o pg_explain.o \
	   aqp_tablesample_swr.o aqp_explain.o aqp_approx_agg_rewriter.o \
	   aqp_math.o

include $(PGXS)
