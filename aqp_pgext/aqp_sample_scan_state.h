/*-------------------------------------------------------------------------
 *
 * aqp_sample_scan_state.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AQP_SAMPLE_SCAN_STATE_H
#define AQP_SAMPLE_SCAN_STATE_H

#include "aqp.h"

#include <utils/float.h>
#include <utils/sampling.h>

extern bool aqp_tablesample_swr_ask_for_samples_attempted;

/* ----------------
 * Additional states for swr sample scans. This struct is the common part
 * of AQPSWRScan and AQPSWRIndexOnlyScan.
 *      SampleSize         the total number of samples to fetch
 *		RemSampleSize	   the remaining number of samples to fetch
 *		NumIndexAccessAttempted
 *						   the number of amsampletuple() call we have made.
 *		HighRejectionRateWarningThreshold
 *						   if NumIndexAccessAttempted is over the threshold, we
 *						   emit a warning to the user about the high rejection
 *						   rate.
 *		Seed			   the seed to use across rescans if repeatable_expr
 *						   is not set
 *		RescanForRuntimeKeysOnly
 *						   true if we are calling from ExecIndexOnlyScan()
 *						   which should only set up the new runtime keys
 *						   rather than re-initialize the sampling. It will
 *						   be reset to false after the ReScan call.
 *		RandState		   the random state of the random number generator
 *		RepeatableExprState the plan state of the repeatable expression
 *		SampleSizeExprState the plan state of the sample size expression
 * ----------------
 */
typedef struct AQPSampleScanState
{
	uint64		SampleSize;
	uint64		RemSampleSize;
	double		HighRejectionRateWarningThreshold;
	uint64		NumIndexAccessAttempted;
	uint32		Seed;
	bool		RescanForRuntimeKeysOnly;
	SamplerRandomState	RandState;
	ExprState	*RepeatableExprState;
	ExprState	*SampleSizeExprState;
} AQPSampleScanState;

/*
 * Increment and check the number of index accesses we have made for the
 * warning of the high rejection rate.
 */
static inline void
aqp_sample_scan_check_for_high_rejection_rate(AQPSampleScanState *node)
{
	if (++node->NumIndexAccessAttempted >
		node->HighRejectionRateWarningThreshold)
	{
		ereport(WARNING,
				(errcode(ERRCODE_WARNING),
				 errmsg("The rejection rate of tablesample swr is higher than "
						"99.9%%. It could because the range is empty. Consider "
						"aborting and retry the query without tablesample "
						"swr.")));

		/* 
		 * Setting the threshold to +inf so that we will only emit one
		 * warning for one plan.
		 */
		node->HighRejectionRateWarningThreshold = get_float8_infinity();
	}
}

static inline double
aqp_sample_scan_next_random_number(AQPSampleScanState *node)
{
	return sampler_random_fract(node->RandState);
}

static inline void
aqp_sample_scan_got_new_sample(AQPSampleScanState *node)
{
	--node->RemSampleSize;
}

static inline void
aqp_sample_scan_new_sample_rejected(AQPSampleScanState *node)
{
	++node->RemSampleSize;
}

static inline bool
aqp_sample_scan_no_more_samples(AQPSampleScanState *node)
{
	if (node->RemSampleSize == 0) {
		if (aqp_tablesample_swr_ask_for_samples_attempted)
			ereport(INFO, (errmsg("N=" UINT64_FORMAT,
								  node->NumIndexAccessAttempted)));
		return true;
	}
	return false;
}

static inline void
aqp_sample_scan_mark_rescan_for_runtime_keys_only(AQPSampleScanState *node)
{
	node->RescanForRuntimeKeysOnly = true;
}

extern void aqp_sample_scan_init(AQPSampleScanState *node,
								 PlanState *planstate,
								 Expr *repeatable_expr,
								 uint64 sample_size,
								 Expr *sample_size_expr);
extern void aqp_sample_scan_start(AQPSampleScanState *node,
									ExprContext *econtext);

extern void aqp_sample_scan_add_guc_entry(void);

#endif						/* AQP_SAMPLE_SCAN_STATE_H */
