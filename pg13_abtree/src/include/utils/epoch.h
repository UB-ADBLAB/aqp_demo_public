/*-------------------------------------------------------------------------
 *
 * epoch.h
 *      Epoch manager for GC/delayed ops.
 *
 * IDENTIFICATION
 *      src/include/utils/epoch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EPOCH_H
#define EPOCH_H

#include "port/atomics.h"
#include "storage/backendid.h"

typedef BackendId       EpochEntryId;
typedef uint64          Epoch;

#define EpochEntryNotInUseMask  (((Epoch) 1) << 63)
#define EpochEntryNumberMask    (EpochEntryNotInUseMask - 1)
#define InvalidEpochEntryId     InvalidBackendId

#define EpochEntryInUse(a) (((a) & EpochEntryNotInUseMask) == 0)
#define InvalidEpoch            (~((Epoch) 0))

#define EPOCH_REFRESH_INTERVAL 5000

/*
 * Epoch numbers use the lower 63 bits of Epoch. An epoch number preceeds
 * another if it is greater than the latter minus 2^62 in the modula 2^63 ring.
 */
static inline bool
EpochPreceedsOrEqualsTo(Epoch a, Epoch b)
{
    Assert(EpochEntryInUse(a) && EpochEntryInUse(b));
    return ((b - a) & EpochEntryNumberMask) < (((Epoch) 1) << 62);
}

#define EpochPreceeds(a, b) (!EpochPreceedsOrEqualsTo((b), (a)))

extern EpochEntryId     MyEpochEntryId;

extern Size EpochShmemSize(void);
extern void EpochShmemInit(void);

/* 
 * This must be called by all backends through postinit, as well as the
 * postmatser, to ensure the clean up hook is set up.
 */
extern void EpochSetup(void);

/*
 * Returns true if a valid MyEpochEntryId is allcoated.
 */
extern bool epoch_acquire(void);

/*
 * Release the local epoch entry of the calling backend.
 */
extern void epoch_release(void);

/*
 * Refresh the epoch if there has been more than EPOCH_REFRESH_INTERVAL calls
 * to this function at the local backend.
 */
extern Epoch epoch_maybe_refresh(void);

/* 
 * Refresh the local epoch number by setting it to the one in the global
 * counter. This might trigger deferred actions so memory barrier is required.
 */
extern Epoch epoch_refresh(void);

/*
 * Get the local epoch without refreshing it.
 */
extern Epoch epoch_get(void);

/* 
 * Returns the old epoch number in the global counter. Also refresh the local
 * epoch number.
 */
extern Epoch epoch_bump(void);

/* Recompute and get the safe epoch number. */
extern Epoch epoch_refresh_safe_epoch(void);

/* Get the safe epoch number. */
extern Epoch epoch_get_safe_epoch(void);

/* Wait until the target_epoch is safe. */
extern Epoch epoch_wait_until_safe(Epoch target_epoch);

#endif /* EPOCH_H */
