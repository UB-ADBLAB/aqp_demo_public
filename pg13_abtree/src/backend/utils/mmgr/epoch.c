/*-------------------------------------------------------------------------
 *
 * epoch.c
 *      Epoch manager implementation for GC/delayed ops.
 *
 * IDENTIFICATION
 *      src/backend/utils/mmgr/epoch.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/epoch.h"

EpochEntryId        MyEpochEntryId = InvalidEpochEntryId;

typedef struct EpochManager {
    int32           NumEpochEntries;
    int32           :32;
    Epoch           GlobalEpoch;
    Epoch           SafeEpoch;
    ConditionVariable   cv;
    Epoch           LocalEpochs[0];
} EpochManager;
static EpochManager *epoch_manager;

static uint64 epoch_refresh_count = 0;

static inline Epoch
EpochIncrement(Epoch a)
{
    return (a + 1) & EpochEntryNumberMask;
}

static inline Epoch
EpochDecrement(Epoch a)
{
    return (a - 1) & EpochEntryNumberMask;
}

static void
epoch_shmem_exit(int code, Datum arg)
{
    if (IsPostmasterEnvironment && IsUnderPostmaster)
    {
        epoch_release();
		
		/*elog(LOG, "epoch_shmem_exit(), now safe epoch = %lu",
				epoch_get_safe_epoch()); */
        return ;
    }
}

Size
EpochShmemSize(void)
{
    Size        size = 0;

    size = add_size(size, sizeof(EpochManager) + sizeof(Epoch) * MaxBackends);
    return MAXALIGN(size);
}


void
EpochShmemInit(void)
{
    char    *shmem;
    bool    found;

    shmem = (char *) ShmemInitStruct("Epoch manager",
                                     EpochShmemSize(),
                                     &found);
    
    if (!IsUnderPostmaster)
    {
        int32 i;

        Assert(!found);
        epoch_manager = (EpochManager *) shmem;
        epoch_manager->NumEpochEntries = MaxBackends;
        epoch_manager->GlobalEpoch = 1;
        epoch_manager->SafeEpoch = 0;
        ConditionVariableInit(&epoch_manager->cv);
        for (i = 0; i < epoch_manager->NumEpochEntries; ++i)
        {
            epoch_manager->LocalEpochs[i] = EpochEntryNotInUseMask;
        }
    }
    else
    {
        Assert(found);
#ifdef EXEC_BACKEND
        epoch_manager = (EpochManager *) shmem;
#endif
    }
}

void
EpochSetup(void)
{
    before_shmem_exit(epoch_shmem_exit, 0);
}

bool
epoch_acquire(void)
{
    EpochEntryId        first,
                        i;
    Epoch               value,
                        global_epoch;

    if (MyEpochEntryId != InvalidEpochEntryId)
    {
        /* already allocated */
        return true;
    }
    
    first = MyBackendId % epoch_manager->NumEpochEntries;
    global_epoch = pg_atomic_read_u64(
        (pg_atomic_uint64*) &epoch_manager->GlobalEpoch);
    i = first;
    do {
        value = pg_atomic_read_u64(
            (pg_atomic_uint64*) &epoch_manager->LocalEpochs[i]);
retry:
        if (EpochEntryInUse(value))
        {
            i = (i + 1) % epoch_manager->NumEpochEntries;
            continue;
        }
        else
        {
            if (!pg_atomic_compare_exchange_u64(
                    (pg_atomic_uint64*) &epoch_manager->LocalEpochs[i],
                    &value, global_epoch))
                goto retry;
            MyEpochEntryId = i;
            epoch_refresh_count = 0;
            return true;
        }
    } while (i != first);

    MyEpochEntryId = InvalidEpochEntryId;
    return false;
}

void
epoch_release(void)
{
    if (MyEpochEntryId == InvalidEpochEntryId)
        return ;

    pg_atomic_write_u64(
        (pg_atomic_uint64*) &epoch_manager->LocalEpochs[MyEpochEntryId],
        EpochEntryNotInUseMask);
    MyEpochEntryId = InvalidEpochEntryId;
    epoch_refresh_safe_epoch();
}

Epoch
epoch_maybe_refresh(void)
{
    if (MyEpochEntryId == InvalidEpochEntryId)
        return InvalidEpoch;
    
    if (++epoch_refresh_count == EPOCH_REFRESH_INTERVAL)
    {
        epoch_refresh_count = 0;
        return epoch_refresh();
    }
    return epoch_get();
}

Epoch
epoch_refresh(void)
{
    Epoch           global_epoch;

    global_epoch = pg_atomic_read_u64(
        (pg_atomic_uint64*) &epoch_manager->GlobalEpoch);
    if (epoch_get() == global_epoch)
        return epoch_get();

    pg_atomic_write_u64(
        (pg_atomic_uint64*) &epoch_manager->LocalEpochs[MyEpochEntryId],
        global_epoch);
    epoch_refresh_safe_epoch();
    return global_epoch;
}

Epoch
epoch_get(void)
{
    return epoch_manager->LocalEpochs[MyEpochEntryId];
}

Epoch
epoch_bump(void)
{
    Epoch       global_epoch,
                old_global_epoch,
                new_global_epoch;

    global_epoch = pg_atomic_read_u64(
        (pg_atomic_uint64*) &epoch_manager->GlobalEpoch);

    for (;;)
    {
        old_global_epoch = global_epoch;
        new_global_epoch = EpochIncrement(global_epoch);
        if (pg_atomic_compare_exchange_u64(
                (pg_atomic_uint64*) &epoch_manager->GlobalEpoch,
                &global_epoch,
                new_global_epoch))
            break;
    }
    
    pg_atomic_write_u64(
        (pg_atomic_uint64*) &epoch_manager->LocalEpochs[MyEpochEntryId],
        new_global_epoch);
    return old_global_epoch;
}

Epoch
epoch_refresh_safe_epoch(void)
{
    int32       i;
    bool        first = true;
    Epoch       new_safe_epoch = 0,
                epoch,
                cur_safe_epoch;

    for (i = 0; i < epoch_manager->NumEpochEntries; ++i)
    {
        epoch = pg_atomic_read_u64(
            (pg_atomic_uint64*) &epoch_manager->LocalEpochs[i]);
        if (EpochEntryInUse(epoch))
        {
            if (first || EpochPreceeds(epoch, new_safe_epoch))
            {
                new_safe_epoch = epoch;
                first = false;
            }
        }
    }

    new_safe_epoch = EpochDecrement(new_safe_epoch);
    cur_safe_epoch = pg_atomic_read_u64(
        (pg_atomic_uint64*) &epoch_manager->SafeEpoch);

    /*
     * We only try to update the safe epoch iff the new one strictly follows
     * the current one and no backend concurrently tries to modify it.
     */
    if (EpochPreceeds(cur_safe_epoch, new_safe_epoch))
    {
        if (pg_atomic_compare_exchange_u64(
                (pg_atomic_uint64*) &epoch_manager->SafeEpoch,
                &cur_safe_epoch, new_safe_epoch))
        {
            cur_safe_epoch = new_safe_epoch;

            /*
             * Wakes up all waiters who might be waiting for a specific epoch
             * to become safe.
             */
            ConditionVariableBroadcast(&epoch_manager->cv);
        }
    }
    return cur_safe_epoch;
}

Epoch
epoch_get_safe_epoch(void)
{
    return pg_atomic_read_u64((pg_atomic_uint64*) &epoch_manager->SafeEpoch);
}

Epoch
epoch_wait_until_safe(Epoch target_epoch)
{
    Epoch safe_epoch = epoch_get_safe_epoch();
	
    while (EpochPreceeds(safe_epoch, target_epoch))
    {
		elog(LOG, "waiting for target_epoch = %lu (safe_epoch =  %lu)",
				target_epoch, safe_epoch);
        ConditionVariableSleep(&epoch_manager->cv, WAIT_EVENT_EPOCH_SAFETY);
        safe_epoch = epoch_get_safe_epoch();
    }
	elog(LOG, "finished waiting for target_epoch = %lu", target_epoch);
    ConditionVariableCancelSleep();
    return safe_epoch;
}

