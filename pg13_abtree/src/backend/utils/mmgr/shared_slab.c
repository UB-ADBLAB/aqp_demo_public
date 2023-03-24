/*-------------------------------------------------------------------------
 * 
 * shared_slab.c
 *		A dynamic shared slab allocator backed by an dsa.
 *
 * IDENTIFICATION
 *		src/backend/utils/mmgr/shared_slab.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/spin.h"
#include "utils/memutils.h"
#include "utils/dynahash.h"
#include "utils/shared_slab.h"

#define SHARED_SLAB_MAGIC					0xefca7345
#define SHARED_SLAB_CHUNK_MODIFYING_MASK	0x1
#define shared_slab_chunk_is_marked(ptr) \
	(((ptr) & SHARED_SLAB_CHUNK_MODIFYING_MASK) != 0)
#define shared_slab_chunk_get_unmarked(ptr) \
	((ptr) & ~(shared_slab_pointer) SHARED_SLAB_CHUNK_MODIFYING_MASK)
#define shared_slab_chunk_get_marked(ptr) \
	((ptr) | SHARED_SLAB_CHUNK_MODIFYING_MASK)

/* keep in sync with DSA_OFFSET_WIDTH dsa.c */
#if SIZEOF_DSA_POINTER == 4
#define SHARED_SLAB_SEGMENT_NUM_BITS	27
#else
#define SHARED_SLAB_SEGMENT_NUM_BITS	40
#endif

/* The maximum size a chunk can be. */
#define SHARED_SLAB_SEGMENT_MAX_ALLOCSIZE	(((Size) 1) << SHARED_SLAB_SEGMENT_NUM_BITS)

/* 1 KB */ 
#define SHARED_SLAB_MAX_ITEMSZ	((Size) 1024)

/* 1 GB */
#define SHARED_SLAB_DEFAULT_CHUNK_SIZE		((Size) 1 * 1024 * 1024 * 1024)

/* the maximum number of segments */
#define SHARED_SLAB_MAX_NUM_CHUNKS			64

static void shared_slab_reclaim_internal(shared_slab *slab, bool wait);
static void shared_slab_reclaim_first_dealloc_list(shared_slab *slab);


Size
shared_slab_shmem_size(void)
{
	return sizeof(shared_slab_shared_header) +
		sizeof(dsa_pointer) * SHARED_SLAB_MAX_NUM_CHUNKS;
}

shared_slab *
shared_slab_init(void *shared_ptr, dsa_area *area, Size itemsz,
                 Size dealloc_list_capacity, bool dealloc_nowait)
{
	shared_slab *slab;

	if (itemsz >= SHARED_SLAB_MAX_ITEMSZ)
	{
		elog(ERROR, "shared_slab supports item size up to %lu, but got %lu",
				SHARED_SLAB_MAX_ITEMSZ, itemsz);
	}
	
	slab = (shared_slab *) palloc0(sizeof(shared_slab) +
					sizeof(void *) * SHARED_SLAB_MAX_NUM_CHUNKS);
	slab->shared = (shared_slab_shared_header *) shared_ptr;
	slab->area = area;
	slab->chunk_local_ptrs = (char**)
		(((char *) slab) + sizeof(void *) * SHARED_SLAB_MAX_NUM_CHUNKS);
	
	slab->dealloc_mcxt = AllocSetContextCreate(CurrentMemoryContext,
											"shared slab dealloc list context",
											ALLOCSET_DEFAULT_SIZES);
    slab->dealloc_nowait = dealloc_nowait;
    slab->dealloc_list_capacity = dealloc_list_capacity;
    slab->dealloc_list = NULL;
    slab->last_dealloc_list = NULL;

	slab->shared->magic = SHARED_SLAB_MAGIC;
	slab->shared->itemsz = MAXALIGN(itemsz);
	Assert(slab->shared->itemsz >= sizeof(shared_slab_pointer));
	slab->shared->chunk_alloc_size =
		Min(SHARED_SLAB_DEFAULT_CHUNK_SIZE, SHARED_SLAB_SEGMENT_MAX_ALLOCSIZE);
	slab->shared->address_offset_num_bits =
		my_log2(slab->shared->chunk_alloc_size);
	slab->shared->address_offset_mask =
		(((Size) 1) << slab->shared->address_offset_num_bits) - 1;
	
	Assert(SHARED_SLAB_NULL == 0);
	shared_slab_pointer_atomic_init(&slab->shared->freelist_head, SHARED_SLAB_NULL);
	shared_slab_pointer_atomic_init(&slab->shared->new_chunk, SHARED_SLAB_NULL);
	/* this forces shared_slab_allocate_new_chunk to alllocate a new one */
	pg_atomic_init_u64(&slab->shared->unallocated_offset, slab->shared->chunk_alloc_size);
	slab->shared->chunks = (dsa_pointer *)
		(((char *) shared_ptr) + sizeof(void *) * SHARED_SLAB_MAX_NUM_CHUNKS);

	return slab;
}

shared_slab *
shared_slab_create(void *shared_ptr, dsa_area *area,
                   Size dealloc_list_capacity, bool dealloc_nowait)
{
	shared_slab_shared_header *shared;
	shared_slab *slab;

	shared = (shared_slab_shared_header *) shared_ptr;
	if (shared->magic != SHARED_SLAB_MAGIC)
	{
		elog(ERROR, "unable to create a shared slab on a shared memory that "
				"was not initialized by us");
	}
	
	slab = (shared_slab *) palloc0(sizeof(shared_slab) +
			sizeof(void *) * SHARED_SLAB_MAX_NUM_CHUNKS);
	slab->shared = shared;
	slab->area = area;
	slab->chunk_local_ptrs = (char**)
		(((char *) slab) + sizeof(void *) * SHARED_SLAB_MAX_NUM_CHUNKS);

	slab->dealloc_mcxt = AllocSetContextCreate(CurrentMemoryContext,
											"shared slab dealloc list context",
											ALLOCSET_DEFAULT_SIZES);
    slab->dealloc_nowait = dealloc_nowait;
    slab->dealloc_list_capacity = dealloc_list_capacity;
    slab->dealloc_list = NULL;
    slab->last_dealloc_list = NULL;
	return slab;
}

void
shared_slab_allocate_new_chunk(shared_slab *slab)
{
	SpinDelayStatus			delay_status;
	shared_slab_pointer		cur;
	Size					old_chunk_num,
							new_chunk_num;
	dsa_pointer				new_chunk;
	shared_slab_pointer		new_chunk_addr;
	uint64					offset;
	
	init_local_spin_delay(&delay_status);

retry:
	cur = shared_slab_pointer_atomic_read(&slab->shared->new_chunk);
	if (shared_slab_chunk_is_marked(cur))
	{
		/* someone's also trying to allocate a new one, wait... */
		perform_spin_delay(&delay_status);
		offset = pg_atomic_read_u64(&slab->shared->unallocated_offset);
		if (offset + slab->shared->itemsz <= slab->shared->chunk_alloc_size)
		{
			/* that some guy has done the job for us. */
			finish_spin_delay(&delay_status);
			return ;
		}
		goto retry;
	}
	else
	{
		if (!shared_slab_pointer_atomic_compare_exchange(
				&slab->shared->new_chunk,
				&cur,
				shared_slab_chunk_get_marked(cur)))
		{
			/* did someone do the job for us? */
			offset = pg_atomic_read_u64(&slab->shared->unallocated_offset);
			if (offset + slab->shared->itemsz <= slab->shared->chunk_alloc_size)
			{
				/* yes, just finish... */
				finish_spin_delay(&delay_status);
				return ;
			}
			goto retry;
		}
	}

	finish_spin_delay(&delay_status);

	/* double check if we really need to reallocate: */
	offset = pg_atomic_read_u64(&slab->shared->unallocated_offset);
	if (offset + slab->shared->itemsz <= slab->shared->chunk_alloc_size)
	{
		/* someone else has allocated a new chunk... */
		cur = shared_slab_chunk_get_marked(cur);

		/* don't forget to change the pointer back... */
		while (!shared_slab_pointer_atomic_compare_exchange(
					&slab->shared->new_chunk,
					&cur, shared_slab_chunk_get_unmarked(cur)));
		return ;
	}
	
	/* allocate a new chunk here */
	old_chunk_num = SHARED_SLAB_PTR_GET_CHUNK_NUM(slab, cur);
	new_chunk_num = old_chunk_num + 1;
	new_chunk = dsa_allocate_extended(slab->area, slab->shared->chunk_alloc_size,
									  DSA_ALLOC_HUGE | DSA_ALLOC_ZERO);
	new_chunk_addr =
		SHARED_SLAB_CHUNK_NUM_OFFSET_GET_PTR(slab, new_chunk_num, 0);
	slab->shared->chunks[new_chunk_num] = new_chunk;
	
	/* update the new chunk pointer in the shared header */
	pg_atomic_write_u64(&slab->shared->unallocated_offset, 0);
	pg_write_barrier();
	cur = shared_slab_chunk_get_marked(cur);
	while (!shared_slab_pointer_atomic_compare_exchange(&slab->shared->new_chunk,
				&cur, new_chunk_addr));

	elog(LOG, "shared_slab allocated a new chunk %lu @ 0x%016lX",
			new_chunk_num, new_chunk_addr);
}

void
shared_slab_destroy(shared_slab *slab)
{
	/* nothing else for now */
	MemoryContextDelete(slab->dealloc_mcxt);
	pfree(slab);
}

shared_slab_pointer
shared_slab_allocate_extended(shared_slab *slab)
{
	shared_slab_pointer		chunk_head;
	uint64					offset;
	uint64					end;

	/* try the trunk */
retry_chunk:
	offset = pg_atomic_read_u64(&slab->shared->unallocated_offset);
	while ((end = offset + slab->shared->itemsz) <=
		   slab->shared->chunk_alloc_size)
	{
		/* must read offset first, then the chunk head */
		pg_read_barrier();
		chunk_head = shared_slab_pointer_atomic_read(&slab->shared->new_chunk);
		/* someone's allocating a new chunk; retry to see if we can use that */
		if (shared_slab_chunk_is_marked(chunk_head))
		{
			goto retry_chunk;
		}
		/* otherwise, try to CAS the offset */
		if (pg_atomic_compare_exchange_u64(&slab->shared->unallocated_offset,
										   &offset,
										   end))
		{
			/* success. */
			return chunk_head + offset;
		}
	}

	/* allocate a new one chunk? */
	shared_slab_allocate_new_chunk(slab);
	
	/* try free list again? */
	chunk_head = shared_slab_try_allocate_from_freelist(slab);
	if (chunk_head != SHARED_SLAB_NULL)
	{
		return chunk_head;
	}
	goto retry_chunk;
}

void
shared_slab_reclaim(shared_slab *slab, bool wait)
{
    if (!slab->last_dealloc_list)
        return;

    if (slab->dealloc_list->size == 0)
        return ;
    
    if (slab->last_dealloc_list->size != 0 &&
        slab->last_dealloc_list->epoch == InvalidEpoch)
    {
        slab->last_dealloc_list->epoch = epoch_bump();
    }
    
    shared_slab_reclaim_internal(slab, wait);
}

static void
shared_slab_reclaim_internal(shared_slab *slab, bool wait)
{
    Epoch safe_epoch;

    safe_epoch = epoch_refresh_safe_epoch();
    while (slab->dealloc_list->epoch != InvalidEpoch)
    {
        if (!EpochPreceedsOrEqualsTo(slab->dealloc_list->epoch, safe_epoch))
        {
            if (wait)
            {
                epoch_wait_until_safe(slab->dealloc_list->epoch);
            }
            else
            {
                return ;
            }
        }
    
        /* We should have a safe epoch now. Do the actual reclamation. */
        shared_slab_reclaim_first_dealloc_list(slab);
        
        /* Remove any deallocation list other than the last one. */
        if (slab->dealloc_list != slab->last_dealloc_list)
        {
            shared_slab_dealloc_list *head;
            head = slab->dealloc_list;
            slab->dealloc_list = head->next_list;
            pfree(head);
        }
    }
}

static void
shared_slab_reclaim_first_dealloc_list(shared_slab *slab)
{
    Size                    i;
    shared_slab_pointer     tail;
    shared_slab_pointer     *p_next;
    
    Assert(slab->dealloc_list);
    Assert(slab->dealloc_list->size > 0);
    Assert(EpochPreceedsOrEqualsTo(slab->dealloc_list->epoch,
                                   epoch_get_safe_epoch()));
    
    for (i = 0; i < slab->dealloc_list->size - 1; ++i)
    {
        p_next = shared_slab_get_address(slab, slab->dealloc_list->pointers[i]);
        *p_next = slab->dealloc_list->pointers[i + 1];
    }
    
    tail = slab->dealloc_list->pointers[slab->dealloc_list->size - 1];
    p_next = shared_slab_get_address(slab, tail);
    *p_next = shared_slab_pointer_atomic_read(&slab->shared->freelist_head);
    while (!shared_slab_pointer_atomic_compare_exchange(
            &slab->shared->freelist_head, p_next, tail));

    slab->dealloc_list->epoch = InvalidEpoch;
    slab->dealloc_list->size = 0;
}

void
shared_slab_free(shared_slab *slab, shared_slab_pointer ptr)
{
    bool alloc_new_list = false;

    if (!slab->last_dealloc_list)
    {
        alloc_new_list = true;
    }
    else if (slab->last_dealloc_list->size == slab->last_dealloc_list->capacity)
    {
        Epoch cur_epoch = epoch_bump();    
        slab->last_dealloc_list->epoch = cur_epoch;

        if (slab->dealloc_nowait)
        {
            shared_slab_reclaim_internal(slab, false);

            /* 
             * If we may not reclaim the last dealloc list at this time,
             * we need to allocate a new one to hold the newly freed ptr.
             */
            if (slab->last_dealloc_list->epoch != InvalidEpoch)
            {
                alloc_new_list = true;
            }
        }
        else
        {
            shared_slab_reclaim_internal(slab, true);
        }
    }
    
    if (alloc_new_list)
    {
        shared_slab_dealloc_list *new_list;

        new_list = (shared_slab_dealloc_list *) MemoryContextAlloc(
			slab->dealloc_mcxt,
            sizeof(shared_slab_dealloc_list) +
            sizeof(shared_slab_pointer) * slab->dealloc_list_capacity);
        new_list->epoch = InvalidEpoch;
        new_list->size = 0;
        new_list->capacity = slab->dealloc_list_capacity;
        new_list->next_list = NULL;
        if (slab->last_dealloc_list)
        {
            slab->last_dealloc_list->next_list = new_list;
            slab->last_dealloc_list = new_list;
        }
        else
        {
            slab->dealloc_list = slab->last_dealloc_list = new_list;
        }
    }

    Assert(slab->last_dealloc_list->epoch == InvalidEpoch);
    Assert(slab->last_dealloc_list->size < slab->last_dealloc_list->capacity);
    slab->last_dealloc_list->pointers[slab->last_dealloc_list->size] = ptr;
    ++slab->last_dealloc_list->size;
}

