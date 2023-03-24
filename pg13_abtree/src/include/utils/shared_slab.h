/*-------------------------------------------------------------------------
 *
 * shared_slab.h
 *	  Dynamic shared slab allocator backed by an dsa.
 *
 * IDENTIFICATION
 *	  src/include/utils/shared_slab.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARED_SLAB_H
#define SHARED_SLAB_H

#include "port/atomics.h"
#include "utils/dsa.h"
#include "utils/epoch.h"

typedef dsa_pointer shared_slab_pointer;
typedef dsa_pointer_atomic shared_slab_pointer_atomic;
#define shared_slab_pointer_atomic_init dsa_pointer_atomic_init
#define shared_slab_pointer_atomic_read dsa_pointer_atomic_read
#define shared_slab_pointer_atomic_write dsa_pointer_atomic_write
#define shared_slab_pointer_atomic_fetch_add dsa_pointer_atomic_fetch_add
#define shared_slab_pointer_atomic_compare_exchange \
	dsa_pointer_atomic_compare_exchange
#define SHARED_SLAB_POINTER_FORMAT DSA_POINTER_FORMAT

#define SHARED_SLAB_NULL	InvalidDsaPointer

typedef struct shared_slab_shared_header
{
	uint32							magic;
	Size							itemsz;
	Size							chunk_alloc_size;
	Size							address_offset_num_bits;
	Size							address_offset_mask;

	
	shared_slab_pointer_atomic		freelist_head;

	shared_slab_pointer_atomic		new_chunk;
	pg_atomic_uint64				unallocated_offset;

	dsa_pointer						*chunks;
} shared_slab_shared_header;

typedef struct shared_slab_dealloc_list
{
    Epoch                           epoch;
    Size                            size;
    Size                            capacity;
    struct shared_slab_dealloc_list *next_list;
    shared_slab_pointer             pointers[0];
} shared_slab_dealloc_list;

typedef struct shared_slab
{
	shared_slab_shared_header		*shared;
	dsa_area						*area;
	char							**chunk_local_ptrs;
	
	/* This is the memory context for allocating deallocation lists. */
	MemoryContext					dealloc_mcxt;
    bool                            dealloc_nowait;
    Size                            dealloc_list_capacity;
    shared_slab_dealloc_list        *dealloc_list;
    shared_slab_dealloc_list        *last_dealloc_list;
} shared_slab;

#define SHARED_SLAB_MAGIC				0xefca7345
#define SHARED_SLAB_PTR_GET_CHUNK_NUM(slab, ptr) \
	(((ptr) >> (slab)->shared->address_offset_num_bits) - 1)
#define SHARED_SLAB_PTR_GET_OFFSET(slab, ptr) \
	((ptr) & (slab)->shared->address_offset_mask)
#define SHARED_SLAB_CHUNK_NUM_OFFSET_GET_PTR(slab, chunk_num, offset) \
	((((shared_slab_pointer) ((chunk_num) + 1)) << \
	 (slab)->shared->address_offset_num_bits) + (offset))

extern Size shared_slab_shmem_size(void);

/*#define shared_slab_get_address(slab, ptr) dsa_get_address(slab->area, ptr) */
static inline void *shared_slab_get_address(shared_slab *slab,
											shared_slab_pointer ptr)
{
	Size chunk_num = SHARED_SLAB_PTR_GET_CHUNK_NUM(slab, ptr);
	shared_slab_pointer offset = SHARED_SLAB_PTR_GET_OFFSET(slab, ptr);
	if (!slab->chunk_local_ptrs[chunk_num])
	{
		slab->chunk_local_ptrs[chunk_num] =
			dsa_get_address(slab->area, slab->shared->chunks[chunk_num]);
	}

	Assert(slab->chunk_local_ptrs[chunk_num]);
	return slab->chunk_local_ptrs[chunk_num] + offset;
}

/*
 * dealloc_list_capacity is the maximum number of objects that may be freed in
 * one epoch before we attempt to either return these objects back to the free
 * list (if !dealloc_nowait), or creating a new list (if dealloc_nowait).
 */
extern shared_slab *shared_slab_init(void *shared_ptr, dsa_area *area,
									 Size itemsz, Size dealloc_list_capacity,
                                     bool dealloc_nowait);
extern shared_slab *shared_slab_create(void *shared_ptr, dsa_area *area,
                                       Size dealloc_list_capacity,
                                       bool dealloc_nowait);
/* should only be called by one caller */
extern void shared_slab_destroy(shared_slab *slab);
extern shared_slab_pointer shared_slab_allocate_extended(shared_slab *slab);
extern void shared_slab_allocate_new_chunk(shared_slab *slab); 

static inline shared_slab_pointer
shared_slab_try_allocate_from_freelist(shared_slab *slab)
{
	shared_slab_pointer		head;
	shared_slab_pointer		*p_next;
	
	/* try the free list */
	head = shared_slab_pointer_atomic_read(&slab->shared->freelist_head);
	while (head != SHARED_SLAB_NULL)
	{
		p_next = (shared_slab_pointer *) shared_slab_get_address(slab, head);
		if (shared_slab_pointer_atomic_compare_exchange(
				&slab->shared->freelist_head,
				&head,
				*p_next))
		{
			return head;
		}
	}
		
	return SHARED_SLAB_NULL;
}

/*
 * The inline part that only contains a few instructions.
 */
static inline shared_slab_pointer
shared_slab_allocate(shared_slab *slab)
{
	shared_slab_pointer	ptr;
	ptr = shared_slab_try_allocate_from_freelist(slab);
	if (ptr != SHARED_SLAB_NULL) return ptr;

	return shared_slab_allocate_extended(slab);
}

static inline shared_slab_pointer
shared_slab_allocate0(shared_slab *slab)
{
	shared_slab_pointer ptr;
	char				*mem;

	ptr = shared_slab_allocate(slab);
	mem = (char *) shared_slab_get_address(slab, ptr);
	memset(mem, 0, slab->shared->itemsz);

	return ptr;
}

/*
 * Return an object to the allocator but concurrent threads may still access
 * it until it is safe to reclaim.
 */
extern void shared_slab_free(shared_slab *slab, shared_slab_pointer ptr);

/*
 * Reclaim the objects previously freed. If wait is true, we'll wait until all
 * freed objects may be reclaimed. If wait is false, we'll reclaim as many
 * objects as possible without waiting.
 *
 * If there is any deallocation list in the local state, all except the last
 * one may be destroyed upon return. The last deallocation list is guanranteed
 * to be empty and its epoch set to invalid if wait == true, unless there's no
 * deallocation list upon entry.
 */
extern void shared_slab_reclaim(shared_slab *slab, bool wait);

#endif				/* SHARED_SLAB_H */
