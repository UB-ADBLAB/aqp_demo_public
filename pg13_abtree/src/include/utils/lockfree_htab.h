/*-------------------------------------------------------------------------
 *
 * lockfree_htab.h
 *		A lock free open hash table that has a fixed number of buckets.
 *
 * src/include/utils/lockfree_htab.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCKFREE_HTAB_H
#define LOCKFREE_HTAB_H

#include "postgres.h"

#include "common/hashfn.h"
#include "utils/shared_slab.h"
#include "utils/hsearch.h"
	
/*
 * Different from HTAB where the elements are owned by the hash table,
 * lockfree_htab does not own any of the items, which are up to the caller
 * to allocate or deallocate.
 */
typedef struct lockfree_htab lockfree_htab;
typedef shared_slab *lfhtab_memory_context;
typedef shared_slab_pointer lfhtab_ptr;

/*
 * The data structure used for iterating through the buckets.
 *
 * Note that, we allow the caller delete the previously returned item, but a
 * concurrent deleter may prevent us from returning all remaining items, even
 * if they are never deleted. Hence, we don't bother setting the hazard
 * pointer for the entries returned by the iteration.
 *
 * XXX if someone does have a need for concurrent deleter, consider set the
 * hazard pointers and come up with some way to recover a scan position.
 */
typedef struct lockfree_htab_iter_status
{
	uint32		next_bucket;
	uint32		max_bucket;
	lfhtab_ptr	next_ptr;
}
lockfree_htab_iter_status;

#define lockfree_htab_hash_uint64(key) \
	hash_combine(hash_uint32((uint32)(key)), \
				 hash_uint32((uint32)((key) >> 32)))

extern Size lockfree_htab_get_elem_size(void);
extern uint32 lockfree_htab_get_num_buckets(lockfree_htab *lfhtab);
extern Size lockfree_htab_get_shared_size(uint32 num_buckets);
extern lockfree_htab *lockfree_htab_init(uint32 num_buckets,
										 void *shared_buf,
										 lfhtab_memory_context mcxt,
										 bool has_sentinal_value,
										 uint64 sentinal_value);
extern lockfree_htab *lockfree_htab_create(void *shared_buf,
										   lfhtab_memory_context mcxt);
extern void lockfree_htab_destroy(lockfree_htab *lfhtab);
extern void lockfree_htab_pin(lockfree_htab *lfhtab);
extern void lockfree_htab_unpin(lockfree_htab *lfhtab);
extern uint64 *lockfree_htab_find(lockfree_htab *lfhtab,
								  uint32 hashval,
								  uint64 key,
								  HASHACTION action,
								  uint64 *old_val);
extern void lockfree_htab_iter_init(lockfree_htab *lfhtab,
									lockfree_htab_iter_status *iter_status);
extern uint64 *lockfree_htab_iter_next(lockfree_htab *lfhtab,
									   lockfree_htab_iter_status *iter_status);
extern uint64 lockfree_htab_value_ptr_get_key(uint64 *value);

static inline uint64 *
lockfree_htab_find_u32(lockfree_htab *lfhtab,
					   uint32 key,
					   HASHACTION action,
					   uint64 *old_val)
{
	return lockfree_htab_find(
		lfhtab, hash_uint32(key), key, action, old_val);
}

static inline uint64 *
lockfree_htab_find_u64(lockfree_htab *lfhtab,
					   uint64 key,
					   HASHACTION action,
					   uint64 *old_val)
{
	return lockfree_htab_find(
		lfhtab, lockfree_htab_hash_uint64(key), key, action, old_val);
}

#endif							/* LOCKFREE_HTAB_H */
