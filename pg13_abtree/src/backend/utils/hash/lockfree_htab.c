/*-------------------------------------------------------------------------
 *
 * lockfree_htab.c
 *		A lock free open hash table that has a fixed number of buckets.
 *
 *		The linked list in the hash bucket is a slightly modified
 *		implementation of "A pragmatic implementation of Non-Blocking
 *		Linked-Lists" by Timothy L. Harris. In particular, an inserting thread
 *		is not allowed to remove nodes marked for deletion. Instead, it spins
 *		until that mark is cleared.
 *
 * IDENTIFICATION
 *	  src/backend/utils/hash/dynahash.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/backendid.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/shared_slab.h"
#include "utils/dynahash.h"
#include "utils/lockfree_htab.h"

/* 
 * TODO remove me
 * We cap the size of pending_deletable_nodes at 64 MB (assuming each ptr is 8
 * bytes) in case we have many threads that want to delete things.
 */
/*#define LFHTAB_MAX_PENDING_DELETABLE	8388608
#define LFHTAB_MIN_PENDING_DELETABLE	256 */

typedef shared_slab_pointer_atomic lfhtab_ptr_atomic;
#define lfhtab_ptr_atomic_init shared_slab_pointer_atomic_init
#define lfhtab_ptr_atomic_read shared_slab_pointer_atomic_read
#define lfhtab_ptr_atomic_write shared_slab_pointer_atomic_write
#define lfhtab_ptr_atomic_compare_exchange shared_slab_pointer_atomic_compare_exchange
#define LFHTAB_PTR_FORMAT SHARED_SLAB_POINTER_FORMAT

#define LFHTAB_NULL SHARED_SLAB_NULL
#define lfhtab_alloc() shared_slab_allocate(lfhtab->mcxt)
#define lfhtab_alloc0() shared_slab_allocate0(lfhtab->mcxt);
#define lfhtab_rawptr(ptr) ((lfhtab_elem *) shared_slab_get_address(lfhtab->mcxt, (ptr)))
#define lfhtab_free(ptr) shared_slab_free(lfhtab->mcxt, (ptr))

#define lfhtab_ptr_is_marked(ptr) (((ptr) & 1) != 0)
#define lfhtab_ptr_mark(ptr) \
	(AssertMacro(!lfhtab_ptr_is_marked(ptr)), ((ptr) ^ 1))
#define lfhtab_ptr_unmark(ptr) \
	(AssertMacro(lfhtab_ptr_is_marked(ptr)), ((ptr) ^ 1))
#define lfhtab_ptr_get_unmarked(ptr) ((ptr) & ~(lfhtab_ptr) 1)
#define lfhtab_ptr_get_marked(ptr) ((ptr) | 1)

#define swap(x, y) x ^= y, y ^= x, x ^= y

/* global shared header */
typedef struct lfhtab_header {
	pg_atomic_uint32			refcnt;
	uint32						hashval_mask;
	bool						has_sentinel_value;
	uint64						sentinel_value;
} lfhtab_header;

struct lockfree_htab {
	lfhtab_header				*header;

	/* local contents */
	lfhtab_memory_context		mcxt;
	lfhtab_ptr					cached_new_node;
	/*Size						num_pending_deletable_nodes;
	Size						num_pending_deletable_nodes_2;
	Size						pending_deletable_nodes_capacity;
	lfhtab_ptr					*pending_deletable_nodes;
	lfhtab_ptr					*pending_deletable_nodes_2; */

	/* shared data */
	lfhtab_ptr_atomic			*buckets;
};

/* per-backend */
typedef struct lfhtab_elem {
	uint64						key;
	uint64						value;
	lfhtab_ptr_atomic			next;
    uint64                      unused; /* align to 32 bytes */
} lfhtab_elem;

static uint32 lockfree_htab_get_hashval_mask(uint32 num_buckets);
static void lockfree_htab_create_internal(lockfree_htab *lfhtab,
										  lfhtab_memory_context mcxt);
static void lockfree_htab_destroy_internal(lockfree_htab *lfhtab);
/*static void lockfree_htab_reclaim_pending_deletable_nodes(
	lockfree_htab *lfhtab); */
static void lockfree_htab_free_node(lockfree_htab *lfhtab, lfhtab_ptr node);
static void lockfree_htab_free_nodes(lockfree_htab *lfhtab, lfhtab_ptr begin,
									 lfhtab_ptr end);

static uint32
lockfree_htab_get_hashval_mask(uint32 num_buckets)
{
	int bit;
	uint32 ret;

	if (num_buckets > UINT32_MAX / 2)
		num_buckets  = UINT32_MAX / 2;
	bit = my_log2(num_buckets);
	ret = (((uint32) 1) << bit) - 1;
	Assert(ret > 0 && ret <= UINT32_MAX / 2);
	return ret;
}

Size
lockfree_htab_get_elem_size(void)
{
	return sizeof(lfhtab_elem);
}

uint32
lockfree_htab_get_num_buckets(lockfree_htab *lfhtab)
{
	return lfhtab->header->hashval_mask + 1;
}

Size
lockfree_htab_get_shared_size(uint32 num_buckets)
{
	Size size;

	num_buckets = lockfree_htab_get_hashval_mask(num_buckets) + 1;
	size = MAXALIGN(sizeof(lfhtab_header));
	size = add_size(size, MAXALIGN(mul_size(sizeof(lfhtab_elem), num_buckets)));
	return size;
}

/*
 * If has_sentinel_value, sentinel_value is a special value that marks an entry
 * as to be deleted very soon. This will cause a HASH_FIND returns NULL,
 * a HASH_ENTER insert after it and a HASH_REMOVE only remove those whose
 * values marked as sentinel.
 */
lockfree_htab *
lockfree_htab_init(uint32 num_buckets,
				   void *shared_buf,
				   lfhtab_memory_context mcxt,
				   bool has_sentinel_value,
				   uint64 sentinel_value)
{
	lockfree_htab *lfhtab = (lockfree_htab *) palloc(sizeof(lockfree_htab));

	num_buckets = lockfree_htab_get_hashval_mask(num_buckets) + 1;
	memset(shared_buf, 0, lockfree_htab_get_shared_size(num_buckets));

	lfhtab->header = (lfhtab_header *) shared_buf;
	pg_atomic_init_u32(&lfhtab->header->refcnt, 0);
	lfhtab->header->hashval_mask = num_buckets - 1;
	lfhtab->header->has_sentinel_value = has_sentinel_value;
	if (has_sentinel_value)
		lfhtab->header->sentinel_value = sentinel_value;
	
	lockfree_htab_create_internal(lfhtab, mcxt); 

	return lfhtab;
}

lockfree_htab *
lockfree_htab_create(void *shared_buf, lfhtab_memory_context mcxt)
{
	lockfree_htab *lfhtab = (lockfree_htab *) palloc(sizeof(lockfree_htab));
	lfhtab->header = (lfhtab_header *) shared_buf;
	lockfree_htab_create_internal(lfhtab, mcxt);
	return lfhtab;
}

static void
lockfree_htab_create_internal(lockfree_htab *lfhtab, lfhtab_memory_context mcxt)
{
	char *shared_buf_cur = (char *) lfhtab->header;
	
	pg_atomic_fetch_add_u32(&lfhtab->header->refcnt, 1);
	lfhtab->mcxt = mcxt;
	shared_buf_cur += MAXALIGN(sizeof(lfhtab_header));
	lfhtab->buckets = (lfhtab_ptr_atomic *) shared_buf_cur;
	shared_buf_cur += MAXALIGN(mul_size(sizeof(lfhtab_elem),
						lfhtab->header->hashval_mask + 1));

	lfhtab->cached_new_node = LFHTAB_NULL;

	/*lfhtab->num_pending_deletable_nodes = 0;
	lfhtab->num_pending_deletable_nodes_2 = 0;
	if (may_delete_from_this_backend)
	{
		lfhtab->pending_deletable_nodes_capacity = LFHTAB_MAX_PENDING_DELETABLE / 2;
	}
	else
	{
		lfhtab->pending_deletable_nodes_capacity =
			Min(LFHTAB_MIN_PENDING_DELETABLE, 3 * MaxBackends);
	}
	lfhtab->pending_deletable_nodes = (lfhtab_ptr *) palloc(
		sizeof(lfhtab_ptr) * lfhtab->pending_deletable_nodes_capacity * 2);
	lfhtab->pending_deletable_nodes_2 =
		lfhtab->pending_deletable_nodes + lfhtab->pending_deletable_nodes_capacity;
	memset(lfhtab->pending_deletable_nodes, 0, sizeof(lfhtab_ptr) *
			lfhtab->pending_deletable_nodes_capacity * 2); */
}

static void
lockfree_htab_destroy_internal(lockfree_htab *lfhtab)
{
	Assert(pg_atomic_read_u32(&lfhtab->header->refcnt) == 0);
	/* XXX we're not trying to free the memory here */
}

void
lockfree_htab_destroy(lockfree_htab *lfhtab)
{
	if (lfhtab->cached_new_node != LFHTAB_NULL)
		lfhtab_free(lfhtab->cached_new_node);

	if (pg_atomic_fetch_sub_u32(&lfhtab->header->refcnt, 1) == 1)
	{
		lockfree_htab_destroy_internal(lfhtab);
	}

	pfree(lfhtab);
}

void
lockfree_htab_pin(lockfree_htab *lfhtab)
{
	pg_atomic_fetch_add_u32(&lfhtab->header->refcnt, 1);
}

void
lockfree_htab_unpin(lockfree_htab *lfhtab)
{
#ifdef USE_ASSERT_CHECKING
	Assert(
#endif
		pg_atomic_fetch_sub_u32(&lfhtab->header->refcnt, 1)
#ifdef USE_ASSERT_CHECKING
			> 1)
#endif
	;
}

/*static void
lockfree_htab_reclaim_pending_deletable_nodes(lockfree_htab *lfhtab)
{
	lfhtab_ptr *t;
	int i;

	for (i = 0; i < lfhtab->num_pending_deletable_nodes_2; ++i)
	{
		lfhtab_free(lfhtab->pending_deletable_nodes_2[i]);	
	}

	t = lfhtab->pending_deletable_nodes;
	lfhtab->pending_deletable_nodes = lfhtab->pending_deletable_nodes_2;
	lfhtab->pending_deletable_nodes_2 = t;
	lfhtab->num_pending_deletable_nodes_2 = lfhtab->num_pending_deletable_nodes;
	lfhtab->num_pending_deletable_nodes = 0;
} */

static void
lockfree_htab_free_node(lockfree_htab *lfhtab, lfhtab_ptr node)
{
	/* just call to shared_slab to free the node */
	lfhtab_free(node);

	/*while (unlikely(lfhtab->pending_deletable_nodes_capacity ==
		   lfhtab->num_pending_deletable_nodes))
	{
		lockfree_htab_reclaim_pending_deletable_nodes(lfhtab);
	}

	lfhtab->pending_deletable_nodes[lfhtab->num_pending_deletable_nodes++] =
		node; */
}

static void
lockfree_htab_free_nodes(lockfree_htab *lfhtab, lfhtab_ptr begin,
						 lfhtab_ptr end)
{
	lfhtab_elem		*elem;
	lfhtab_ptr		next;

	if (begin == LFHTAB_NULL)
	return ;
	
	begin = lfhtab_ptr_unmark(begin);
	while (begin != end)
	{
		elem = lfhtab_rawptr(begin);
		next = lfhtab_ptr_atomic_read(&elem->next);
		lockfree_htab_free_node(lfhtab, begin);
		begin = lfhtab_ptr_unmark(next);
	}
}


/*
 * action \in {HASH_FIND, HASH_ENTER, HASH_REMOVE}. HASH_ENTER_NULL is not
 * valid here.
 *
 * For HASH_FIND, it returns the pointer to the value in the entry if found.
 * Otherwise NULL.
 *
 * For HASH_ENTER, it returns the pointer to the value in the entry.
 *
 * For HASH_REMOVE, it returns NULL if nothing is removed by this call. Some
 * non-NULL pointer (not safe to deference) if it is removed by this call. The
 * old value of the removed item is stored in *old_val if old_val if non-null.
 * 
 */
uint64 *
lockfree_htab_find(lockfree_htab *lfhtab,
				   uint32 hashval,
				   uint64 key,
				   HASHACTION action,
				   uint64 *old_val)
{
	lfhtab_ptr_atomic	*left_node_next_ptr;
	lfhtab_ptr			left_node_next;
	lfhtab_ptr			cur;
	lfhtab_ptr_atomic	*cur_next_ptr;
	lfhtab_elem			*cur_elem;
	lfhtab_ptr			next;
	lfhtab_ptr			new_node = LFHTAB_NULL;
	lfhtab_elem			*new_elem = NULL;
	SpinDelayStatus		delay_status;
	lfhtab_ptr			removed_begin;
	lfhtab_ptr			removed_end = LFHTAB_NULL;
	bool				remove_succeeded = false;
	
	Assert(action != HASH_ENTER_NULL);

	hashval = hashval & lfhtab->header->hashval_mask;

	/* 
	 * set up the new node content early if we have a cached one to reduce
	 * the amount of cycles during the insertion.
	 */
	if (action == HASH_ENTER)
	{
		if (lfhtab->cached_new_node != LFHTAB_NULL)
		{

			new_node = lfhtab->cached_new_node;
			lfhtab->cached_new_node = LFHTAB_NULL;
		}
		else
		{
			new_node = lfhtab_alloc0();
		}
		new_elem = lfhtab_rawptr(new_node);
		new_elem->key = key;
		Assert(new_elem->value == 0);

		init_local_spin_delay(&delay_status);
	}

try_again:
	left_node_next_ptr = NULL;
	left_node_next = LFHTAB_NULL;
	cur = LFHTAB_NULL; /* shouldn't be referenced at all before line 224*/
	cur_next_ptr = &lfhtab->buckets[hashval];
	cur_elem = NULL;
	next = lfhtab_ptr_atomic_read(cur_next_ptr);
	/* exit is at the end of the loop */
	for (;;)
	{

		/* The first link cannot be marked. It can be null though. */
		Assert(cur != LFHTAB_NULL || !lfhtab_ptr_is_marked(next));

		/* 
		 * record the last unmarked reference we found and move cur to
		 * the next item 
		 */
		if (!lfhtab_ptr_is_marked(next)) {
			left_node_next_ptr = cur_next_ptr;
			left_node_next = next;
			cur = next;
		} 
		else
		{
			cur = lfhtab_ptr_unmark(next);
		}

try_again_fastpath:

		/* move to the next node as the current */
		if (cur == LFHTAB_NULL)
		{
			break;
		}
		cur_elem = lfhtab_rawptr(cur);
		Assert(cur_elem);
		cur_next_ptr = &cur_elem->next;

		/* Read the next pointer of the new current item. */
		next = lfhtab_ptr_atomic_read(cur_next_ptr);
	
		/*
		 * Decide whether we can break out of the loop.
		 * Generally we should stop if we are at a node with an unmarked
		 * next key (meaning it's not deleted), and its key is not smaller
		 * than the insertion key. But there might be fast path for insertion
		 * and find (see below).
		 */
		if (cur_elem->key >= key)
		{
			/* 
			 * find can stop at any node whose key >= the insertion key
			 */
			if (action == HASH_FIND || action == HASH_ENTER)
			{
				break;
			}
			
			/*
			 * HASH_REMOVE can stop at some unmarked item at this point. But it
			 * shouldn't stop if that unmarked item doesn't have the sentinel
			 * value set (once set, will not be cleared until removed).
			 */
			if (!lfhtab_ptr_is_marked(next))
			{
				break;
			}
		}
	}
	
	Assert(cur == LFHTAB_NULL || cur_elem != NULL);
	
	switch (action)
	{
	case HASH_FIND:
		/* 
		 * We have a valid node with a matching key and its value wasn't set
		 * the sentinel value. 
		 */
		if (cur != LFHTAB_NULL && cur_elem->key == key &&
				!lfhtab_ptr_is_marked(next) &&
			(!lfhtab->header->has_sentinel_value ||
			 pg_atomic_read_u64((pg_atomic_uint64 *) &cur_elem->key) !=
			 lfhtab->header->sentinel_value))
		{
			return &cur_elem->value;
		}
		
		/* not found  */
		return NULL;

	case HASH_ENTER:
		/* 
		 * We have a valid node with a matching key and its value wasn't set
		 * the sentinel value. Cache the new node for use next time HASH_ENTER
		 * call and return its value pointer.
		 */
		while (cur != LFHTAB_NULL && cur_elem->key == key)
		{
			if (!lfhtab_ptr_is_marked(next) &&
			    (!lfhtab->header->has_sentinel_value ||
				pg_atomic_read_u64((pg_atomic_uint64 *) &cur_elem->key) !=
				lfhtab->header->sentinel_value))
			{
				lfhtab->cached_new_node = new_node;
				finish_spin_delay(&delay_status);
				return &cur_elem->value;
			}
			else
			{
				/* 
				 * The item is being deleted or is going to be deleted. Spin
				 * a bit for that to happen.
				 */
				perform_spin_delay(&delay_status);

				/* get the current item again */
				cur = lfhtab_ptr_atomic_read(left_node_next_ptr);
				if (lfhtab_ptr_is_marked(cur))
				{
					/* oops, left node is also deleted in the meantime... */
					goto try_again;
				}
				left_node_next = cur;
				cur_elem = lfhtab_rawptr(cur);
				next = lfhtab_ptr_atomic_read(&cur_elem->next);
				/* 
				 * Now, we can check again if this is the right item to return,
				 * or we should insert a new one, or wait for a bit longer?
				 */
			}
		}
	
		/* 
		 * We will attempt an insertion even if we stopped at some deleted key
		 * or some to-be-deleted key. At this point, we only need to ensure
		 * left node and new node won't be reclaimed once inserted.
		 */
		lfhtab_ptr_atomic_write(&new_elem->next, cur);
		/* lfhtab_ptr_compare_exhcnage has full memory barrier semantics */
		if (lfhtab_ptr_atomic_compare_exchange(left_node_next_ptr,
											   &left_node_next,
											   new_node))
		{
			/* successful insert, we're done */
			finish_spin_delay(&delay_status);
			return &new_elem->value;
		}

		/* CAS failed, need to retry. */
		break;
		
	case HASH_REMOVE:
		/*
		 * left node is not adjacent to cur, try to remove one or more marked
		 * nodes 
		 */
		Assert(cur == LFHTAB_NULL || !lfhtab_ptr_is_marked(next));
		if (left_node_next != cur)
		{
			/* 
			 * Remove the marked nodes between left and cur. Save the begin
			 * of the nodes removed before it's overwritten.
			 */
			removed_begin = left_node_next;
			removed_end = cur;
			if (!lfhtab_ptr_atomic_compare_exchange(left_node_next_ptr,
												    &left_node_next,
												    cur))
			{
				/* CAS failed, retry */	
				break;
			}
			/* 
			 * Starting from this point, we need to deallocate the nodes from
			 * removed_begin to removed_end no matter what happens. But it
			 * should be delayed until we are certain we can return from
			 * the call or retry.
			 */
			left_node_next = cur;
		} else {
			removed_begin = LFHTAB_NULL;
		}
	
		/* left and cur should be adjacent now */
		Assert(!lfhtab_ptr_is_marked(left_node_next));
		if (remove_succeeded)
		{
			/* 
			 * We are here just because we weren't able to physically remove a
			 * node marked for deletion. At this point, we are sure that we
			 * don't have to worry for that possibility. Note that, even if now
			 * cur stops at a node with a matching key and a sentinel value, it
			 * must not be the node we previously marked, as that node must
			 * have been between left_node_next and cur when we enter this
			 * brach if it hadn't been removed.
			 */
			lockfree_htab_free_nodes(lfhtab, removed_begin, removed_end);
			return (uint64 *) 1;
		}

		/* 
		 * We don't have a candidate to remove from the list if any of these is
		 * true:
		 * 1. cur is NULL.
		 * 2. key doesn't match
		 * 3. If a sentinel value is provided in iniitalization, the value in
		 * the entry doesn't match the sentinel value.
		 */
		if (cur == LFHTAB_NULL || cur_elem->key != key ||
			(lfhtab->header->has_sentinel_value &&
			 pg_atomic_read_u64((pg_atomic_uint64 *) &cur_elem->value) !=
				lfhtab->header->sentinel_value))
		{
			/* didn't find the item, we're done. */
			lockfree_htab_free_nodes(lfhtab, removed_begin, removed_end);
			return NULL;
		}
		
		/* 
		 * Try to mark the next of the current node for deletion if not
		 * already. Read the next link again before we attempt the CAS since
		 * it could have been a while since we last read next.
		 *
		 * If someone managed to mark the next pointer before we can, just
		 * accept that and continue to physically remove the node. If someone
		 * managed to insert an unmarked new node after us, we'll have to retry.
		 * In any case, we shouldn't have to spin for too long unless there's
		 * a constant stream of insertions right after us (TODO detect that?).
		 */
		next = lfhtab_ptr_atomic_read(&cur_elem->next);
		while (!lfhtab_ptr_is_marked(next))
		{
			if (lfhtab_ptr_atomic_compare_exchange(&cur_elem->next, &next,
												   lfhtab_ptr_mark(next)))
			{
				break;
			}
		}

		/* 
		 * cur is now marked for deletion. Try to physically unlink it from
		 * left.
		 */
		remove_succeeded = true;
		next = lfhtab_ptr_get_unmarked(next);
		if (!lfhtab_ptr_atomic_compare_exchange(left_node_next_ptr,
												&left_node_next,
												next))
		{
			/* 
			 * CAS failed to remove the link to the marked node, which is fine.
			 * We'll just need to do one more round of search to make sure it
			 * is really removed. remove_succeeded makes sure that next time we
			 * won't try to mark additional nodes with a matching key.
			 *
			 * Before we continue, we need to free the removed nodes and save
			 * the old value if requested.
			 */	
			lockfree_htab_free_nodes(lfhtab, removed_begin, removed_end);
			if (old_val)
				*old_val = cur_elem->value;
			break;
		}

		/* we're done */
		lockfree_htab_free_nodes(lfhtab, removed_begin, removed_end);
		if (old_val)
			*old_val = cur_elem->value;
		lockfree_htab_free_node(lfhtab, cur);
		return (uint64*) 1;

	default:
		elog(ERROR, "unexpected action: %d", action);
	}


	/* 
	 * CAS on left_node_next_ptr failed in HASH_ENTER or HASH_REMOVE cases.
	 * Need to restart. Can we start from left node? 
	 *
	 * At this point, the new value read from left_node_next_ptr is stored
	 * in left_node_next. left_hp points to the left node. cur_hp may have been
	 * cleared and next_hp may point to the new_node or was already cleared.
	 *
	 * Other local variables are not changed since the search loop.
	 */
	if (lfhtab_ptr_is_marked(left_node_next))
	{
		/* left node is deleted... start over. */
		goto try_again;
	}
	else
	{
		/* 
		 * not marked, left node is still valid, we can start the search
		 * from the left_node 
		 */
		/* left_node_next_ptr, left_node_next doesn't change */
		next = left_node_next;
		cur = next; /* start from moving to the next */
		/* cur_next_ptr, cur_elem will be immediately overwritten */
		goto try_again_fastpath;
	}

	elog(ERROR, "unreachable");
	return NULL;
}

void
lockfree_htab_iter_init(lockfree_htab *lfhtab,
						lockfree_htab_iter_status *iter_status)
{
	iter_status->next_bucket = 0;
	iter_status->max_bucket = lfhtab->header->hashval_mask;
	iter_status->next_ptr = LFHTAB_NULL;
}

uint64 *
lockfree_htab_iter_next(lockfree_htab *lfhtab,
						lockfree_htab_iter_status *iter_status)
{
	lfhtab_elem *elem;	
	uint32 hashval;

	if (iter_status->next_ptr == LFHTAB_NULL)
	{
		while (iter_status->next_bucket <= iter_status->max_bucket)
		{
			hashval = iter_status->next_bucket++;
			iter_status->next_ptr =
				lfhtab_ptr_atomic_read(&lfhtab->buckets[hashval]);
			Assert(!lfhtab_ptr_is_marked(iter_status->next_ptr));
			if (iter_status->next_ptr != LFHTAB_NULL)
			{
				goto return_entry;
			}
		}

		/* end of the iteration */
		if (iter_status->next_bucket > iter_status->max_bucket)
			return NULL;
	}

return_entry:
	elem = lfhtab_rawptr(iter_status->next_ptr);
	iter_status->next_ptr = lfhtab_ptr_atomic_read(&elem->next);
	return &elem->value;
}

uint64
lockfree_htab_value_ptr_get_key(uint64 *value)
{
	lfhtab_elem *elem = (lfhtab_elem *) (
		(char *) value - offsetof(lfhtab_elem, value));
	return elem->key;
}

