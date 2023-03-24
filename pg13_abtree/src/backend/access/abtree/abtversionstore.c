/*-------------------------------------------------------------------------
 *
 * abtversionstore.c
 *	  Support functions for versioning of the aggregates.
 *
 * IDENTIFICATION
 *	  src/backend/access/abtree/abtversionstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/abtree.h"
#include "access/transam.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/shared_slab.h"
#include "utils/lockfree_htab.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timeout.h"

#define swap(x, y) \
	(x ^= y, y ^= x, x ^= y)

typedef dsa_pointer			abtvs_ptr;
typedef dsa_pointer_atomic	abtvs_ptr_atomic;
#define abtvs_ptr_atomic_init dsa_pointer_atomic_init
#define abtvs_ptr_atomic_read dsa_pointer_atomic_read
#define abtvs_ptr_atomic_write dsa_pointer_atomic_write
#define abtvs_ptr_atomic_compare_exchange dsa_pointer_atomic_compare_exchange
#define ABTVS_PTR_FORMAT DSA_POINTER_FORMAT

/*
 * We use our customized shared slab allocator for the the version nodes.
 * However, everyone except for the initialization and destruction routines
 * should use abtvs_mcxt instead in case we want to use a different allocator
 * later.
 */
#define abtvs_mcxt abtvs_shared_slab
#define ABTVS_NULL SHARED_SLAB_NULL
#define abtvs_alloc() shared_slab_allocate(abtvs_mcxt)
#define abtvs_alloc0() shared_slab_allocate0(abtvs_mcxt)
#define abtvs_rawptr(ptr) \
	((ABTVSNode *) shared_slab_get_address(abtvs_mcxt, (ptr)))
#define abtvs_free(ptr) shared_slab_free(abtvs_mcxt, (ptr))

/*
 * We use the low second bit as a hint if we deleted all the version nodes from
 * a chain and will delete the entry from the hash table very soon. It doesn't
 * actually matter even if this mark is the same as in lockfree_htab, because
 * that is set on the bucket head ptr and next links of the bucket elements,
 * while this is set on the value fields of the bucket elements.
 *
 * There's only one case when this will be set:
 * 1) The garbage collector will mark the chain header while it is doing GC on
 * a chain.
 *
 * If any backend with a read-lock sees a marked value that is not
 * abtvs_deleted_chain_head, it should treat it as an unmarked value. If it
 * inserts anything before the head, the new head also needs to be marked and
 * the next pointer of the new head must not be marked.
 *
 * When a backend with a write-lock (which is replacing the entire chain) sees
 * a marked value that is not abtvs_deleted_chain_head, it should wait until
 * it gets unmarked or becomes abtvs_deleted_chain_head.
 */
#define ABTVS_DELETED_CHAIN_MASK	0x2
#define abtvs_deleted_chain_head	(abtvs_ptr) ABTVS_DELETED_CHAIN_MASK

#define abtvs_ptr_is_marked(ptr) (((ptr) & ABTVS_DELETED_CHAIN_MASK) != 0)
#define abtvs_ptr_mark(ptr) \
	(AssertMacro(!abtvs_ptr_is_marked(ptr)), (ptr) ^ ABTVS_DELETED_CHAIN_MASK)
#define abtvs_ptr_unmark(ptr) \
	(AssertMacro(abtvs_ptr_is_marked(ptr)), (ptr) ^ ABTVS_DELETED_CHAIN_MASK)
#define abtvs_ptr_get_marked(ptr) \
	((ptr) | ABTVS_DELETED_CHAIN_MASK)
#define abtvs_ptr_get_unmarked(ptr) \
	((ptr) & ~(abtvs_ptr) ABTVS_DELETED_CHAIN_MASK)


/* The shared struct that stores meta-info about the version store. */
#define ABTVS_MAGIC 0x347abe90
typedef struct ABTVSHeader
{
	uint32		magic;
} ABTVSHeader;

/* Version nodes */
typedef struct ABTVSNode
{
	TransactionId			xmin;
	abtvs_ptr_atomic		next;
	/* datums follows */
	union {
		Datum					datum;
		pg_atomic_uint64		atomic_u64;
		pg_atomic_uint32		atomic_u32;
		char					buf[sizeof(Datum)]; /* may be variable length */
	}						delta_val;
} ABTVSNode;

#define ABTVS_VERSION_NODE_FIXED_PART_SIZE \
	offsetof(ABTVSNode, delta_val)
StaticAssertDecl(MAXALIGN(ABTVS_VERSION_NODE_FIXED_PART_SIZE) ==
				 ABTVS_VERSION_NODE_FIXED_PART_SIZE,
				 "ABTVSNode's fixed header must be maxaligned");

#define ABTVS_VERSION_NODE_MAX_SIZE \
    TYPEALIGN64(32, (ABTVS_VERSION_NODE_FIXED_PART_SIZE + 8))
#define abtvs_alloc_version_node(agg_info) \
	(AssertMacro(agg_info->agg_typlen <= 8), abtvs_alloc0())

/* guc variables */

/*
 * We've removed this option because everything should come from dsm now... 
 * 8 MB is the minimum size dsa should initially be set to. 
 */
int abtree_version_store_initial_mem = 8; /* in MB */
int abtree_version_store_num_buckets;
int abtree_version_store_garbage_collector_delay; /* in milliseconds */

/* shared data */
static ABTVSHeader				*abtvs_header = NULL;
static shared_slab				*abtvs_shared_slab = NULL;
static shared_slab              *abtvs_shared_slab_for_htab = NULL;
static lockfree_htab			*abtvs_htab = NULL;

/* per backend data */
static abtvs_ptr				abtvs_cached_new_node = ABTVS_NULL;

/*
 * TODO are the sizes of the dealloc lists reasonable?
 */
#define ABTVS_HTAB_DEALLOC_LIST_SIZE_FOR_GC_BACKEND 1048576
#define ABTVS_HTAB_DEALLOC_LIST_SIZE_FOR_ORDINARY_BACKEND 64
#define ABTVS_VERSION_NODE_DEALLOC_LIST_SIZE_FOR_GC_BACKEND 8388608
#define ABTVS_VERSION_NODE_DEALLOC_LIST_SIZE_FOR_ORDINARY_BACKEND 8192

/*
 * This is an array of list where all nodes in these lists are deletable.  We
 * are certain that no one will insert/delete/access anything from the lists
 * once they are here, except for the list heads which might still be accessed
 * (but not inserted/deleted) concurrently as they are put into here.
 *
 * For GC backends, we cap the capacity at 131072.
 * For ordinary backends that haven't run any vacuum command, we cap it at 16,
 * or 8 splits in one descend, which should be more than enough in practice. In
 * the extreme case where we have 0x3ffff backends, the total size is capped at
 * 16 * 8 * 0x3ffff (about 32MB).
 *
 * TODO remove the following
 */
/*#define ABT_MAX_PENDING_DELETABLE_GC			8388608
#define ABT_MAX_PENDING_DELETABLE_USER_BACKEND	128
static Size						abtvs_num_pending_deletable = 0;
static Size						abtvs_num_pending_deletable_2 = 0;
static Size						abtvs_pending_deletable_capacity = 0;
static abtvs_ptr				*abtvs_pending_deletable = NULL;
static abtvs_ptr				*abtvs_pending_deletable_2 = NULL;
static uint64					abtvs_num_deletable_reclaimed = 0; */

typedef uint64 abtvs_key;
#define ABTVS_KEY(oid, blkno) \
	((((uint64) (oid)) << 32) | (uint64) (blkno))
#define ABTVS_KEY_GET_OID(key) \
	((Oid) ((key) >> 32))
#define ABTVS_KEY_GET_BLKNO(key) \
	((BlockNumber) (key))

typedef struct ABTVSItupGetXminItemPtr
{
	ItemIdData	itemid;
	int32		postingoff;
} ABTVSItupGetXminItemPtr;
StaticAssertDecl(sizeof(ABTVSItupGetXminItemPtr) == sizeof(uint64),
				 "ABTVSItupGetXminItemPtr's size should be 8 bytes");

#define ABTVS_GC_BGW_NAME	"abtree version store garbage collector"
#define ABTVS_GC_BGW_TYPE	"abtvs_gc"
StaticAssertDecl(sizeof(ABTVS_GC_BGW_NAME) <= BGW_MAXLEN &&
				 sizeof(ABTVS_GC_BGW_TYPE) <= BGW_MAXLEN,
				 "abtree version store garbage collector name or type is too "
				 "long for a background worker entry");


/* The number of NULL heads to accumulate before we attempt to remove them. */
#define ABTVS_GC_MAX_MAYBE_DELETABLE	8192
static Size						abtvs_maybe_deletable_capacity;
static Size						abtvs_num_maybe_deletable;
static abtvs_ptr_atomic			**abtvs_maybe_deletable;
static uint64					abtvs_num_maybe_deletable_deleted = 0;

static inline void
_abt_reset_gc_counters(void)
{
	abtvs_num_maybe_deletable_deleted = 0;
}

static void _abt_version_store_cleanup(int code, Datum arg);
static abtvs_ptr _abtvs_find_chain_head(abtvs_key key);
static abtvs_ptr_atomic * _abtvs_find_chain_head_for_insertion(abtvs_key key,
															abtvs_ptr *p_head);
static ABTVSNode * _abtvs_get_node_if_safe(abtvs_ptr node,
										   TransactionId oldestXmin);
static void _abtvs_replace_chain_head(abtvs_key key, abtvs_ptr new_head);
static void _abtvs_delete_chain(abtvs_ptr head);
/*static void _abtvs_reclaim_pending_deletables(void); */
static abtvs_ptr _abtvs_find_or_create_node(abtvs_key key, TransactionId xmin,
											ABTCachedAggInfo agg_info);
static uint64 _abtvs_exclude_agg_in_snapshot(abtvs_key key, uint64 agg,
											 ABTSnapshot snapshot,
											 ABTCachedAggInfo agg_info);
static void _abtvs_push_xmin_max_heap(uint64 *max_heap,
									  int i,
									  int n,
									  TransactionId (*get_xmin)(uint64, void *),
									  void *context);
static void _abtvs_make_xmin_max_heap(uint64 *max_heap,
									  int n,
									  TransactionId (*get_xmin)(uint64, void *),
									  void *context);
static TransactionId _abtvs_node_get_xmin(uint64 ptrval, void *context);
static TransactionId _abtvs_itup_get_xmin(uint64 ptrval, void *context);
static int _abt_compare_xid(const void *a, const void *b);
static void _abtvs_scan_and_recycle_old_versions(TransactionId oldestXmin);
static void _abtvs_init_maybe_deletable(void);
static void _abtvs_try_delete_maybe_deletable(void);
static void _abtvs_append_maybe_deletable(abtvs_ptr_atomic *head_ptr);

/********************************************************************
 * Section 1: initialization and destruction of shared data structures.
 ********************************************************************/

Size
ABTreeVersionStoreShmemSize(void)
{
	Size			size = 0;

	size = add_size(size, TYPEALIGN(64, (sizeof(ABTVSHeader))));
	/* one for hash table */
	size = add_size(size, TYPEALIGN(64, shared_slab_shmem_size()));
	/* the other for version nodes */
	size = add_size(size, TYPEALIGN(64, shared_slab_shmem_size()));
	size = add_size(size,
		TYPEALIGN(64, abtree_version_store_initial_mem * 1024));
	size = add_size(size, MAXALIGN(lockfree_htab_get_shared_size(
							abtree_version_store_num_buckets)));

	Assert(MAXALIGN(size) == size);
	return size;
}

void
ABTreeVersionStoreShmemInit(void)
{
	bool found;
	char *shmem;

	shmem = (char *) ShmemInitStruct("ABTree Version Store Initial Segment",
							ABTreeVersionStoreShmemSize(),
							&found);
	if (!IsUnderPostmaster)
	{
		Assert(!found);
		abtvs_header = (ABTVSHeader *) shmem;
		abtvs_header->magic = ABTVS_MAGIC;
	}
	else
	{
		Assert(found);
#ifdef EXEC_BACKEND
        abtvs_header = (ABTVSHeader *) shmem;
#endif
		Assert(abtvs_header->magic == ABTVS_MAGIC);
	}
}

void
ABTreeVersionStoreSetup(void)
{
	bool		is_postmaster = IsPostmasterEnvironment && !IsUnderPostmaster;
	char		*shmem;
	char		*shared_slab_shmem;
	char		*shared_slab_for_lfhtab_shmem;
	char		*dsa_place;
	char		*htab_data;
	MemoryContext	old_context;
	bool		is_gc_backend = false;
	dsa_area	*area;

	/*
	 * don't initialize the shared mem if this is not the postmaster, an
	 * autovac worker or an ordinary backend.
	 */
	if (MyBackendType != B_BG_WORKER && MyBackendType != B_BACKEND &&
		!is_postmaster)
	{
		/*ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("ABTreeVersionStoreSetup() shouldn't be called in "
					 "any process other than autovac worker, backend or "
					 "postmatser."))); */
		return ;
	}

	if (MyBackendType == B_BG_WORKER)
	{
		if (strcmp(MyBgworkerEntry->bgw_type, ABTVS_GC_BGW_TYPE) != 0)
		{
			return ;
		}
		is_gc_backend = true;
	}

	old_context = MemoryContextSwitchTo(TopMemoryContext);

	shmem = (char *) abtvs_header;
	shmem += TYPEALIGN(64, (sizeof(ABTVSHeader)));
	shared_slab_shmem = shmem;
	shmem += TYPEALIGN(64, shared_slab_shmem_size());
	shared_slab_for_lfhtab_shmem = shmem;
	shmem += TYPEALIGN(64, shared_slab_shmem_size());
	dsa_place = shmem;
	shmem += TYPEALIGN(64, abtree_version_store_initial_mem * 1024);
	htab_data = shmem;
	Assert(htab_data != dsa_place);

	if (!IsUnderPostmaster)
	{
		/* Need to initialize all the data structures. */
		area = dsa_create_in_place(
			dsa_place,
			abtree_version_store_initial_mem * 1024,
			LWTRANCHE_ABTREE_VERSION_STORE_DSA_AREA,
			NULL);

		abtvs_shared_slab = shared_slab_init(
            shared_slab_shmem, area,
			ABTVS_VERSION_NODE_MAX_SIZE,
            is_gc_backend
                ? ABTVS_VERSION_NODE_DEALLOC_LIST_SIZE_FOR_GC_BACKEND
                : ABTVS_VERSION_NODE_DEALLOC_LIST_SIZE_FOR_ORDINARY_BACKEND,
            /*dealloc_no_wait = */!is_gc_backend);
		abtvs_shared_slab_for_htab = shared_slab_init(
			shared_slab_for_lfhtab_shmem,
			area,
			lockfree_htab_get_elem_size(),
            is_gc_backend ? ABTVS_HTAB_DEALLOC_LIST_SIZE_FOR_GC_BACKEND
                          : ABTVS_HTAB_DEALLOC_LIST_SIZE_FOR_ORDINARY_BACKEND,
            /*dealloc_no_wait = */!is_gc_backend);

		abtvs_htab = lockfree_htab_init(abtree_version_store_num_buckets,
										 htab_data,
										 abtvs_shared_slab_for_htab,
										 true,
										 (uint64) abtvs_deleted_chain_head);

		if (is_postmaster)
		{
			/*
			 * pin and release the area so the forked subprocesses won't inherit
			 * the area.
			 */
			lockfree_htab_pin(abtvs_htab);
			lockfree_htab_destroy(abtvs_htab);
			abtvs_htab = NULL;

			shared_slab_destroy(abtvs_shared_slab);
			abtvs_shared_slab = NULL;
			shared_slab_destroy(abtvs_shared_slab_for_htab);
			abtvs_shared_slab_for_htab = NULL;

			dsa_pin(area);
			dsa_release_in_place(dsa_place);
			area = NULL;
		}
	}
	else
	{
		/* attach to the dsa area */
		area = dsa_attach_in_place(dsa_place, NULL);
		
		abtvs_shared_slab = shared_slab_create(
            shared_slab_shmem, area,
            is_gc_backend
                ? ABTVS_VERSION_NODE_DEALLOC_LIST_SIZE_FOR_GC_BACKEND
                : ABTVS_VERSION_NODE_DEALLOC_LIST_SIZE_FOR_ORDINARY_BACKEND,
            /*dealloc_nowait = */!is_gc_backend);
		abtvs_shared_slab_for_htab = shared_slab_create(
			shared_slab_for_lfhtab_shmem, area,
            is_gc_backend ? ABTVS_HTAB_DEALLOC_LIST_SIZE_FOR_GC_BACKEND
                          : ABTVS_HTAB_DEALLOC_LIST_SIZE_FOR_ORDINARY_BACKEND,
            /*dealloc_nowait = */!is_gc_backend);

		abtvs_htab = lockfree_htab_create(htab_data,
                                          abtvs_shared_slab_for_htab);
	}

	if (!IsPostmasterEnvironment || IsUnderPostmaster)
	{
		Assert(area);
		/*
		 * This allows any segment created by the backend to continue exist
		 */
		dsa_pin_mapping(area);

		/* 
         * TODO remove me
         * stand-alone backend or backend under postmaster. */
		/*if (is_gc_backend)
		{

			abtvs_pending_deletable_capacity = ABT_MAX_PENDING_DELETABLE_GC / 2;
		}
		else
		{

			abtvs_pending_deletable_capacity = ABT_MAX_PENDING_DELETABLE_USER_BACKEND;
		}
		abtvs_num_pending_deletable = 0;
		abtvs_num_pending_deletable_2 = 0;
		abtvs_pending_deletable = (abtvs_ptr *) palloc(sizeof(abtvs_ptr) *
			((is_gc_backend) ? 
			(abtvs_pending_deletable_capacity * 2) :
			abtvs_pending_deletable_capacity));
		abtvs_pending_deletable_2 =
			abtvs_pending_deletable + abtvs_pending_deletable_capacity; */
	}

	if (is_gc_backend)
	{
		/* 
		 * We'll allocate the initial new chunks for others if they are
		 * not set up yet. These functions won't do anything if there's
		 * already a chunk allocated.
		 */
		shared_slab_allocate_new_chunk(abtvs_shared_slab);
		shared_slab_allocate_new_chunk(abtvs_shared_slab_for_htab);
	}

	/*
	 * We must do this before dsm backend shutdown since we might still
	 * try to free a few dsa pointers.
	 */
	before_shmem_exit(_abt_version_store_cleanup, PointerGetDatum(dsa_place));

	MemoryContextSwitchTo(old_context);
}

static void
_abt_version_store_cleanup(int code, Datum arg)
{
	void *dsa_place;
	dsa_area *area;
	/* bool is_postmaster = IsPostmasterEnvironment && !IsUnderPostmaster; */

	/* TODO clean up shared_slab */

	dsa_place = DatumGetPointer(arg);
	/* htab_data = (char *) dsa_place
		+ TYPEALIGN(64, abtree_version_store_initial_mem * 1024); */

	/* 
     * Clean up the hash table locally. If this is the last reference (i.e.,
     * postmaster), nothing in the shared memory will be released here.
     *
     * This must be done before we call shared_slab_reclaim, as there might be
     * one additional cached new node to be freed.
     */
	if (abtvs_htab)
		lockfree_htab_destroy(abtvs_htab);
	
	if (!IsPostmasterEnvironment ||
			(IsUnderPostmaster && MyBackendType != B_BG_WORKER))
	{
		/* backends, deallocate pending deletable nodes */
		Assert(dsa_place);
        shared_slab_reclaim(abtvs_shared_slab, true);
        shared_slab_reclaim(abtvs_shared_slab_for_htab, true);
	}
	/*
	 * gc backend should just skip reclaiming nodes here as it only reaches
	 * here during a shutdown.
	 */

	if (!IsUnderPostmaster)
	{
		if (IsPostmasterEnvironment)
		{
			/* postmaster */
			Assert(!abtvs_htab);
			area = dsa_attach_in_place(dsa_place, NULL);
			/* TODO clean up the hash table */
			/* abtvs_htab = lockfree_htab_create(htab_data,
											   area,
											   false); */

			/* lockfree_htab_unpin(abtvs_htab); */
			dsa_unpin(area);
		}
		/* else: stand-alone backend */
	} /* else backends under postmaster */
	
	dsa_release_in_place(dsa_place);
}

/********************************************************************
 * Section 2: low level routines for version lookups and updates
 ********************************************************************/

/*
 * Find the chain head with the matching key. If the chain is empty,
 * it returns ABTVS_NULL. Otherwise, it returns a reference to a valid
 * node in the shared memory.
 *
 * Note that, the node that the returned ptr points to can still become pending
 * deletion after it's returned and its next pointers may become dangling
 * references. Hence, the caller needs to check if the xmin if safe before
 * traversing through the chain.
 *
 * The return value is never marked.
 */
static abtvs_ptr
_abtvs_find_chain_head(abtvs_key key)
{
	uint64				*retval;
	abtvs_ptr_atomic	*head_ptr;
	abtvs_ptr			head;

	retval = lockfree_htab_find_u64(abtvs_htab, key, HASH_FIND, NULL);
	/* entry doesn't exist */
	if (!retval)
		return ABTVS_NULL;

	head_ptr = (abtvs_ptr_atomic *) retval;
	head = abtvs_ptr_atomic_read(head_ptr);
	head = abtvs_ptr_get_unmarked(head);

	return head;
}

/*
 * Different from _abtvs_find_chain_head(), this may set *p_head to a marked
 * value, which indicates a GC thread is examining the chain.
 */
static abtvs_ptr_atomic *
_abtvs_find_chain_head_for_insertion(abtvs_key key,
									 abtvs_ptr *p_head)
{
	abtvs_ptr_atomic	*head_ptr;
	abtvs_ptr			head;

try_again:
	head_ptr = (abtvs_ptr_atomic *)
		lockfree_htab_find_u64(abtvs_htab, key, HASH_ENTER, NULL);
	Assert(head_ptr);  /* the entry must exist */

	head = abtvs_ptr_atomic_read(head_ptr);

	/*
	 * Someone may have deleted deleted the entire chain and marked it for
	 * deletion at the mean time. We'll need a new entry for insertion.
	 */
	if (unlikely(head == abtvs_deleted_chain_head))
	{
		goto try_again;
	}

	/*
	 * It's ok to use head instead of unmarked_head here, because a marked
	 * ABTVS_NULL is abtvs_deleted_chain_head. See the above if statement.
	 */
	if (p_head)
		*p_head = head;
	return head_ptr;
}

static ABTVSNode *
_abtvs_get_node_if_safe(abtvs_ptr node, TransactionId oldestXmin)
{
	ABTVSNode *node_;

	Assert(node != abtvs_deleted_chain_head);
	if (node == ABTVS_NULL) return NULL;

	node_ = abtvs_rawptr(node);
	if (TransactionIdPrecedes(node_->xmin, oldestXmin))
	{
		node_ = NULL;
	}

	return node_;
}

/*
 * This is only safe if the page containing the downlink denoted by the key
 * is write-locked.
 */
static void
_abtvs_replace_chain_head(abtvs_key key, abtvs_ptr new_head)
{
	abtvs_ptr_atomic	*head_ptr;
	abtvs_ptr			head;
	SpinDelayStatus		delay_status;

	init_local_spin_delay(&delay_status);

retry_find_the_head:
	if (new_head == ABTVS_NULL)
	{
		head_ptr = (abtvs_ptr_atomic *)
			lockfree_htab_find_u64(abtvs_htab, key, HASH_FIND, NULL);

		/* the key is not there, we're all good. */
		if (!head_ptr)
			return ;
	}
	else
	{
		head_ptr = (abtvs_ptr_atomic *)
			lockfree_htab_find_u64(abtvs_htab, key, HASH_ENTER, NULL);
	}

	/* read the chain head */
	head = abtvs_ptr_atomic_read(head_ptr);
	for (;;)
	{
		/* concurrent GC removed this entry */
		if (unlikely(head == abtvs_deleted_chain_head))
		{
			/* Someone removed the entire chain for us. That's fine. */
			if (new_head == ABTVS_NULL)
				return ;
			goto retry_find_the_head;
		}

		if (unlikely(abtvs_ptr_is_marked(head)))
		{
			/* spin a bit to see what happens */
			perform_spin_delay(&delay_status);
			head = abtvs_ptr_atomic_read(head_ptr);
		}
		else
		{
			/* we're good with the head, try to install the new value. */
			Assert(!abtvs_ptr_is_marked(head));
			if (abtvs_ptr_atomic_compare_exchange(head_ptr, &head, new_head))
			{
				break;
			}

			/*
			 * someone managed to change the value, it must he the GC process.
			 */
		}
	}

	Assert(!abtvs_ptr_is_marked(head));
	_abtvs_delete_chain(head);
}

static void
_abtvs_delete_chain(abtvs_ptr head)
{
    abtvs_ptr node_to_delete;
    ABTVSNode *node_;

	if (head == ABTVS_NULL || head == abtvs_deleted_chain_head)
		return ;
    
    
    do {
        node_to_delete = head;
        node_ = abtvs_rawptr(node_to_delete);
        head = abtvs_ptr_atomic_read(&node_->next);
        abtvs_free(node_to_delete);
    } while (head != ABTVS_NULL);
}

/*static void
_abtvs_reclaim_pending_deletables(void)
{
	abtvs_ptr	node,
				next_node;
	abtvs_ptr	*t;
	int			i;
	ABTVSNode	*node_;

	for (i = 0; i < abtvs_num_pending_deletable_2; ++i)
	{
		node = abtvs_pending_deletable_2[i];
		while (node != ABTVS_NULL)
		{
			node_ = abtvs_rawptr(node);
			next_node = abtvs_ptr_atomic_read(&node_->next);
			abtvs_free(node);
			node = next_node;
			++abtvs_num_deletable_reclaimed;
		}
	}
		
	t = abtvs_pending_deletable_2;
	abtvs_pending_deletable_2 = abtvs_pending_deletable;
	abtvs_pending_deletable = t;
	abtvs_num_pending_deletable_2 = abtvs_num_pending_deletable;
	abtvs_num_pending_deletable = 0;
} */

static abtvs_ptr
_abtvs_find_or_create_node(abtvs_key key, TransactionId xmin,
						   ABTCachedAggInfo agg_info)
{
	abtvs_ptr_atomic	*head_ptr,
						*cur_ptr;
	abtvs_ptr			old_head,
						cur,
						new_node;
	ABTVSNode			*cur_node_;
	ABTVSNode			*new_node_;

	if (abtvs_cached_new_node == ABTVS_NULL)
	{
		abtvs_cached_new_node = abtvs_alloc_version_node(agg_info);
		new_node_ = abtvs_rawptr(abtvs_cached_new_node);
		new_node_->delta_val.datum = 0;
	}
	else
	{
		new_node_ = abtvs_rawptr(abtvs_cached_new_node);
	}
	new_node_->xmin = xmin;

restart_from_head:
	/*
	 * This assertion still holds if we jumped to restart_from_head from a
	 * failed CAS.
	 */
	head_ptr = _abtvs_find_chain_head_for_insertion(key, &old_head);
	cur_ptr = head_ptr;
	cur = abtvs_ptr_get_unmarked(old_head);

search_for_new_insertion_point:
	while (cur != ABTVS_NULL)
	{
		cur_node_ = abtvs_rawptr(cur);

		/* found existing node */
		if (cur_node_->xmin == xmin)
		{
			return cur;
		}

		/* found an insertion point */
		if (NormalTransactionIdPrecedes(cur_node_->xmin, xmin))
		{
			break;
		}

		/* cur_node_->xmin > xmin: cur is a safe location to restart search */
		cur_ptr = &cur_node_->next;
		cur = abtvs_ptr_atomic_read(cur_ptr);
	}

	abtvs_ptr_atomic_write(&new_node_->next, cur);
	/*
	 * recovers the old value of head ptr if cur_ptr is the head, as the value
	 * might be marked
	 */
	if (cur_ptr == head_ptr && abtvs_ptr_is_marked(old_head))
	{
		/*
		 * keep the mark if the old head was marked and we are inserting
		 * to the chain head
		 */
		cur = old_head;
		new_node = abtvs_ptr_mark(abtvs_cached_new_node);
	}
	else
	{
		/*
		 * keep it unmarked.
		 */
		new_node = abtvs_cached_new_node;
	}
	if (!abtvs_ptr_atomic_compare_exchange(cur_ptr, &cur, new_node))
	{
		/*
		 * A concurrent deleter deleted the entry from the hash table. cur_ptr
		 * must be the head pointer.
		 */
		if (cur == abtvs_deleted_chain_head)
			goto restart_from_head;

		/*
		 * This is something that a concurrent backend inserted, so it
		 * cannot have a xmin < global data xmin and thus is not subject
		 * to GC. 
		 */
		goto search_for_new_insertion_point;
	}

	/* new node inserted */
	cur = abtvs_cached_new_node;
	abtvs_cached_new_node = ABTVS_NULL;
	return cur;
}

/*
 * Subtract the aggregation values of any xid >= xmax or in the xip array.
 *
 * The snapshot passed here contains a xip array sorted in descending order.
 */
static uint64
_abtvs_exclude_agg_in_snapshot(abtvs_key key, uint64 agg, ABTSnapshot snapshot,
							   ABTCachedAggInfo agg_info)
{
	uint32		i = 0;
	abtvs_ptr	cur;
	ABTVSNode	*cur_node_;
	uint64		rop;

	if (!snapshot) return agg;

	cur = _abtvs_find_chain_head(key);
	while (cur != ABTVS_NULL)
	{
		cur_node_ = abtvs_rawptr(cur);

		/* cur_node_->xmin < snapshot->xmin: we're done */
		if (NormalTransactionIdPrecedes(cur_node_->xmin, snapshot->xmin))
		{
			break;
		}

		/* cur_node_->xmin < snapshot->xmax: consult the xip array */
		if (NormalTransactionIdPrecedes(cur_node_->xmin, snapshot->xmax))
		{
			/* find the first xid in xip that is <= cur_node_->xmin */
			while (i < snapshot->xcnt &&
				NormalTransactionIdPrecedes(cur_node_->xmin, snapshot->xip[i]))
			{
				++i;
			}

			if (i < snapshot->xcnt && cur_node_->xmin == snapshot->xip[i])
			{
				/* concurrent version: subtract from the agg  */
				++i;
			}
			else
			{
				/* committed/aborted version: keep it in agg */
				cur = abtvs_ptr_atomic_read(&cur_node_->next);
				Assert(cur != abtvs_deleted_chain_head);
				continue;
			}
		}
		/*
		 * else, cur_node_->xmin >= xnapshot->xmax: subtract from the agg
		 */

		/* We must subtract cur_node_->delta_val from the agg here. */
		if (agg_info->agg_typlen == 8)
		{
			rop = pg_atomic_read_u64(&cur_node_->delta_val.atomic_u64);
		}
		else if (agg_info->agg_typlen == 4)
		{
			rop = pg_atomic_read_u32(&cur_node_->delta_val.atomic_u32);
		}
		else
		{
			rop = (uint16) pg_atomic_read_u32(&cur_node_->delta_val.atomic_u32);
		}

		Assert(agg >= rop);
		agg -= rop;
		cur = abtvs_ptr_atomic_read(&cur_node_->next);
	}

	return agg;
}

/********************************************************************
 * Section 3: high level routines for version lookups and updates
 ********************************************************************/

StaticAssertDecl(sizeof(abtvs_ptr) <= sizeof(uint64),
				 "abtvs_ptr should fit in a uint64");
StaticAssertDecl(sizeof(IndexTuple *) <= sizeof(uint64),
				 "IndexTuple * should fit in a uint64");


/*
 * We can use NormalTransactionIdPrecedes() for comparison because we
 * guanrantee that anything that's inserted into the min heap, is >=
 * RecentGlobalDataXmin, which must be a normal xid.
 */
static void
_abtvs_push_xmin_max_heap(uint64 *max_heap,
						  int i,
						  int n,
						  TransactionId (*get_xmin)(uint64, void *),
						  void *context)
{
	int				j;
	TransactionId	parent_xmin,
					child_xmin,
					child2_xmin;

	parent_xmin = get_xmin(max_heap[i], context);
	while (i <= (n >> 1))
	{
		j = i << 1;
		child_xmin = get_xmin(max_heap[j], context);
		if (j < n)
		{
			child2_xmin = get_xmin(max_heap[j + 1], context);
			if (NormalTransactionIdPrecedes(child_xmin, child2_xmin))
			{
				++j;
				child_xmin = child2_xmin;
			}
		}

		if (NormalTransactionIdPrecedes(parent_xmin, child_xmin))
		{
			swap(max_heap[i], max_heap[j]);
			i = j;
		}
		else{
			break;
		}
	}
}

static void
_abtvs_make_xmin_max_heap(uint64 *max_heap,
						  int n,
						  TransactionId (*get_xmin)(uint64, void *),
						  void *context)
{
	int				i;

	for (i = n >> 1; i >= 1; --i)
		_abtvs_push_xmin_max_heap(max_heap, i, n, get_xmin, context);
}

static TransactionId
_abtvs_node_get_xmin(uint64 ptrval, void *context)
{
	abtvs_ptr	node = (abtvs_ptr) ptrval;
	ABTVSNode	*node_ = abtvs_rawptr(node);
	return node_->xmin;
}

static TransactionId
_abtvs_itup_get_xmin(uint64 ptrval, void *context)
{
	ABTVSItupGetXminItemPtr	*p_itemptr = (ABTVSItupGetXminItemPtr *) &ptrval;
	Page					page = (Page) context;
	IndexTuple				itup;

	itup = (IndexTuple) PageGetItem(page, &p_itemptr->itemid);
	if (p_itemptr->postingoff >= 0)
	{
		return ABTreePostingTupleGetXminPtr(itup)[p_itemptr->postingoff];
	}
	return *ABTreeNonPivotTupleGetXminPtr(itup);
}

/*
 * Compute the on-page aggregate value for this page and create a new version
 * chain for this page. The new version chain is returned to the caller as
 * an opaque pointer in *opaque_p_chain. It may not be installed until the
 * parent page is write-locked. If we were to install it here, a vacuum thread
 * may be traversing the old version chain we just replaced and reclaim some or
 * all of the nodes, while we are also trying to do the same. That will lead to
 * double free.
 *
 * The buffer ``buf'' should be write-locked upon entry and we do not drop the
 * lock upon finish. This guarantees that no one is able to insert into and/or
 * remove anything from the version chain of downlink on this page.
 */
Datum
_abt_compute_page_aggregate_and_version_chain(Relation rel,
											  Buffer buf,
											  uint64 *opaque_p_chain,
											  ABTCachedAggInfo agg_info)
{
	Oid					index_id;
	Datum				agg;
	bool				is_leaf;
	Page				page;
	ABTPageOpaque		opaque;
	ItemId				itemid;
	IndexTuple			itup;
	OffsetNumber		off,
						minoff,
						maxoff;
	BlockNumber			blkno;
	uint64				*max_heap;
	int					n,
						i;
	int					postingn;
	ABTVSItupGetXminItemPtr itemptr;
	ABTVSItupGetXminItemPtr	*p_itemptr;
	TransactionId		*xmin_ptr;
	TransactionId		xmin;
	Pointer				agg_ptr = NULL;
	Datum				item_agg;
	abtvs_ptr			head;
	abtvs_ptr_atomic	*tail_next_ptr;
	ABTVSNode			*tail_node_;
	TransactionId		tail_xmin;
	abtvs_ptr			node;
	ABTVSNode			*node_;
	abtvs_key			key;

	index_id = RelationGetRelid(rel);
	page = BufferGetPage(buf);
	opaque = (ABTPageOpaque) PageGetSpecialPointer(page);
	minoff = ABT_P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);
	is_leaf = ABT_P_ISLEAF(opaque);

	Assert(minoff <= maxoff);

	if (is_leaf)
	{
		/*
		 * compute the true number of leaf items after considering the
		 * posting tuples
		 */
		n = maxoff - minoff + 1;
		for (off = minoff; off <= maxoff; off = OffsetNumberNext(off))
		{
			itemid = PageGetItemId(page, off);
			itup = (IndexTuple) PageGetItem(page, itemid);

			if (ABTreeTupleIsPosting(itup))
				n += ABTreeTupleGetNPosting(itup) - 1;
		}
	}
	else
	{
		n = maxoff - minoff + 1;
	}

	/* the new chain is simply empty, initialize the agg with a zero */
	if (agg_info->int2_to_agg_type_fn_initialized)
	{
		agg = FunctionCall1(&agg_info->int2_to_agg_type_fn,
							Int16GetDatum(0));
	}
	else
	{
		agg = Int16GetDatum(0);
	}


	max_heap = (uint64 *) palloc(sizeof(uint64) * n);
	max_heap -= 1; /* index starts from 1 */
	i = 1;

	for (off = minoff; off <= maxoff; off = OffsetNumberNext(off))
	{
		itemid = PageGetItemId(page, off);
		itup = (IndexTuple) PageGetItem(page, itemid);

		/*
		 * Filter out all that is older than RecentGlobalDataXmin or is invalid
		 * and put the remaining into the heap. Since invalid xid precedes any
		 * normal transaction id, we can just use TransactionIdPrecedes()
		 * below.
		 *
		 * RecentGlobalDataXmin is one that might be older than actually
		 * needed but it's still a safe cutoff value to determine whether a
		 * version is needed or not. While we can get a more recent one
		 * by calling GetOldestXmin(), that needs to grab a shared lock
		 * on proc array and thus is too expensive for here.
		 *
		 * The page aggregate should always include everything that is still
		 * there, except for those with invalid xmin (meaning the insertion is
		 * still in flight or was in flight before a crash), even if the entire
		 * version chain is invisible.  There might still be live tuples whose
		 * xmin is simply too old, which should count towards the page
		 * aggregate and no one wants to exclude them from the page aggregate.
		 * Aggregates of the dead tuples in this subtree will eventually be
		 * subtracted when they are vacuumed.
		 *
		 */
		if (is_leaf)
		{
			if (ABTreeTupleIsPosting(itup))
			{
				itemptr.itemid = *itemid;
				postingn = ABTreeTupleGetNPosting(itup);
				xmin_ptr = ABTreePostingTupleGetXminPtr(itup);
				if (agg_info->leaf_has_agg)
					agg_ptr = ABTreePostingTupleGetAggPtr(itup, agg_info);
				for (itemptr.postingoff = 0;
					 itemptr.postingoff < postingn;
					 ++itemptr.postingoff)
				{
					if (!TransactionIdIsValid(xmin_ptr[itemptr.postingoff]))
						continue;

					if (agg_info->leaf_has_agg)
					{
						_abt_accum_value(agg_info, &agg,
							fetch_att(agg_ptr +
								agg_info->agg_stride * itemptr.postingoff,
								agg_info->agg_byval,
								agg_info->agg_typlen),
							/* is_first = */false);
					}
					else
					{
						_abt_accum_value(agg_info, &agg, agg_info->agg_map_val,
									 /* is_first = */false);
					}

					if (TransactionIdPrecedes(xmin_ptr[itemptr.postingoff],
											  RecentGlobalDataXmin))
						continue;

					max_heap[i++] = *(uint64 *) &itemptr;
				}
			}
			else
			{
				Assert(!ABTreeTupleIsPivot(itup));

				xmin_ptr = ABTreeNonPivotTupleGetXminPtr(itup);
				if (!TransactionIdIsValid(*xmin_ptr))
					continue;

				if (agg_info->leaf_has_agg)
				{
					agg_ptr = ABTreeNonPivotTupleGetAggPtr(itup, agg_info);
					_abt_accum_value(agg_info, &agg,
									 fetch_att(agg_ptr,
											   agg_info->agg_byval,
											   agg_info->agg_typlen),
									 /* is_first = */false);
				}
				else
				{
					_abt_accum_value(agg_info, &agg, agg_info->agg_map_val,
									 /* is_first = */false);
				}

				if (TransactionIdPrecedes(*xmin_ptr, RecentGlobalDataXmin))
					continue;

				itemptr.itemid = *itemid;
				itemptr.postingoff = -1;
				max_heap[i++] = *(uint64 *) &itemptr;
			}
		}
		else
		{
			Assert(ABTreeTupleIsPivot(itup));
			blkno = ABTreeTupleGetDownLink(itup);
			key = ABTVS_KEY(index_id, blkno);
			node = _abtvs_find_chain_head(key);
			node_ = _abtvs_get_node_if_safe(node, RecentGlobalDataXmin);
			if (node_ != NULL)
			{
				/* this might be a live version visible to someone */
				max_heap[i++] = (uint64) node;
			}

			_abt_accum_index_tuple_value(agg_info, &agg, itup,
										 /* is_leaf = */false,
										 /* is_first = */false);
		}
	}
	Assert(i >= 1 && i - 1 <= n);
	n = i - 1;

	head = ABTVS_NULL;
	if (n != 0)
	{

		/*
		 * Build the new version chain of this page. To do that, we start from
		 * the smallest xmin and add more nodes as we find larger xmin.
		 */
		if (is_leaf)
		{
			_abtvs_make_xmin_max_heap(max_heap, n, _abtvs_itup_get_xmin, page);
		}
		else
		{
			_abtvs_make_xmin_max_heap(max_heap, n, _abtvs_node_get_xmin, NULL);
		}

		head = ABTVS_NULL;
		tail_next_ptr = (abtvs_ptr_atomic *)&head;
		tail_node_ = NULL;
		tail_xmin = InvalidTransactionId;
		/*
		 * We don't need anything that equals or is older than
		 * FrozenTransactionId in the version chain. Hence, this is
		 * essentially a -inf value for our purpose.
		 *
		 * The exit of the loop is at the end.
		 */
		for (; /* n > 0 */ ;)
		{
			/*
			 * set up the xmin_ptr and item_agg.
			 *
			 * If page is a leaf page, xmin_ptr (and item_agg when it's passed
			 * by ref) points into the index tuple.
			 *
			 * If page is not a leaf page, then xmin_ptr and (item_agg when
			 * it's passed by ref) points into an ABTVSNode. Note that, it is
			 * safe to use these pointers without using atomic reads because we
			 * have write-locked the buffer page and thus no one can update
			 * these fields.
			 *
			 */
			if (is_leaf)
			{
				p_itemptr = (ABTVSItupGetXminItemPtr *) &max_heap[1];
				itup = (IndexTuple) PageGetItem(page, &p_itemptr->itemid);
				if (p_itemptr->postingoff >= 0)
				{
					xmin_ptr = ABTreePostingTupleGetXminPtr(itup) +
						p_itemptr->postingoff;
					if (agg_info->leaf_has_agg)
					{
						agg_ptr = ABTreePostingTupleGetAggPtr(itup, agg_info) +
							agg_info->agg_stride * p_itemptr->postingoff;
						item_agg = fetch_att(agg_ptr, agg_info->agg_byval,
											 agg_info->agg_typlen);
					}
					else
					{
						item_agg = agg_info->agg_map_val;
					}
				}
				else
				{
					xmin_ptr = ABTreeNonPivotTupleGetXminPtr(itup);
					if (agg_info->leaf_has_agg)
					{
						agg_ptr = ABTreeNonPivotTupleGetAggPtr(itup, agg_info);
						item_agg = fetch_att(agg_ptr, agg_info->agg_byval,
											 agg_info->agg_typlen);
					}
					else
					{
						item_agg = agg_info->agg_map_val;
					}
				}
			}
			else
			{
				node = (abtvs_ptr) max_heap[1];
				node_ = abtvs_rawptr(node);
				xmin_ptr = &node_->xmin;
				if (agg_info->agg_byval)
				{
					item_agg = node_->delta_val.datum;
				}
				else
				{
					item_agg = PointerGetDatum(node_->delta_val.buf);
				}
			}

			/*
			 * copy xmin value because the node_ variable is going to be
			 * overwritten below.
			 */
			xmin = *xmin_ptr;

			/* Add item_agg to an appropriate version node. */
			if (TransactionIdEquals(xmin, tail_xmin))
			{
				/* same version as head, so add to head */
				if (agg_info->agg_byval)
				{
					tail_node_->delta_val.datum = FunctionCall2(
						&agg_info->agg_add_fn,
						tail_node_->delta_val.datum,
						item_agg);
				}
				else
				{
					item_agg = FunctionCall2(
						&agg_info->agg_add_fn,
						PointerGetDatum(tail_node_->delta_val.buf),
						item_agg);
					memcpy(tail_node_->delta_val.buf,
						   DatumGetPointer(item_agg),
						   agg_info->agg_typlen);
					pfree(DatumGetPointer(item_agg));
				}
			}
			else
			{
				/* new version, add to a new node */
				Assert(!TransactionIdIsValid(tail_xmin) ||
						NormalTransactionIdPrecedes(xmin, tail_xmin));
				node = abtvs_alloc_version_node(agg_info);
				node_ = abtvs_rawptr(node);

				node_->xmin = xmin;
				abtvs_ptr_atomic_write(tail_next_ptr, node);
				tail_node_ = node_;
				tail_xmin = xmin;
				tail_next_ptr = &tail_node_->next;

				/* initialize the delta_val */
				if (agg_info->agg_byval)
				{
					node_->delta_val.datum = item_agg;
				}
				else
				{
					memcpy(node_->delta_val.buf, DatumGetPointer(item_agg),
						   agg_info->agg_typlen);
				}
			}

			/* pop the min heap */
			if (is_leaf)
			{
				if (--n == 0)
				{
					/* nothing left in the heap, we're done. */
					break;
				}
				max_heap[1] = max_heap[n + 1];
				_abtvs_push_xmin_max_heap(max_heap, 1, n, _abtvs_itup_get_xmin,
										  page);
			}
			else
			{
				node = (abtvs_ptr) max_heap[1];
				node_ = abtvs_rawptr(node);

				node = abtvs_ptr_atomic_read(&node_->next);
				node_ = _abtvs_get_node_if_safe(node, RecentGlobalDataXmin);
				if (node_ == NULL)
				{
					/* end of chain, pop the heap top */
					if (--n == 0)
						break;
					max_heap[1] = max_heap[n + 1];
				}
				else
				{
					/*
					 * There's still something in the chain, replace the heap
					 * top with the next node.
					 */
					max_heap[1] = node;
				}
				_abtvs_push_xmin_max_heap(max_heap, 1, n, _abtvs_node_get_xmin,
										  NULL);
			}
		}
		abtvs_ptr_atomic_write(tail_next_ptr, ABTVS_NULL);
	}

	/* return the chain to the caller */
	*opaque_p_chain = (uint64) head;

	/* be tidy */
	pfree(max_heap + 1);

	return agg;
}

/*
 * Substitutes the new version chain for or removes the old version chain for
 * the page blkno in the index. This may only be called when the page
 * containing the index tuple with a downlink of blkno is write-locked. See
 * comments of _abt_compute_page_aggregate_and_version_chain() for the
 * rationale.
 */
void
_abt_install_version_chain(Relation rel, BlockNumber blkno, uint64 opaque_chain)
{
	Oid			index_id = RelationGetRelid(rel);
	abtvs_key	key = ABTVS_KEY(index_id, blkno);
	abtvs_ptr	head = (abtvs_ptr) opaque_chain;

	/*
	 * don't delete the entry for this key if it's in the root as there might
	 * be significantly higher contention. Vacuum will eventually remove the
	 * entries if no one is modifying the tree.
	 */
	_abtvs_replace_chain_head(key, head);
}

/*
 * Add dval to the aggregation at version xmin of block blkno in the index rel.
 *
 * Must be called with a read lock on the page containing the index tuple
 * with a downlink of blkno.
 */
void
_abt_add_to_version(Relation rel, BlockNumber blkno, TransactionId xmin,
					Datum dval, ABTCachedAggInfo agg_info)
{
	Oid			index_id = RelationGetRelid(rel);
	abtvs_key	key = ABTVS_KEY(index_id, blkno);
	abtvs_ptr	node;
	ABTVSNode	*node_;

	node = _abtvs_find_or_create_node(key, xmin, agg_info);
	node_ = abtvs_rawptr(node);

	/*
	 * XXX setting and accessing a union through differnt fields is not
	 * strictly C99 compliant but should work on any known compilers.
	 */
	if (agg_info->agg_typlen == 8)
	{
		int64	rop = DatumGetInt64(dval);
		pg_atomic_fetch_add_u64(&node_->delta_val.atomic_u64, rop);
	}
	else if (agg_info->agg_typlen == 4)
	{
		int32	rop = DatumGetInt32(dval);
		pg_atomic_fetch_add_u32(&node_->delta_val.atomic_u32, rop);
	}
	else
	{
		int16	rop = DatumGetInt16(dval);
		pg_atomic_fetch_add_u32(&node_->delta_val.atomic_u32, rop);
	}
}

static int
_abt_compare_xid(const void *a, const void *b)
{
	TransactionId	xid_a = *(TransactionId *) a;
	TransactionId	xid_b = *(TransactionId *) b;

	/* sort in descending order */
	if (NormalTransactionIdPrecedes(xid_a, xid_b)) return 1;
	if (NormalTransactionIdPrecedes(xid_b, xid_a)) return -1;
	return 0;
}

/*
 * Take all the top level xids and sort them in decreasing order.
 */
ABTSnapshot
_abt_prepare_snapshot_for_sampling(Snapshot snapshot)
{
	ABTSnapshot		abt_snapshot;
	uint32			xcnt;

	if (!snapshot || snapshot->snapshot_type != SNAPSHOT_MVCC)
	{
		/* for any other snapshot, we just use the overestimated agg */
		return NULL;
	}

	/* see XidInMVCCSnapshot() in utils/time/snapmgr.c. */
	if (!snapshot->takenDuringRecovery)
	{
		xcnt = snapshot->xcnt;
	}
	else
	{
		xcnt = snapshot->subxcnt;
	}

	/* Note abt_snapshot may be 4 bytes smaller than sizeof(ABTSnapshotData) */
	abt_snapshot = palloc(offsetof(ABTSnapshotData, xip) +
						  sizeof(TransactionId) * xcnt);

	abt_snapshot->xmin = snapshot->xmin;
	abt_snapshot->xmax = snapshot->xmax;
	abt_snapshot->xcnt = xcnt;

	if (!snapshot->takenDuringRecovery)
		memcpy(abt_snapshot->xip, snapshot->xip, sizeof(TransactionId) * xcnt);
	else
		memcpy(abt_snapshot->xip, snapshot->subxip,
			   sizeof(TransactionId) * xcnt);

	/* sort the xids in decreasing order */
	qsort(abt_snapshot->xip, xcnt, sizeof(TransactionId), _abt_compare_xid);

	/* for our purpose, xmin is the same as the smallest xid in the snapshot. */
	if (xcnt != 0)
		abt_snapshot->xmin = abt_snapshot->xip[xcnt - 1];
	else
		abt_snapshot->xmin = abt_snapshot->xmax;
	Assert(!NormalTransactionIdPrecedes(abt_snapshot->xmax, abt_snapshot->xmin));

#ifdef USE_ASSERT_CHECKING
	/*{
		uint32 i;
		for (i = 1; i < xcnt; ++i)
		{
			Assert(NormalTransactionIdPrecedes(abt_snapshot->xip[i - 1],
											   abt_snapshot->xip[i]);
		}
	} */
#endif

	return abt_snapshot;
}

/*
 * Returns whether this xid is in the snapshot (invisible to this snapshot).
 */
bool
_abt_xid_in_snapshot(TransactionId xid, ABTSnapshot snapshot)
{
	/* empty snapshot indicates everything is visible */
	if (!snapshot) return false;

	/* invalid transaction id is not visible to anyone except for the inserter*/
	if (!TransactionIdIsValid(xid))
		return true;

	/* xid >= xmax, not visible */
	if (!TransactionIdPrecedes(xid, snapshot->xmax))
		return true;

	/* xid < xmin, visible */
	if (TransactionIdPrecedes(xid, snapshot->xmin))
		return false;

	Assert(snapshot->xcnt != 0);
	Assert(TransactionIdIsNormal(xid));

	/* found xid in snapshot->xip, not visiblle */
	if (bsearch(&xid,
				snapshot->xip,
				snapshot->xcnt,
				sizeof(TransactionId),
				_abt_compare_xid))
		return true;

	/* not found, it's visible */
	return false;
}

/* XXX we only allow IN2, INT4 and INT8 aggs right now. So we just return them
 * all as uint64 for _abt_sample() purpose. */
uint64
_abt_read_agg(Relation rel, IndexTuple itup, ABTSnapshot snapshot,
			  ABTCachedAggInfo agg_info)
{
	Oid			index_id = RelationGetRelid(rel);
	BlockNumber blkno;
	abtvs_key	key;
	Pointer		agg_ptr;
	uint64		agg;

	Assert(ABTreeTupleIsPivot(itup));
	blkno = ABTreeTupleGetDownLink(itup);
	key = ABTVS_KEY(index_id, blkno);

	agg_ptr = ABTreePivotTupleGetAggPtr(itup, agg_info);
	agg = _abt_atomic_read_agg_ptr(agg_ptr, agg_info);
	agg = _abtvs_exclude_agg_in_snapshot(key, agg, snapshot, agg_info);
	return agg;
}

/********************************************************************
 * Section 3: garbage collector
 ********************************************************************/
void
ABTreeRegisterGarbageCollector(void)
{
	BackgroundWorker	bgw;

	memset(&bgw, 0, sizeof(bgw));
	snprintf(bgw.bgw_name, BGW_MAXLEN, ABTVS_GC_BGW_NAME);
	snprintf(bgw.bgw_type, BGW_MAXLEN, ABTVS_GC_BGW_TYPE);
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
                    BGWORKER_BACKEND_DATABASE_CONNECTION;
	/* start early so that we can set up the initial chunk of shared memory */
	bgw.bgw_start_time = BgWorkerStart_ConsistentState;
	bgw.bgw_restart_time = 10;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ABTreeGarbageCollectorMain");
	/* main_arg is currently ignored. */
	bgw.bgw_main_arg = Int32GetDatum(0);
    bgw.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&bgw);
}

void
ABTreeGarbageCollectorMain(Datum main_arg)
{
	TransactionId				oldestXmin;

	/*
	 * Set up signal handlers. We only need to respond to shutdown requests.
	 */
	pqsignal(SIGHUP, SIG_IGN); /* no config reload allowed */
	pqsignal(SIGINT, SIG_IGN); /* nothing to cancel */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest); /* */
	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts();
	/* other signals (SIGPIPE, SIGUSR1, SIGCHLD and SIGUSR2) are standard */
	BackgroundWorkerUnblockSignals();

	/* We need to get a backend ID from InitPostgres(). */
    BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	_abtvs_init_maybe_deletable();

	SetProcessingMode(NormalProcessing);
	elog(LOG , "abtree version store garbage collector started");
	ResetLatch(MyLatch);

	while (!ShutdownRequestPending)
	{
		if (abtree_version_store_garbage_collector_delay != 0)\
		{
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 abtree_version_store_garbage_collector_delay,
							 WAIT_EVENT_PG_SLEEP);
			ResetLatch(MyLatch);
		}

		/* Shutdown if we are asked to. */
		if (ShutdownRequestPending)
			break;

		/*
		 * Get a new oldest xmin. This may not be as aggressive as heap vacuum
		 * if old snapshot feature is enabled. But we don't have a choice
		 * because we put everything into one hash table.
		 *
		 * Also we don't care about frozen limit because oldestXmin must >=
		 * frozen limit.
		 */
		oldestXmin = GetOldestXmin(NULL, PROCARRAY_FLAGS_VACUUM);
		_abtvs_scan_and_recycle_old_versions(oldestXmin);
	}

	/* shuting down */
}

static void
_abtvs_scan_and_recycle_old_versions(TransactionId oldestXmin)
{
	lockfree_htab_iter_status	iter_status;
	abtvs_ptr_atomic			*head_ptr,
								*cur_ptr;
	abtvs_ptr					head,
								cur,
								deletable;
	ABTVSNode					*cur_node_;
	
	epoch_acquire();
	_abt_reset_gc_counters();
	lockfree_htab_iter_init(abtvs_htab, &iter_status);
	while ((head_ptr = (abtvs_ptr_atomic *)
				lockfree_htab_iter_next(abtvs_htab, &iter_status)))
	{

		/* If we are asked to shutdown, do it quickly. */
		if (ShutdownRequestPending)
			break;

		head = abtvs_ptr_atomic_read(head_ptr);
		Assert(!abtvs_ptr_is_marked(head));

		epoch_maybe_refresh();

		do {
			if (head == ABTVS_NULL)
			{
				break;
			}
		} while (!abtvs_ptr_atomic_compare_exchange(head_ptr, &head,
													abtvs_ptr_mark(head)));

		if (head == ABTVS_NULL)
		{
			_abtvs_append_maybe_deletable(head_ptr);
		}
		else
		{
			cur_ptr = head_ptr;
			cur = head;
			cur_node_ = abtvs_rawptr(cur);

			while (!NormalTransactionIdPrecedes(cur_node_->xmin,
												oldestXmin))
			{
				cur_ptr = &cur_node_->next;
				cur = abtvs_ptr_atomic_read(cur_ptr);
				if (cur == ABTVS_NULL)
					break;
				cur_node_ = abtvs_rawptr(cur);
			}

			if (cur != ABTVS_NULL)
			{
				/* found a delete target, unlink it from the chain  */
				deletable = cur;
				for (;;)
				{
					/*
					 * If we're still at the chain head, that was marked
					 * by us. We need to CAS a marked value to succeed.
					 */
					if (head_ptr == cur_ptr)
						cur = abtvs_ptr_mark(deletable);

					if (abtvs_ptr_atomic_compare_exchange(cur_ptr, &cur,
														  ABTVS_NULL))
					{
						/* done! */
						if (cur_ptr == head_ptr)
						{
							/*
							 * the chain is now empty, add that to maybe
							 * deletable list
							 */
							_abtvs_append_maybe_deletable(head_ptr);
						}
						break;
					}

					/*
					 * Otherwise, someone inserted something before cur.
					 * follow the chain until we re-find the deletable.
					 */
					if (head_ptr == cur_ptr)
						cur = abtvs_ptr_unmark(cur);
					while (cur != deletable)
					{
						Assert(cur != ABTVS_NULL);
						cur_node_ = abtvs_rawptr(cur);
						cur_ptr = &cur_node_->next;
						cur = abtvs_ptr_atomic_read(cur_ptr);
					}
				}
			}
			else
			{
				deletable = ABTVS_NULL;
			}

			head = abtvs_ptr_atomic_read(head_ptr);
			if (abtvs_ptr_is_marked(head))
			{
				for (;;)
				{
					if (abtvs_ptr_atomic_compare_exchange(
							head_ptr, &head, abtvs_ptr_unmark(head)))
						break;
					Assert(abtvs_ptr_is_marked(head));
				}
			}

			/* delete the chain */
			_abtvs_delete_chain(deletable);
		}

		/* try to delete a few chain heads that we previously found NULL */
		if (abtvs_num_maybe_deletable == abtvs_maybe_deletable_capacity)
			_abtvs_try_delete_maybe_deletable();
	}

	if (!ShutdownRequestPending)
	{
		_abtvs_try_delete_maybe_deletable();
		/*_abtvs_reclaim_pending_deletables(); */
	}

	epoch_release();

	/*elog(LOG, "num_entry_deleted = %lu", abtvs_num_maybe_deletable_deleted);*/
}

static void
_abtvs_init_maybe_deletable(void)
{
	abtvs_maybe_deletable_capacity = ABTVS_GC_MAX_MAYBE_DELETABLE;
	abtvs_maybe_deletable = (abtvs_ptr_atomic **)
		palloc(sizeof(abtvs_ptr_atomic **) * abtvs_maybe_deletable_capacity);
	abtvs_num_maybe_deletable = 0;

}

static void
_abtvs_try_delete_maybe_deletable(void)
{
	Size				i;
	abtvs_ptr_atomic	*head_ptr;
	abtvs_ptr			head;
	uint64				key;

	for (i = 0; i < abtvs_num_maybe_deletable; ++i)
	{
		head_ptr = abtvs_maybe_deletable[i];
		head = abtvs_ptr_atomic_read(head_ptr);

		/* no one except the GC itself should be able to mark the head */
		Assert(!abtvs_ptr_is_marked(head));

		/* someone revived this chain by inserting some live verison into it. */
		while (head == ABTVS_NULL)
		{
			if (abtvs_ptr_atomic_compare_exchange(head_ptr, &head,
												  abtvs_deleted_chain_head))
			{
#ifdef USE_ASSERT_CHECKING
				uint64	old_val;
				uint64	*res;
#endif

				key = lockfree_htab_value_ptr_get_key((uint64 *) head_ptr);

#ifdef USE_ASSERT_CHECKING
				res =
#endif
				lockfree_htab_find_u64(abtvs_htab, key, HASH_REMOVE,
#ifdef USE_ASSERT_CHECKING
					&old_val
#else
					NULL
#endif
				);
				Assert(res);
				Assert(*(abtvs_ptr *) &old_val == abtvs_deleted_chain_head);

				/* done removing this chain */
				++abtvs_num_maybe_deletable_deleted;
				break;
			}
		}
	}

	/*
	 * Different from pending deletable, if anything is not removed, it's not
	 * a memory leak. Instead, someone just inserted something into that empty
	 * chain and thus that's not eligible for deletion. Don't retry.
	 */
	abtvs_num_maybe_deletable = 0;
}

static void
_abtvs_append_maybe_deletable(abtvs_ptr_atomic *head_ptr)
{
	Assert(abtvs_num_maybe_deletable < abtvs_maybe_deletable_capacity);
	abtvs_maybe_deletable[abtvs_num_maybe_deletable++] = head_ptr;
}

