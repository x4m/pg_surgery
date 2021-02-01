/*-------------------------------------------------------------------------
 *
 * heap_surgery.c
 *	  Functions to perform surgery on the damaged heap table.
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_surgery/heap_surgery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/visibilitymap.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/acl.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/* Options to forcefully change the state of a heap tuple. */
typedef enum HeapTupleForceOption
{
	HEAP_FORCE_KILL,
	HEAP_FORCE_FREEZE
} HeapTupleForceOption;

PG_FUNCTION_INFO_V1(heap_force_kill);
PG_FUNCTION_INFO_V1(heap_force_freeze);

static int32 tidcmp(const void *a, const void *b);
static Datum heap_force_common(FunctionCallInfo fcinfo,
							   HeapTupleForceOption heap_force_opt);
static void sanity_check_tid_array(ArrayType *ta, int *ntids);
static void sanity_check_relation(Relation rel);
static BlockNumber tids_same_page_fetch_offnums(ItemPointer tids, int ntids,
												OffsetNumber *next_start_ptr,
												OffsetNumber *offnos);

/*-------------------------------------------------------------------------
 * heap_force_kill()
 *
 * Force kill the tuple(s) pointed to by the item pointer(s) stored in the
 * given tid array.
 *
 * Usage: SELECT heap_force_kill(regclass, tid[]);
 *-------------------------------------------------------------------------
 */
Datum
heap_force_kill(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(heap_force_common(fcinfo, HEAP_FORCE_KILL));
}

/*-------------------------------------------------------------------------
 * heap_force_freeze()
 *
 * Force freeze the tuple(s) pointed to by the item pointer(s) stored in the
 * given tid array.
 *
 * Usage: SELECT heap_force_freeze(regclass, tid[]);
 *-------------------------------------------------------------------------
 */
Datum
heap_force_freeze(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(heap_force_common(fcinfo, HEAP_FORCE_FREEZE));
}

/*-------------------------------------------------------------------------
 * heap_force_common()
 *
 * Common code for heap_force_kill and heap_force_freeze
 *-------------------------------------------------------------------------
 */
static Datum
heap_force_common(FunctionCallInfo fcinfo, HeapTupleForceOption heap_force_opt)
{
	Oid				relid = PG_GETARG_OID(0);
	ArrayType	   *ta = PG_GETARG_ARRAYTYPE_P_COPY(1);
	ItemPointer		tids;
	int				ntids,
					nblocks;
	Relation		rel;
	OffsetNumber   *offnos;
	OffsetNumber	noffs,
					curr_start_ptr,
					next_start_ptr;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				 errhint("heap surgery functions cannot be executed during recovery.")));

	/* Basic sanity checking. */
	sanity_check_tid_array(ta, &ntids);

	rel = relation_open(relid, RowExclusiveLock);

	sanity_check_relation(rel);

	tids = ((ItemPointer) ARR_DATA_PTR(ta));

	/*
	 * If there is more than one tid in the array, sort it so that we can
	 * easily fetch all the tids belonging to one particular page from the
	 * array.
	 */
	if (ntids > 1)
		qsort((void*) tids, ntids, sizeof(ItemPointerData), tidcmp);

	offnos = (OffsetNumber *) palloc(ntids * sizeof(OffsetNumber));
	noffs = curr_start_ptr = next_start_ptr = 0;
	nblocks = RelationGetNumberOfBlocks(rel);

	do
	{
		Buffer			buf;
		Buffer			vmbuf = InvalidBuffer;
		Page			page;
		BlockNumber		blkno;
		OffsetNumber	maxoffset;
		int				i;
		bool			did_modify_page = false;
		bool			did_modify_vm = false;

		/*
		 * Get the offset numbers from the tids belonging to one particular page
		 * and process them one by one.
		 */
		blkno = tids_same_page_fetch_offnums(tids, ntids, &next_start_ptr,
											 offnos);

		/* Calculate the number of offsets stored in offnos array. */
		noffs = next_start_ptr - curr_start_ptr;

		/*
		 * Update the current start pointer so that next time when
		 * tids_same_page_fetch_offnums() is called, we can calculate the number
		 * of offsets present in the offnos array.
		 */
		curr_start_ptr = next_start_ptr;

		/* Check whether the block number is valid. */
		if (blkno >= nblocks)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("skipping block %u for relation \"%s\" because the block number is out of range",
							blkno, RelationGetRelationName(rel))));
			continue;
		}

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(rel, blkno);
		LockBufferForCleanup(buf);

		page = BufferGetPage(buf);

		maxoffset = PageGetMaxOffsetNumber(page);

		/*
		 * Before entering the critical section, pin the visibility map page if
		 * it appears to be necessary.
		 */
		if (heap_force_opt == HEAP_FORCE_KILL && PageIsAllVisible(page))
			visibilitymap_pin(rel, blkno, &vmbuf);

		/* No ereport(ERROR) from here until all the changes are logged. */
		START_CRIT_SECTION();

		for (i = 0; i < noffs; i++)
		{
			OffsetNumber	offno;
			ItemId			itemid;

			if (offnos[i] == 0 || offnos[i] > maxoffset)
			{
				ereport(NOTICE,
						 (errmsg("skipping tid (%u, %u) for relation \"%s\" because the item number is out of range for this block",
								blkno, offnos[i], RelationGetRelationName(rel))));
				continue;
			}

			itemid = PageGetItemId(page, offnos[i]);

			/* Follow any redirections until we find something useful. */
			while (ItemIdIsRedirected(itemid))
			{
				offno = ItemIdGetRedirect(itemid);
				itemid = PageGetItemId(page, offno);
				CHECK_FOR_INTERRUPTS();
			}

			/* Nothing to do if the itemid is unused or already dead. */
			if (!ItemIdIsUsed(itemid) || ItemIdIsDead(itemid))
			{
				if (!ItemIdIsUsed(itemid))
					ereport(NOTICE,
							(errmsg("skipping tid (%u, %u) for relation \"%s\" because it is marked unused",
									blkno, offnos[i], RelationGetRelationName(rel))));
				else
					ereport(NOTICE,
							(errmsg("skipping tid (%u, %u) for relation \"%s\" because it is marked dead",
									blkno, offnos[i], RelationGetRelationName(rel))));
				continue;
			}

			Assert(ItemIdIsNormal(itemid));

			did_modify_page = true;

			if (heap_force_opt == HEAP_FORCE_KILL)
			{

				ItemIdSetDead(itemid);

				/*
				 * If the page is marked all-visible, we must clear
				 * PD_ALL_VISIBLE flag on the page header and an all-visible bit
				 * on the visibility map corresponding to the page.
				 */
				if (PageIsAllVisible(page))
				{
					PageClearAllVisible(page);
					visibilitymap_clear(rel, blkno, vmbuf,
										VISIBILITYMAP_ALL_VISIBLE);
					did_modify_vm = true;
				}
			}
			else
			{
				HeapTupleHeader	htup;
				ItemPointerData	ctid;

				Assert(heap_force_opt == HEAP_FORCE_FREEZE);

				ItemPointerSet(&ctid, blkno, offnos[i]);

				htup = (HeapTupleHeader) PageGetItem(page, itemid);

				/*
				 * Make sure that this tuple holds the correct item pointer
				 * value.
				 */
				if (!ItemPointerEquals(&ctid, &htup->t_ctid))
					ItemPointerSet(&htup->t_ctid, blkno, offnos[i]);

				HeapTupleHeaderSetXmin(htup, FrozenTransactionId);
				HeapTupleHeaderSetXmax(htup, InvalidTransactionId);

				/* We might have MOVED_OFF/MOVED_IN tuples in the database */
				if (htup->t_infomask & HEAP_MOVED)
				{
					if (htup->t_infomask & HEAP_MOVED_OFF)
						HeapTupleHeaderSetXvac(htup, InvalidTransactionId);
					else
						HeapTupleHeaderSetXvac(htup, FrozenTransactionId);
				}

				/*
				 * Clear all the visibility-related bits of this tuple and mark
				 * it as frozen. Also, get rid of HOT_UPDATED and KEYS_UPDATES
				 * bits.
				 */
				htup->t_infomask &= ~HEAP_XACT_MASK;
				htup->t_infomask |= (HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID);
				htup->t_infomask2 &= ~HEAP_HOT_UPDATED;
				htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
			}
		}

		/*
		 * If the page was modified, only then, we mark the buffer dirty or do
		 * the WAL logging.
		 */
		if (did_modify_page)
		{
			/* Mark buffer dirty before we write WAL. */
			MarkBufferDirty(buf);

			/* XLOG stuff */
			if (RelationNeedsWAL(rel))
				log_newpage_buffer(buf, true);
		}

		/* WAL log the VM page if it was modified. */
		if (did_modify_vm && RelationNeedsWAL(rel))
			log_newpage_buffer(vmbuf, false);

		END_CRIT_SECTION();

		UnlockReleaseBuffer(buf);

		if (vmbuf != InvalidBuffer)
			ReleaseBuffer(vmbuf);
	} while (next_start_ptr != ntids);

	relation_close(rel, RowExclusiveLock);

	pfree(ta);
	pfree(offnos);

	PG_RETURN_VOID();
}

/*-------------------------------------------------------------------------
 * tidcmp()
 *
 * Compare two item pointers, return -1, 0, or +1.
 *
 * See ItemPointerCompare for details.
 * ------------------------------------------------------------------------
 */
static int32
tidcmp(const void *a, const void *b)
{
	ItemPointer iptr1 = ((const ItemPointer) a);
	ItemPointer iptr2 = ((const ItemPointer) b);

	return ItemPointerCompare(iptr1, iptr2);
}

/*-------------------------------------------------------------------------
 * sanity_check_tid_array()
 *
 * Perform sanity check on the given tid array.
 * ------------------------------------------------------------------------
 */
static void
sanity_check_tid_array(ArrayType *ta, int *ntids)
{
	if (ARR_HASNULL(ta) && array_contains_nulls(ta))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("array must not contain nulls")));

	*ntids = ArrayGetNItems(ARR_NDIM(ta), ARR_DIMS(ta));

	if (*ntids == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("empty tid array")));
}

/*-------------------------------------------------------------------------
 * sanity_check_relation()
 *
 * Perform sanity check on the given relation.
 * ------------------------------------------------------------------------
 */
static void
sanity_check_relation(Relation rel)
{
	//if (rel->rd_amhandler != HEAP_TABLE_AM_HANDLER_OID)
	//	ereport(ERROR,
	//			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	//			 errmsg("only the relation using heap_tableam_handler is supported")));

	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_MATVIEW &&
		rel->rd_rel->relkind != RELKIND_TOASTVALUE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table, materialized view, or TOAST table",
						RelationGetRelationName(rel))));

	/* Must be owner of the table or superuser. */
	if (!pg_class_ownercheck(RelationGetRelid(rel), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   ACL_KIND_CLASS,
					   RelationGetRelationName(rel));
}

/*-------------------------------------------------------------------------
 * tids_same_page_fetch_offnums()
 *
 * Find out all the tids residing in the same page as tids[next_start_ptr] and
 * fetch the offset number stored in each of them into a caller-allocated offset
 * number array.
 * ------------------------------------------------------------------------
 */
static BlockNumber
tids_same_page_fetch_offnums(ItemPointer tids, int ntids,
							 OffsetNumber *next_start_ptr, OffsetNumber *offnos)
{
	int				i;
	BlockNumber		prev_blkno,
					blkno;
	OffsetNumber	offno;

	prev_blkno = blkno = InvalidBlockNumber;

	for (i = *next_start_ptr; i < ntids; i++)
	{
		ItemPointerData tid = tids[i];

		blkno = ItemPointerGetBlockNumberNoCheck(&tid);
		offno = ItemPointerGetOffsetNumberNoCheck(&tid);

		if (i == *next_start_ptr || (prev_blkno == blkno))
			offnos[i - *next_start_ptr] = offno;
		else
			break;

		prev_blkno = blkno;
	}

	*next_start_ptr = i;
	return prev_blkno;
}