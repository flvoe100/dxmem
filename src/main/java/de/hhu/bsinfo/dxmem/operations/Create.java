/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.AllocationException;
import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.LockManager;
import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ValuePool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Create a new chunk by generating a CID and allocating memory for it
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public final class Create {
    private static final Logger LOGGER = LogManager.getFormatterLogger(Create.class.getSimpleName());

    private static final ValuePool SOP_CREATE = new ValuePool(DXMem.class, "Create");

    static {
        StatisticsManager.get().registerOperation(DXMem.class, SOP_CREATE);
    }

    private final Context m_context;

    public Context getM_context() {
        return m_context;
    }

    /**
     * Constructor
     *
     * @param p_context Context
     */
    public Create(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Create a new chunk
     *
     * @param p_ds AbstractChunk to create/allocate memory for. On success, the resulting CID will be assigned to the
     *             AbstractChunk and the state is set to OK. If the operation failed, the state indicates the error.
     */
    public void create(final AbstractChunk p_ds) {
        p_ds.setID(create(p_ds.sizeofObject()));
        p_ds.setState(ChunkState.OK);
    }

    /**
     * Create a new chunk
     *
     * @param p_ds            AbstractChunk to create/allocate memory for. On success, the resulting CID will be assigned to the
     *                        AbstractChunk and the state is set to OK. If the operation failed, the state indicates the error.
     * @param p_lockOperation Lock operation to execute right after the chunk is created
     */
    public void create(final AbstractChunk p_ds, final ChunkLockOperation p_lockOperation) {
        p_ds.setID(create(p_ds.sizeofObject(), p_lockOperation));
        p_ds.setState(ChunkState.OK);
    }

    /**
     * Create a new chunk with a custom local id
     *
     * @param p_ds            AbstractChunk to create/allocate memory for. On success, the resulting CID will be assigned to the
     *                        AbstractChunk and the state is set to OK. If the operation failed, the state indicates the error.
     * @param p_lid           Custom local id
     * @param p_lockOperation Lock operation to execute right after the chunk is created
     */
    public void create(final AbstractChunk p_ds, final long p_lid, final ChunkLockOperation p_lockOperation) {
        p_ds.setID(create(p_ds.sizeofObject(), p_lid, p_lockOperation));
        p_ds.setState(ChunkState.OK);
    }


    /**
     * Create a new chunk
     *
     * @param p_size Size of the chunk to create (payload size)
     * @return On success, CID assigned to the allocated memory for the chunk, ChunkID.INVALID_ID on failure
     */
    public long create(final int p_size) {
        return create(p_size, ChunkLockOperation.NONE);
    }

    /**
     * Create a new chunk
     *
     * @param p_size          Size of the chunk to create (payload size)
     * @param p_lockOperation Lock operation to execute right after the chunk is created
     * @return On success, CID assigned to the allocated memory for the chunk, ChunkID.INVALID_ID on failure
     */
    public long create(final int p_size, final ChunkLockOperation p_lockOperation) {
        assert assertLockOperationSupport(p_lockOperation);
        assert p_size > 0;

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        if (!m_context.getHeap().malloc(p_size, tableEntry)) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new AllocationException(p_size);
        }

        long cid = ChunkID.getChunkID(m_context.getNodeId(), m_context.getLIDStore().get());

        if (!m_context.getCIDTable().insert(cid, tableEntry)) {
            // revert malloc to avoid corrupted memory
            m_context.getHeap().free(tableEntry);

            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new AllocationException("Allocation of block of memory for LID table failed. Out of memory.");
        }

        // This is actually
        LockManager.LockStatus status = LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry,
                p_lockOperation, -1);

        // this should never fail because the chunk was just created and the defragmentation thread lock is still
        // acquired
        if (status != LockManager.LockStatus.OK) {
            throw new IllegalStateException("Executing lock operation after create op " + p_lockOperation +
                    " for cid " + ChunkID.toHexString(cid) + " failed: " + status);
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_CREATE.add(p_size);

        return cid;
    }

    /**
     * Create one or multiple chunks of the same size
     *
     * @param p_chunkIDs       Pre-allocated array for the CIDs returned
     * @param p_offset         Offset in array to start putting the CIDs to
     * @param p_count          Number of chunks to allocate
     * @param p_size           Size of a single chunk
     * @param p_consecutiveIDs True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *                         consecutive CIDs if available.
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_chunkIDs, final int p_offset, final int p_count, final int p_size,
                      final boolean p_consecutiveIDs) {
        return create(p_chunkIDs, p_offset, p_count, p_size, p_consecutiveIDs, false, ChunkLockOperation.NONE);
    }

    public int create(final long[] p_chunkIDs, final int p_offset, final int p_count, final int p_size,
                            final boolean p_consecutiveIDs, final boolean p_customLID) {
        return create(p_chunkIDs, p_offset, p_count, p_size, p_consecutiveIDs, p_customLID, ChunkLockOperation.NONE);
    }

    /**
     * Create one or multiple chunks of the same size
     *
     * @param p_chunkIDs       Pre-allocated array for the CIDs returned
     * @param p_offset         Offset in array to start putting the CIDs to
     * @param p_count          Number of chunks to allocate
     * @param p_size           Size of a single chunk
     * @param p_consecutiveIDs True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *                         consecutive CIDs if available.
     * @param p_lockOperation  Lock operation to execute for every chunk right after the chunks are created
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_chunkIDs, final int p_offset, final int p_count, final int p_size,
                      final boolean p_consecutiveIDs, final boolean p_customLIDs, final ChunkLockOperation p_lockOperation) {
        assert assertLockOperationSupport(p_lockOperation);
        assert p_size > 0;
        assert p_count > 0;
        assert p_offset >= 0;
        assert p_chunkIDs.length >= p_count;

        m_context.getDefragmenter().acquireApplicationThreadLock();

        if (p_consecutiveIDs) {
            m_context.getLIDStore().getConsecutive(p_chunkIDs, p_offset, p_count);
        } else {
            if (p_customLIDs) {
                m_context.getLIDStore().put(p_chunkIDs, p_offset, p_count);
            } else {
                m_context.getLIDStore().get(p_chunkIDs, p_offset, p_count);
            }
        }

        // create CIDs from LIDs
        for (int i = 0; i < p_count; i++) {
            p_chunkIDs[p_offset + i] = ChunkID.getChunkID(m_context.getNodeId(), p_chunkIDs[p_offset + i]);
        }

        // can't use thread local pool here
        CIDTableChunkEntry[] entries = new CIDTableChunkEntry[p_count];

        for (int i = 0; i < entries.length; i++) {
            entries[i] = new CIDTableChunkEntry();
        }

        int successfulMallocs = m_context.getHeap().malloc(p_size, p_count, entries);

        // add all entries to table
        for (int i = 0; i < successfulMallocs; i++) {
            if (!m_context.getCIDTable().insert(p_chunkIDs[p_offset + i], entries[i])) {
                // revert mallocs for remaining chunks to avoid corrupted memory
                for (int j = i; j < successfulMallocs; j++) {
                    m_context.getHeap().free(entries[j]);
                }

                successfulMallocs = i;
            }

            LockManager.LockStatus status = LockManager.executeAfterOp(m_context.getCIDTable(), entries[i],
                    p_lockOperation, -1);

            // this should never fail because the chunk was just created and the defragmentation thread lock is still
            // acquired
            if (status != LockManager.LockStatus.OK) {
                throw new IllegalStateException("Executing lock operation after create op " + p_lockOperation +
                        " for cid " + ChunkID.toHexString(p_chunkIDs[p_offset + i]) + " failed: " + status);
            }
        }

        // put back or flag as zombies: entries of non successful allocs (rare case)
        if (successfulMallocs != p_count) {
            // put back LIDs that could not be used (after they were added to the table because they might be marked
            // as zombies if LID store is full)
            for (int i = successfulMallocs; i < p_count; i++) {
                if (!m_context.getLIDStore().put(ChunkID.getLocalID(p_chunkIDs[p_offset + i]))) {
                    // lid store full, flag as zombie
                    if (entries[i].isValid()) {
                        m_context.getCIDTable().entryFlagZombie(entries[i]);
                    } else {
                        LOGGER.error("Putting back LIDs failed, invalid entry due to malloc failure. LID %X lost",
                                ChunkID.getLocalID(p_chunkIDs[p_offset + i]));
                    }
                }
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return successfulMallocs;
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_chunkIDs       Pre-allocated array for the CIDs returned
     * @param p_offset         Offset in array to start putting the CIDs to
     * @param p_consecutiveIDs True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *                         consecutive CIDs if available.
     * @param p_sizes          One or multiple (different) sizes. The amount of sizes declared here denotes the number of
     *                         chunks to create
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_chunkIDs, final int p_offset, final boolean p_consecutiveIDs,
                      final int... p_sizes) {
        return create(p_chunkIDs, p_offset, p_consecutiveIDs, ChunkLockOperation.NONE, p_sizes);
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_chunkIDs       Pre-allocated array for the CIDs returned
     * @param p_offset         Offset in array to start putting the CIDs to
     * @param p_consecutiveIDs True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *                         consecutive CIDs if available.
     * @param p_sizes          One or multiple (different) sizes. The amount of sizes declared here denotes the number of
     *                         chunks to create
     * @param p_lockOperation  Lock operation to execute for every chunk right after the chunks are created
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_chunkIDs, final int p_offset, final boolean p_consecutiveIDs,
                      final ChunkLockOperation p_lockOperation, final int... p_sizes) {
        assert assertLockOperationSupport(p_lockOperation);
        assert p_chunkIDs != null;
        assert p_offset >= 0;
        assert p_sizes != null;
        assert p_chunkIDs.length >= p_sizes.length;

        m_context.getDefragmenter().acquireApplicationThreadLock();

        if (p_consecutiveIDs) {
            m_context.getLIDStore().getConsecutive(p_chunkIDs, p_offset, p_sizes.length);
        } else {
            m_context.getLIDStore().get(p_chunkIDs, p_offset, p_sizes.length);
        }

        // create CIDs from LIDs
        for (int i = 0; i < p_sizes.length; i++) {
            p_chunkIDs[p_offset + i] = ChunkID.getChunkID(m_context.getNodeId(), p_chunkIDs[p_offset + i]);
        }

        // can't use thread local pool here
        CIDTableChunkEntry[] entries = new CIDTableChunkEntry[p_sizes.length];

        for (int i = 0; i < entries.length; i++) {
            entries[i] = new CIDTableChunkEntry();
        }

        int successfulMallocs = m_context.getHeap().malloc(entries, p_sizes);
        // add all entries to table
        for (int i = 0; i < successfulMallocs; i++) {
            if (!m_context.getCIDTable().insert(p_chunkIDs[p_offset + i], entries[i])) {
                // revert mallocs for remaining chunks to avoid corrupted memory
                for (int j = i; j < successfulMallocs; j++) {
                    m_context.getHeap().free(entries[j]);
                }

                successfulMallocs = i;
            }

            LockManager.LockStatus status = LockManager.executeAfterOp(m_context.getCIDTable(), entries[i],
                    p_lockOperation, -1);

            // this should never fail because the chunk was just created and the defragmentation thread lock is still
            // acquired
            if (status != LockManager.LockStatus.OK) {
                throw new IllegalStateException("Executing lock operation after create op " + p_lockOperation +
                        " for cid " + ChunkID.toHexString(p_chunkIDs[p_offset + i]) + " failed: " + status);
            }
        }

        // put back or flag as zombies: entries of non successful allocs (rare case)
        if (successfulMallocs != p_sizes.length) {
            // put back LIDs that could not be used (after they were added to the table because they might be marked
            // as zombies if LID store is full)
            for (int i = successfulMallocs; i < p_sizes.length; i++) {
                if (!m_context.getLIDStore().put(ChunkID.getLocalID(p_chunkIDs[p_offset + i]))) {
                    // lid store full, flag as zombie
                    if (entries[i].isValid()) {
                        m_context.getCIDTable().entryFlagZombie(entries[i]);
                    } else {
                        LOGGER.error("Putting back LIDs failed, invalid entry due to malloc failure. LID %X lost",
                                ChunkID.getLocalID(p_chunkIDs[p_offset + i]));
                    }
                }
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return successfulMallocs;
    }

    public int create(final long[] p_chunkIDs, final int p_offset, final long[] p_lids,
                      final ChunkLockOperation p_lockOperation, final int... p_sizes) {
        assert assertLockOperationSupport(p_lockOperation);
        assert p_chunkIDs != null;
        assert p_offset >= 0;
        assert p_sizes != null;
        assert p_chunkIDs.length >= p_sizes.length;
        assert p_lids.length > 0;
        assert p_lids.length == p_sizes.length;

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getLIDStore().put(p_lids);


        // create CIDs from LIDs
        for (int i = 0; i < p_sizes.length; i++) {
            p_chunkIDs[p_offset + i] = ChunkID.getChunkID(m_context.getNodeId(), p_lids[i]);
        }

        // can't use thread local pool here
        CIDTableChunkEntry[] entries = new CIDTableChunkEntry[p_sizes.length];

        for (int i = 0; i < entries.length; i++) {
            entries[i] = new CIDTableChunkEntry();
        }

        int successfulMallocs = m_context.getHeap().malloc(entries, p_sizes);

        // add all entries to table
        for (int i = 0; i < successfulMallocs; i++) {
            if (!m_context.getCIDTable().insert(p_chunkIDs[p_offset + i], entries[i])) {
                // revert mallocs for remaining chunks to avoid corrupted memory
                for (int j = i; j < successfulMallocs; j++) {
                    m_context.getHeap().free(entries[j]);
                }

                successfulMallocs = i;
            }

            LockManager.LockStatus status = LockManager.executeAfterOp(m_context.getCIDTable(), entries[i],
                    p_lockOperation, -1);

            // this should never fail because the chunk was just created and the defragmentation thread lock is still
            // acquired
            if (status != LockManager.LockStatus.OK) {
                throw new IllegalStateException("Executing lock operation after create op " + p_lockOperation +
                        " for cid " + ChunkID.toHexString(p_chunkIDs[p_offset + i]) + " failed: " + status);
            }
        }

        // put back or flag as zombies: entries of non successful allocs (rare case)
        if (successfulMallocs != p_sizes.length) {
            // put back LIDs that could not be used (after they were added to the table because they might be marked
            // as zombies if LID store is full)
            for (int i = successfulMallocs; i < p_sizes.length; i++) {
                if (!m_context.getLIDStore().put(ChunkID.getLocalID(p_chunkIDs[p_offset + i]))) {
                    // lid store full, flag as zombie
                    if (entries[i].isValid()) {
                        m_context.getCIDTable().entryFlagZombie(entries[i]);
                    } else {
                        LOGGER.error("Putting back LIDs failed, invalid entry due to malloc failure. LID %X lost",
                                ChunkID.getLocalID(p_chunkIDs[p_offset + i]));
                    }
                }
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return successfulMallocs;
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_offset         Offset in array to start putting the CIDs to
     * @param p_count          Number of chunks to create (might be less than objects provided)
     * @param p_consecutiveIDs True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *                         consecutive CIDs if available.
     * @param p_chunks         Instances of chunk objects to allocate storage for. On success, the CID is assigned to the object
     *                         and the state is set to OK.
     * @return Number of chunks successfully created. If less than expected, check the chunk objects states for errors.
     */
    public int create(final int p_offset, final int p_count, final boolean p_consecutiveIDs,
                      final AbstractChunk... p_chunks) {
        return create(p_offset, p_count, p_consecutiveIDs, ChunkLockOperation.NONE, p_chunks);
    }

    public int create(final int p_offset, final int p_count, final long[] p_lids, final AbstractChunk... p_chunks) {
        return create(p_offset, p_count, ChunkLockOperation.NONE, p_lids, p_chunks);
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_offset         Offset in array to start putting the CIDs to
     * @param p_count          Number of chunks to create (might be less than objects provided)
     * @param p_consecutiveIDs True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *                         consecutive CIDs if available.
     * @param p_lockOperation  Lock operation to execute for every chunk right after the chunks are created
     * @param p_chunks         Instances of chunk objects to allocate storage for. On success, the CID is assigned to the object
     *                         and the state is set to OK.
     * @return Number of chunks successfully created. If less than expected, check the chunk objects states for errors.
     */
    public int create(final int p_offset, final int p_count, final boolean p_consecutiveIDs,
                      final ChunkLockOperation p_lockOperation, final AbstractChunk... p_chunks) {
        assert assertLockOperationSupport(p_lockOperation);
        assert p_chunks != null;

        long[] cids = new long[p_count];
        int[] sizes = new int[p_count];

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = p_chunks[p_offset + i].sizeofObject();
        }

        int successfullMallocs = create(cids, 0, p_consecutiveIDs, p_lockOperation, sizes);

        for (int i = 0; i < successfullMallocs; i++) {
            p_chunks[p_offset + i].setID(cids[i]);
            p_chunks[p_offset + i].setState(ChunkState.OK);
        }

        for (int i = successfullMallocs; i < p_count; i++) {
            p_chunks[p_offset + i].setID(ChunkID.INVALID_ID);
            p_chunks[p_offset + i].setState(ChunkState.DOES_NOT_EXIST);
        }

        return successfullMallocs;
    }

    public int create(final int p_offset, final int p_count,
                      final ChunkLockOperation p_lockOperation, final long[] p_lids, final AbstractChunk... p_chunks) {
        assert assertLockOperationSupport(p_lockOperation);
        assert p_chunks != null;

        long[] cids = new long[p_count];
        int[] sizes = new int[p_count];

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = p_chunks[p_offset + i].sizeofObject();
        }
        int successfullMallocs = create(cids, 0, p_lids, p_lockOperation, sizes);


        for (int i = 0; i < successfullMallocs; i++) {
            p_chunks[p_offset + i].setID(cids[i]);
            p_chunks[p_offset + i].setState(ChunkState.OK);
        }

        for (int i = successfullMallocs; i < p_count; i++) {
            p_chunks[p_offset + i].setID(ChunkID.INVALID_ID);
            p_chunks[p_offset + i].setState(ChunkState.DOES_NOT_EXIST);
        }

        return successfullMallocs;
    }


    public long create(final int p_size, final long p_lid, final ChunkLockOperation p_lockOperation) {
        assert assertLockOperationSupport(p_lockOperation);
        assert p_size > 0;

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        if (!m_context.getHeap().malloc(p_size, tableEntry)) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new AllocationException(p_size);
        }
        m_context.getLIDStore().put(p_lid);
        //check if chunkID is reserved
        long cid = ChunkID.getChunkID(m_context.getNodeId(), p_lid);

        if (!m_context.getCIDTable().insert(cid, tableEntry)) {
            // revert malloc to avoid corrupted memory
            m_context.getHeap().free(tableEntry);

            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new AllocationException("Allocation of block of memory for LID table failed. Out of memory.");
        }

        // This is actually
        LockManager.LockStatus status = LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry,
                p_lockOperation, -1);

        // this should never fail because the chunk was just created and the defragmentation thread lock is still
        // acquired
        if (status != LockManager.LockStatus.OK) {
            throw new IllegalStateException("Executing lock operation after create op " + p_lockOperation +
                    " for cid " + ChunkID.toHexString(cid) + " failed: " + status);
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_CREATE.add(p_size);

        return cid;
    }


    /**
     * Assert the lock operation used
     *
     * @param p_lockOperation Lock operation to use with the current op
     * @return True if ok, exception thrown if not supported
     */
    private boolean assertLockOperationSupport(final ChunkLockOperation p_lockOperation) {
        switch (p_lockOperation) {
            case NONE:
            case WRITE_LOCK_ACQ_POST_OP:
            case READ_LOCK_ACQ_POST_OP:
                return true;

            case WRITE_LOCK_ACQ_PRE_OP:
            case WRITE_LOCK_SWAP_PRE_OP:
            case WRITE_LOCK_REL_POST_OP:
            case WRITE_LOCK_SWAP_POST_OP:
            case WRITE_LOCK_ACQ_OP_REL:
            case WRITE_LOCK_SWAP_OP_REL:
            case WRITE_LOCK_ACQ_OP_SWAP:
            case READ_LOCK_ACQ_PRE_OP:
            case READ_LOCK_SWAP_PRE_OP:
            case READ_LOCK_REL_POST_OP:
            case READ_LOCK_SWAP_POST_OP:
            case READ_LOCK_ACQ_OP_REL:
            case READ_LOCK_SWAP_OP_REL:
            case READ_LOCK_ACQ_OP_SWAP:
                throw new MemoryRuntimeException("Unsupported lock operation on create op: " + p_lockOperation);

            default:
                throw new IllegalStateException("Unhandled lock operation");
        }
    }
}
