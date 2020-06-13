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

package de.hhu.bsinfo.dxmem.core;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.SpareLocalID;
import de.hhu.bsinfo.dxutils.serialization.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static de.hhu.bsinfo.dxmem.data.SpareLocalID.MAX_SINGLE_VALUE_INTERVAL_SIZE;

/**
 * Stores free LocalIDs
 *
 * @author Florian Klein 30.04.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public final class LIDStore implements Importable, Exportable {
    private static final int STORE_CAPACITY = 5000000; //is overwritten by config
    private static final int MAX_INTERVAL_SIZE = 65_535;


    private static final Logger LOGGER = LogManager.getFormatterLogger(LIDStore.class.getSimpleName());

    private SpareLIDStore m_spareLIDStore;
    private AtomicLong m_localIDCounter;

    /**
     * Constructor for importing from memory dump
     */
    LIDStore() {
        m_spareLIDStore = new SpareLIDStore();
        m_localIDCounter = new AtomicLong(0);

    }

    public SpareLIDStore getM_spareLIDStore() {
        return m_spareLIDStore;
    }

    /**
     * Creates an instance of LIDStore
     *
     * @param p_ownNodeId Node id of current instance
     * @param p_cidTable  CIDTable instance
     */
    LIDStore(final short p_ownNodeId, final CIDTable p_cidTable) {
        m_spareLIDStore = new SpareLIDStore(p_ownNodeId, p_cidTable, STORE_CAPACITY);
        m_localIDCounter = new AtomicLong(0);

    }

    /**
     * Creates an instance of LIDStore
     *
     * @param p_ownNodeId         Node id of current instance
     * @param p_cidTable          CIDTable instance
     * @param p_spareLIDStoreSize Size of the spare local id store (ring buffer)
     */
    LIDStore(final short p_ownNodeId, final CIDTable p_cidTable, final int p_spareLIDStoreSize) {
        m_spareLIDStore = new SpareLIDStore(p_ownNodeId, p_cidTable, p_spareLIDStoreSize);
        m_localIDCounter = new AtomicLong(0);
    }

    /**
     * Get the status object
     *
     * @return Status object
     */
    public LIDStoreStatus getStatus() {
        LIDStoreStatus status = new LIDStoreStatus();

        status.m_currentLIDCounter = m_localIDCounter.get() - 1;
        status.m_totalFreeLIDs = m_spareLIDStore.m_overallCount;
        status.m_lidsInStore = m_spareLIDStore.m_count;

        return status;
    }

    /**
     * Get the currently highest LID used
     *
     * @return Currently highest LID used
     */
    public long getCurrentHighestLID() {
        return m_localIDCounter.get() - 1;
    }

    /**
     * Get an LID from the store
     *
     * @return LID
     */
    public long get() {
        long ret;

        // try to re-use spare ones first
        ret = m_spareLIDStore.get();

        // If no free ID exist, get next local ID
        if (ret == -1) {
            ret = m_localIDCounter.getAndIncrement();
            // a 48-bit counter is enough for now and a while, so we don't check for overflows
            // (not counting the assert)
            assert ret <= ChunkID.MAX_LOCALID;
        }

        return ret;
    }

    /**
     * Get multiple LIDs from the store
     *
     * @param p_lids   Array to write LIDs to
     * @param p_offset Offset to start in Array
     * @param p_count  Number of LIDs to get
     */
    public void get(final long[] p_lids, final int p_offset, final int p_count) {
        assert p_lids != null;
        assert p_count > 0;
        // try to re-use as many already used LIDs as possible
        int reusedLids;
        int offset = p_offset;
        do {
            reusedLids = m_spareLIDStore.get(p_lids, offset, p_count - (p_offset - offset));
            offset += reusedLids;
        } while (reusedLids > 0 && offset - p_offset < p_count);


        // fill up with new LIDs if necessary
        if (offset - p_offset < p_count) {
            long startId;
            long endId;

            // generate new LIDs
            do {
                startId = m_localIDCounter.get();
                endId = startId + (p_count - (offset - p_offset));

                // a 48-bit counter is enough for now and a while, so we don't check for overflows
                // (not counting the assert)
                assert endId <= ChunkID.MAX_LOCALID;
            } while (!m_localIDCounter.compareAndSet(startId, endId));

            for (int i = 0; i < p_count - (offset - p_offset); i++) {
                p_lids[offset + i] = startId++;
            }
        }
    }

    public boolean put(final long... p_lids) {
        assert p_lids.length > 0;
        for (long p_lid : p_lids) {
            this.put(p_lid);
        }
        return false;
    }

    public boolean put(final long[] p_lids, final int p_offset, final int p_count) {
        assert p_lids.length > 0;
        long startIncrementCounter = m_localIDCounter.get();

        for (int i = 0; i < p_count; i++) {
            if (!this.put(p_lids[p_offset + i])) {
                //spare Store is full or something went wrong!!
                //move putPointer
                LOGGER.error("Error in putting the lid: %d with localIDCounter: %d ", p_lids[p_offset + i], m_localIDCounter.get());
                LOGGER.error("lid before: ", p_lids[p_offset + i - 1]);
                LOGGER.error("lid 2 before: ", p_lids[p_offset + i - 2]);
                LOGGER.error(m_spareLIDStore.m_count);
                LOGGER.error(m_spareLIDStore.m_overallCount);
                m_spareLIDStore.writeRingBufferSpareLocalIDs();
                m_spareLIDStore.undoPuts(-i);
                m_localIDCounter.set(startIncrementCounter);

                return false;
            }
        }
        return true;
    }

    public boolean put(final long p_lid) {
        assert p_lid <= ChunkID.MAX_LOCALID;
        long p_currentLocalIDCounter = m_localIDCounter.get();

        if (p_lid < p_currentLocalIDCounter) {
            // Two possibilities: 1. Extend an interval 2. Insert a single value and move all elements one field
            //mainly reused ids either removed or ringbuffer was to full
            if(p_lid == 2199025182474L || p_lid == 4398053735176L|| p_lid ==8796093867667L || p_lid == 13194148520201L) {
                LOGGER.info("1 p_currentLocalIDCounter", p_currentLocalIDCounter);

            }
            return m_spareLIDStore.putLowerLID(p_lid);

        } else if (p_lid > p_currentLocalIDCounter) {
            //put lid into spare until p_lid
            boolean success = false;
            if(p_lid == 2199025182474L || p_lid == 4398053735176L|| p_lid ==8796093867667L || p_lid == 13194148520201L) {
                LOGGER.info("2 p_currentLocalIDCounter", p_currentLocalIDCounter);

            }
            if (p_currentLocalIDCounter == p_lid - 1) {
                success = m_spareLIDStore.put(m_localIDCounter.get());
                if (success) {
                    m_localIDCounter.set(p_lid + 1);
                }
                return success;
            }
            long intervalSize = p_lid - p_currentLocalIDCounter;

            //check if intervalSize is grater than 65.536
            if (intervalSize > MAX_INTERVAL_SIZE) {
                if(p_lid == 2199025182474L || p_lid == 4398053735176L|| p_lid ==8796093867667L || p_lid == 13194148520201L) {
                    LOGGER.info("3 p_currentLocalIDCounter", p_currentLocalIDCounter);

                }
                success = m_spareLIDStore.putInterval(p_currentLocalIDCounter, p_lid - 1);
            } else {
                if(p_lid == 2199025182474L || p_lid == 4398053735176L|| p_lid ==8796093867667L || p_lid == 13194148520201L) {
                    LOGGER.info("4 p_currentLocalIDCounter", p_currentLocalIDCounter);

                }
                success = m_spareLIDStore.putSingleValueInterval(p_currentLocalIDCounter, intervalSize);

            }
            if (success) {
                if(p_lid == 2199025182474L || p_lid == 4398053735176L|| p_lid ==8796093867667L || p_lid == 13194148520201L) {
                    LOGGER.info("5 p_currentLocalIDCounter", p_currentLocalIDCounter);

                }
                m_localIDCounter.set(p_lid + 1);

            }
            return success;
        } else {

            // LOGGER.trace("LID is just next local id counter");
            //dont use get-Method because we dont want to get a lid of sparse
            long counterLID = m_localIDCounter.getAndIncrement();
            assert counterLID == p_lid;
            return true;
        }
    }

    /**
     * Get multiple consecutive LIDs
     *
     * @param p_lids   Array to write LIDs to
     * @param p_offset Offset to start in Array
     * @param p_count  Number of LIDs to get
     */
    public void getConsecutive(final long[] p_lids, final int p_offset, final int p_count) {
        assert p_lids != null;
        assert p_count > 0;

        // don't use the lid store for consecutive LIDs because that won't work very well on
        // random delete patterns and just wastes processing time searching that's likely not going to be found

        long startId;
        long endId;

        // generate new LIDs

        do {
            startId = m_localIDCounter.get();
            endId = startId + p_count;

            // a 48-bit counter is enough for now and a while, so we don't check for overflows
            // (not counting the assert)
            assert endId <= ChunkID.MAX_LOCALID;
        } while (!m_localIDCounter.compareAndSet(startId, endId));

        for (int i = 0; i < p_count; i++) {
            p_lids[p_offset + i] = startId++;
        }
    }


    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_localIDCounter.get());
        p_exporter.exportObject(m_spareLIDStore);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_localIDCounter.set(p_importer.readLong(m_localIDCounter.get()));
        p_importer.importObject(m_spareLIDStore);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES + m_spareLIDStore.sizeofObject();
    }

    /**
     * Store for spare free LIDs
     * Note: using a lock here because this simplifies synchronization and we can't let
     * multiple threads re-fill the spare lid store when it's empty but there are still
     * zombie entries in the cid table
     */
    public static final class SpareLIDStore implements Importable, Exportable {

        private short m_ownNodeId;
        private CIDTable m_cidTable;

        private long[] m_ringBufferSpareLocalIDs;
        private int m_getPosition;
        private int m_putPosition;
        private int m_freeLIDs;
        // available free lid elements stored in ring buffer
        private int m_count;
        // This counts the total available lids in the array
        // as well as elements that are still allocated
        // (because they don't fit into the local array anymore)
        // but not valid -> zombies
        private volatile long m_overallCount;

        private final Lock m_ringBufferLock = new ReentrantLock(false);
        private static final Logger LOGGER = LogManager.getFormatterLogger(SpareLIDStore.class.getSimpleName());

        /**
         * Constructor for importing from memory dump
         */
        SpareLIDStore() {

        }

        /**
         * Constructor
         *
         * @param p_ownNodeId Node id of current instance
         * @param p_cidTable  CIDTable instance
         * @param p_capacity  Capacity of store
         */
        SpareLIDStore(final short p_ownNodeId, final CIDTable p_cidTable, final int p_capacity) {
            m_ownNodeId = p_ownNodeId;
            m_cidTable = p_cidTable;
            m_ringBufferSpareLocalIDs = new long[p_capacity];
            m_getPosition = 0;
            m_putPosition = 0;
            m_count = 0;
            m_freeLIDs = 0;
            m_overallCount = 0;
        }

        /**
         * Get a LID from the store
         *
         * @return LID or -1 if store is empty and no zombies are available anymore
         */
        public long get() {
            long ret = -1;
            if (m_overallCount > 0) {
                m_ringBufferLock.lock();

                if (m_count == 0 && m_overallCount > 0) {
                    // ignore return value
                    refillStore();
                }
                if (m_count > 0) {
                    ret = m_ringBufferSpareLocalIDs[m_getPosition];
                    boolean isInterval = SpareLocalID.getEntryInfo(ret) == 1;
                    if (isInterval) {
                        long startIntervalId = SpareLocalID.getLID(ret);
                        long endIntervalId = SpareLocalID.getLID(m_ringBufferSpareLocalIDs[(m_getPosition + 1) % m_ringBufferSpareLocalIDs.length]);
                        ret = startIntervalId;
                        if (startIntervalId + 1 == endIntervalId) {
                            //only one element left
                            m_ringBufferSpareLocalIDs[m_getPosition] = 0;
                            moveGetPointer(1);
                            m_ringBufferSpareLocalIDs[m_getPosition] = SpareLocalID.getSpareLocalID(false, endIntervalId);
                            m_count--;
                            m_overallCount--;
                            m_freeLIDs--;
                        } else {
                            //just shrink interval
                            m_freeLIDs--;
                            m_ringBufferSpareLocalIDs[m_getPosition] = SpareLocalID.getSpareLocalID(true, startIntervalId + 1);
                        }
                    } else {
                        //just return single value

                        ret = SpareLocalID.getLID(ret);
                        m_count--;
                        m_overallCount--;
                        m_freeLIDs--;
                        moveGetPointer(1);
                    }
                }
                m_ringBufferLock.unlock();
            }
            return ret;
        }

        /**
         * Get multiple LIDs from the store
         *
         * @param p_lids   Array to write LIDs to
         * @param p_offset Offset to start in Array
         * @param p_count  Number of LIDs to get
         * @return Number of LIDs returned. If less than p_count, store is empty and no zombies are available anymore
         */
        public int get(final long[] p_lids, final int p_offset, final int p_count) {
            assert p_lids != null;
            assert p_offset >= 0;
            assert p_count > 0;
            int counter = 0;
            // lids in store or zombie entries in table
            if (m_overallCount > 0) {
                m_ringBufferLock.lock();
                int restCount = p_count;
                do {
                    if (m_count == 0 && m_overallCount > 0) {
                        // store empty but there are still zombies in the tables
                        refillStore();
                    }
                    long currSparseEntry = m_ringBufferSpareLocalIDs[m_getPosition];

                    long isInterval = currSparseEntry >> 48;
                    isInterval = isInterval < 0 ? SpareLocalID.getSingleEntryIntervalSize(currSparseEntry) : isInterval;
                    if (isInterval > 1) {

                        //singleValueInterval
                        long startIntervalId = SpareLocalID.getLID(currSparseEntry);
                        long intervalSize = SpareLocalID.getSingleEntryIntervalSize(currSparseEntry);
                        if (intervalSize > restCount) {
                            //just shrink
                            for (int i = 0; i < restCount; i++) {

                                p_lids[p_offset + i + counter] = startIntervalId + i;
                            }
                            if (intervalSize - restCount == 1) {
                                //only one value left
                                m_ringBufferSpareLocalIDs[m_getPosition] = SpareLocalID.getSpareLocalID(false, startIntervalId + intervalSize - 1);
                            } else {
                                //interval just shrinks left site
                                m_ringBufferSpareLocalIDs[m_getPosition] = SpareLocalID.getSingleEntryIntervalID(startIntervalId + restCount, intervalSize - restCount);
                            }
                            counter = p_count;
                            m_freeLIDs -= restCount;
                            restCount = 0;

                        } else if (intervalSize <= restCount) {
                            //get whole interval and move

                            for (int i = 0; i < intervalSize; i++) {
                                p_lids[p_offset + i + counter] = startIntervalId + i;
                            }
                            restCount -= intervalSize;
                            counter += intervalSize;
                            m_count--;
                            m_freeLIDs -= intervalSize;
                            m_ringBufferSpareLocalIDs[m_getPosition] = 0;
                            moveGetPointer(1);
                        }

                    } else if (isInterval == 1) {
                        long startIntervalId = SpareLocalID.getLID(currSparseEntry);
                        long endIntervalId = SpareLocalID.getLID(m_ringBufferSpareLocalIDs[(m_getPosition + 1) % m_ringBufferSpareLocalIDs.length]);
                        long intervalSize = endIntervalId - startIntervalId + 1;
                        if (intervalSize > restCount) {
                            //just shrink
                            for (int i = 0; i < restCount; i++) {

                                p_lids[p_offset + i + counter] = startIntervalId + i;
                            }
                            if (intervalSize - restCount == 1) {
                                //only one value left
                                m_ringBufferSpareLocalIDs[m_getPosition] = 0;
                                moveGetPointer(1);
                                m_ringBufferSpareLocalIDs[m_getPosition] = SpareLocalID.getSpareLocalID(false, endIntervalId);
                                m_count--;

                            } else if (intervalSize - restCount <= MAX_INTERVAL_SIZE && intervalSize - restCount != 1) {
                                //one single value interval left
                                m_ringBufferSpareLocalIDs[m_getPosition] = 0;
                                moveGetPointer(1);

                                m_ringBufferSpareLocalIDs[m_getPosition] = SpareLocalID.getSingleEntryIntervalID(startIntervalId + restCount, (endIntervalId - startIntervalId + 1) - restCount);
                                m_count--;

                            } else {
                                //interval just shrinks left site
                                m_ringBufferSpareLocalIDs[m_getPosition] = SpareLocalID.getSpareLocalID(true, startIntervalId + restCount);

                            }
                            counter = p_count;
                            m_freeLIDs -= restCount;
                            restCount = 0;

                        } else if (intervalSize <= restCount) {
                            //get whole interval and move

                            for (int i = 0; i < intervalSize; i++) {
                                p_lids[p_offset + i + counter] = startIntervalId + i;
                            }

                            restCount -= intervalSize;
                            counter += intervalSize;
                            m_count -= 2;
                            m_freeLIDs -= intervalSize;
                            m_ringBufferSpareLocalIDs[m_getPosition] = 0;
                            m_ringBufferSpareLocalIDs[(m_getPosition + 1) % m_ringBufferSpareLocalIDs.length] = 0;

                            moveGetPointer(2);

                        }

                    } else if (isInterval == 0) {
                        //just return single value
                        p_lids[p_offset + counter] = SpareLocalID.getLID(currSparseEntry);
                        restCount--;
                        counter++;
                        m_count--;
                        m_freeLIDs--;
                        m_ringBufferSpareLocalIDs[m_getPosition] = 0;
                        moveGetPointer(1);

                    }

                } while (counter != p_count && restCount <= m_freeLIDs && (m_overallCount > 0 || m_count > 0));
                m_overallCount -= counter;
                m_ringBufferLock.unlock();
            }

            return counter;
        }

        /**
         * Put a single LID to the store
         *
         * @param p_lid LID to add to the store
         * @return True if successful, false if store is full (callers has to treat chunk as a zombie)
         */
        public boolean put(final long p_lid) {
            boolean ret;
            m_ringBufferLock.lock();
            if (m_count + 1 <= m_ringBufferSpareLocalIDs.length) {
                m_ringBufferSpareLocalIDs[m_putPosition] = SpareLocalID.getSpareLocalID(false, p_lid);
                m_count++;
                m_overallCount++;
                m_freeLIDs++;
                movePutPointer(1);

                ret = true;
            } else {
                LOGGER.error("Not enough space in the ring buffer");
                ret = false;
            }
            m_ringBufferLock.unlock();
            return ret;
        }

        /**
         * Puts and interval [p_startLID, p_endLID] into the ring buffer
         *
         * @param p_startLid Start-LID of the interval
         * @param p_endLid   End-LID of the interval
         * @return success or failure
         */
        public boolean putInterval(final long p_startLid, final long p_endLid) {
            boolean ret;
            m_ringBufferLock.lock();

            if (m_count + 2 <= m_ringBufferSpareLocalIDs.length) {
                //Left border
                m_ringBufferSpareLocalIDs[m_putPosition] = SpareLocalID.getSpareLocalID(true, p_startLid);
                //move one position
                movePutPointer(1);

                //right border
                m_ringBufferSpareLocalIDs[m_putPosition] = SpareLocalID.getSpareLocalID(true, p_endLid);

                movePutPointer(1);

                m_count += 2;
                m_overallCount += 2;
                m_freeLIDs += p_endLid - p_startLid + 1;

                ret = true;
            } else {
                LOGGER.error("Not enough space in the ring buffer");
                ret = false;
            }
            m_ringBufferLock.unlock();
            return ret;
        }

        /**
         * Puts limited interval into the ring buffer. The interval can max. be 65535 long, otherwise use {@link #putInterval(long, long) putInterval} method.
         *
         * @param p_startLID   Inclusive Start-LID
         * @param intervalSize Interval size. Inclusive Start-LID
         * @return success or failure
         */
        public boolean putSingleValueInterval(final long p_startLID, final long intervalSize) {
            boolean ret;
            m_ringBufferLock.lock();
            if (m_count + 1 <= m_ringBufferSpareLocalIDs.length) {

                //abuse ChunkID.getChunkID, because it is the same as we need
                long singleValueInterval = SpareLocalID.getSingleEntryIntervalID(p_startLID, intervalSize);
                m_ringBufferSpareLocalIDs[m_putPosition] = singleValueInterval;
                m_count++;
                m_overallCount++;
                movePutPointer(1);
                m_freeLIDs += intervalSize;

                ret = true;
            } else {
                LOGGER.error("Not enough space in the ring buffer");
                ret = false;
            }
            m_ringBufferLock.unlock();
            return ret;
        }

        //-----------------------Methods for putting a lower lid than the counter------------------------------------

        /**
         * Inserts a lid which is lower than the {@link #m_localIDCounter localIDCounter}.
         *
         * @param p_lid lid, which should inserted
         * @return success or failure
         */
        public boolean putLowerLID(final long p_lid) {
            if (m_overallCount > 0) {
                m_ringBufferLock.lock();
                if (m_count == 0 && m_overallCount > 0) {
                    // ignore return value
                    refillStore();
                }
                int ringBufferSize = m_ringBufferSpareLocalIDs.length;
                int left = m_getPosition;
                int right = m_putPosition;
                boolean ret = false;
                while (left != right) {
                    int expandType = getExpandTypeOfPosition(p_lid, left);
                    if (expandType != 0) {
                        switch (expandType) {
                            case 1: //expand interval with p_lid left side
                                return expandSingleValueToIntervalLeftSide(p_lid, left);
                            case 2: //expand interval with p_lid right side
                                return expandSingleValueToIntervalRightSide(p_lid, left);
                            case 3: //expand interval with p_lid and next right entry
                                combineIntervalWithRightSide(left);
                                return true;
                            case 4:
                                ret = moveElementsOneField(left);
                                m_ringBufferSpareLocalIDs[left] = SpareLocalID.getSpareLocalID(false, p_lid);
                                return ret;
                            case 5:
                                //break interval
                                return breakEntry(p_lid, left);
                        }
                        break;
                    }

                    long intervalMetaData = SpareLocalID.getEntryInfo(m_ringBufferSpareLocalIDs[left]);
                    int step = intervalMetaData == 1 ? 2 : 1;
                    left = (left + step) % ringBufferSize;
                }

            }
            return false;
        }

        /**
         * Combines the current entry with the lower lid and the right neighbour entry. It checks first what type the current entry is and then the neighbour.
         *
         * @param pos position of the current entry
         */
        private void combineIntervalWithRightSide(final int pos) {
            long posEntry = m_ringBufferSpareLocalIDs[pos];
            int nextPos = (pos + 1) % m_ringBufferSpareLocalIDs.length;

            long nextEntry = m_ringBufferSpareLocalIDs[nextPos];
            long currIntervalMetaData = SpareLocalID.getEntryInfo(posEntry);
            currIntervalMetaData = currIntervalMetaData < 0 ? SpareLocalID.getSingleEntryIntervalSize(posEntry) : currIntervalMetaData;

            if (currIntervalMetaData > 1) {
                //single entry interval
                long posID = SpareLocalID.getLID(posEntry);

                long nextIntervalMetaData = SpareLocalID.getEntryInfo(nextEntry);
                nextIntervalMetaData = nextIntervalMetaData < 0 ? SpareLocalID.getSingleEntryIntervalSize(nextEntry) : nextIntervalMetaData;

                if (nextIntervalMetaData > 1) {
                    //single entry interval next right to single entry interval
                    if (currIntervalMetaData + nextIntervalMetaData + 1 > MAX_INTERVAL_SIZE) {
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(true, posID);
                        m_ringBufferSpareLocalIDs[nextPos] = SpareLocalID.getSpareLocalID(true, currIntervalMetaData + nextIntervalMetaData + 1);

                    } else {
                        moveElementsOneFieldBack(pos);
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(posID, currIntervalMetaData + nextIntervalMetaData + 1);
                    }
                } else if (nextIntervalMetaData == 1) {
                    //single entry interval next right to two entry interval
                    long rightBorderEntry = m_ringBufferSpareLocalIDs[(nextPos + 1) % m_ringBufferSpareLocalIDs.length];
                    long rightBorderID = SpareLocalID.getLID(rightBorderEntry);
                    moveElementsOneFieldBack(nextPos);
                    m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(true, posID);
                    m_ringBufferSpareLocalIDs[nextPos] = SpareLocalID.getSpareLocalID(true, rightBorderID);

                } else {
                    //single entry interval next right to single value
                    if (currIntervalMetaData + 1 > MAX_INTERVAL_SIZE) {
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(true, posID);
                        m_ringBufferSpareLocalIDs[nextPos] = SpareLocalID.getSpareLocalID(true, posID + currIntervalMetaData + 1);

                    } else {
                        moveElementsOneFieldBack(pos);
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(posID, currIntervalMetaData + 1);
                    }
                }

            } else if (currIntervalMetaData == 1) {
                //two entry interval [tested]

                nextPos = (nextPos + 1) % m_ringBufferSpareLocalIDs.length;
                nextEntry = m_ringBufferSpareLocalIDs[nextPos];

                long nextIntervalMetaData = SpareLocalID.getEntryInfo(nextEntry);
                nextIntervalMetaData = nextIntervalMetaData < 0 ? SpareLocalID.getSingleEntryIntervalSize(nextEntry) : nextIntervalMetaData;
                if (nextIntervalMetaData > 1) {
                    //two entry interval next right to single entry interval
                    long rightBorderID = SpareLocalID.getLID(nextEntry) + nextIntervalMetaData - 1;
                    nextPos = (pos + 1) % m_ringBufferSpareLocalIDs.length;
                    moveElementsOneFieldBack(nextPos);
                    m_ringBufferSpareLocalIDs[nextPos] = SpareLocalID.getSpareLocalID(true, rightBorderID);

                } else if (nextIntervalMetaData == 1) {
                    //two entry interval next right to two entry interval
                    int nextRightBorderPos = (nextPos + 1) % m_ringBufferSpareLocalIDs.length;
                    long rightBorderID = SpareLocalID.getLID(m_ringBufferSpareLocalIDs[nextRightBorderPos]);
                    moveElementsOneFieldBack(nextPos);
                    nextPos = (nextPos - 1) % m_ringBufferSpareLocalIDs.length;
                    moveElementsOneFieldBack(nextPos);
                    //maybe better method where direct 2 steps back
                    m_ringBufferSpareLocalIDs[nextPos] = SpareLocalID.getSpareLocalID(true, rightBorderID);
                } else {
                    //two entry interval next right to single value
                    long singleValueID = SpareLocalID.getLID(nextEntry);
                    nextPos = (pos + 1) % m_ringBufferSpareLocalIDs.length;
                    moveElementsOneFieldBack(nextPos);
                    m_ringBufferSpareLocalIDs[nextPos] = SpareLocalID.getSpareLocalID(true, singleValueID);
                }
            } else {
                //single value [tested]

                long posID = SpareLocalID.getLID(posEntry);

                long nextIntervalMetaData = SpareLocalID.getEntryInfo(nextEntry);
                nextIntervalMetaData = nextIntervalMetaData < 0 ? SpareLocalID.getSingleEntryIntervalSize(nextEntry) : nextIntervalMetaData;
                if (nextIntervalMetaData > 1) {
                    //single value next right to single entry interval
                    //check size
                    if (nextIntervalMetaData + 1 > MAX_INTERVAL_SIZE) {
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(true, posID);
                        m_ringBufferSpareLocalIDs[nextPos] = SpareLocalID.getSpareLocalID(true, posID + nextIntervalMetaData + 1);

                    } else {
                        moveElementsOneFieldBack(pos);
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(posID, nextIntervalMetaData + 2);

                    }
                } else if (nextIntervalMetaData == 1) {
                    //single value next right to two entry interval
                    int rightBorderEntryPos = (nextPos + 1) % m_ringBufferSpareLocalIDs.length;
                    long rightBorderEntry = m_ringBufferSpareLocalIDs[rightBorderEntryPos];
                    long rightBorderID = SpareLocalID.getLID(rightBorderEntry);
                    moveElementsOneFieldBack(nextPos);
                    m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(true, posID);
                    m_ringBufferSpareLocalIDs[nextPos] = SpareLocalID.getSpareLocalID(true, rightBorderID);

                } else {
                    //single value next right to single value
                    moveElementsOneFieldBack(pos);
                    m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(posID, 3);
                }
            }
        }

        /**
         * Transforms a single value entry to an interval with the lower lid. Right border is the lower lid
         *
         * @param p_lid lower lid
         * @param pos   position of the single value entry
         * @return success or failure
         */
        private boolean expandSingleValueToIntervalRightSide(final long p_lid, final int pos) {
            long posEntry = m_ringBufferSpareLocalIDs[pos];
            long posID = SpareLocalID.getLID(posEntry);
            long intervalMetaData = SpareLocalID.getEntryInfo(posEntry);
            intervalMetaData = intervalMetaData < 0 ? SpareLocalID.getSingleEntryIntervalSize(posEntry) : intervalMetaData;
            if (intervalMetaData > 1) {
                //single entry interval
                //check first if interval has enough space
                if (intervalMetaData + 1 > MAX_INTERVAL_SIZE) {
                    //not enough! move one and create a two entry interval
                    boolean ret = moveElementsOneField(pos);
                    if (ret) {
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(true, posID);
                        m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, p_lid);

                    }
                    return ret;
                } else {
                    m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(posID, intervalMetaData + 1);
                    return true;

                }
            } else if (intervalMetaData == 1) {
                m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, p_lid);
                return true;

            } else {
                m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(posID, 2);
                return true;
            }
        }

        /**
         * Transforms a single value entry to an interval with the lower lid. Left border of the interval is p_lid.
         *
         * @param p_lid lower lid
         * @param pos   position of the single value entry
         * @return success or failure
         */
        private boolean expandSingleValueToIntervalLeftSide(final long p_lid, final int pos) {
            long posEntry = m_ringBufferSpareLocalIDs[pos];
            long intervalMetaData = SpareLocalID.getEntryInfo(posEntry);
            intervalMetaData = intervalMetaData < 0 ? SpareLocalID.getSingleEntryIntervalSize(posEntry) : intervalMetaData;

            if (intervalMetaData > 1) {
                //single entry interval
                //check first if interval has enough space
                if (intervalMetaData + 1 > MAX_INTERVAL_SIZE) {
                    //not enough! move one and create a two entry interval
                    boolean ret = moveElementsOneField(pos);
                    if (ret) {
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(true, p_lid);
                        m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, p_lid + intervalMetaData);

                    }
                    return ret;
                } else {
                    m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(p_lid, intervalMetaData + 1);
                    return true;

                }
            } else if (intervalMetaData == 1) {
                m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(true, p_lid);
                return true;

            } else {
                m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(p_lid, 2);
                return true;
            }
        }

        public boolean breakEntry(final long p_lid, final int pos) {
            boolean ret;
            long posEntry = m_ringBufferSpareLocalIDs[pos];
            long intervalMetaData = SpareLocalID.getEntryInfo(posEntry);
            intervalMetaData = intervalMetaData < 0 ? SpareLocalID.getSingleEntryIntervalSize(posEntry) : intervalMetaData;
            long leftBorderID = SpareLocalID.getLID(posEntry);
            long rightBorderID = leftBorderID + intervalMetaData - 1;

            if (intervalMetaData > 1) {
                if (leftBorderID == p_lid) {
                    m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(leftBorderID + 1, rightBorderID);
                    return true;
                } else if (rightBorderID == p_lid) {
                    m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(leftBorderID, rightBorderID);
                    return true;

                }
                ret = moveElementsOneField(pos);
                if (ret) {
                    if (rightBorderID - leftBorderID - 1 == 1) {
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(false, leftBorderID);
                        m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(false, p_lid);

                    } else {
                        if (p_lid - leftBorderID == 1) {
                            m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(false, leftBorderID);
                            m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSingleEntryIntervalID(p_lid + 1, rightBorderID - 1);
                        } else if (rightBorderID - p_lid == 1) {
                            m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(leftBorderID, p_lid - leftBorderID);
                            m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(false, rightBorderID);
                        } else {
                            m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(leftBorderID, p_lid - leftBorderID);
                            ;
                            m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSingleEntryIntervalID(p_lid + 1, rightBorderID - p_lid);
                            ;
                        }
                    }
                } else {
                    LOGGER.error("Moving elements went wrong");
                }
            } else if (intervalMetaData == 1) {
                long nextPosEntry = m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length];
                rightBorderID = SpareLocalID.getLID(nextPosEntry);

                long intervalLength = rightBorderID - leftBorderID;
                if (intervalLength - 1 <= MAX_SINGLE_VALUE_INTERVAL_SIZE) {
                    //no need to move
                    if (leftBorderID == p_lid) {
                        moveElementsOneFieldBack(pos);
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(leftBorderID + 1, MAX_SINGLE_VALUE_INTERVAL_SIZE);
                        return true;
                    } else if (rightBorderID == p_lid) {
                        moveElementsOneFieldBack(pos);
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(leftBorderID, MAX_SINGLE_VALUE_INTERVAL_SIZE);
                        return true;
                    } else {
                        //p_lid not the borders
                        m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(leftBorderID, p_lid - leftBorderID);
                        m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSingleEntryIntervalID(p_lid + 1, intervalLength - p_lid - leftBorderID);
                        return true;
                    }
                } else {
                    //break double entry interval
                    long leftIntervalSize = p_lid - leftBorderID;
                    long rightIntervalSize = intervalLength - p_lid - leftBorderID;

                    if (leftIntervalSize == 1) {
                        ret = moveElementsOneField((pos + 1) % m_ringBufferSpareLocalIDs.length);
                        if (ret) {
                            m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(false, leftBorderID);
                            m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, p_lid + 1);
                            m_ringBufferSpareLocalIDs[(pos + 2) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, rightBorderID);
                        }
                        return ret;
                    }
                    if (rightIntervalSize == 1) {
                        ret = moveElementsOneField((pos + 1) % m_ringBufferSpareLocalIDs.length);
                        if (ret) {
                            m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(true, leftBorderID);
                            m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, p_lid - 1);
                            m_ringBufferSpareLocalIDs[(pos + 2) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(false, rightBorderID);
                        }
                        return ret;
                    }
                    if (leftIntervalSize > MAX_SINGLE_VALUE_INTERVAL_SIZE) {
                        if (rightIntervalSize > MAX_SINGLE_VALUE_INTERVAL_SIZE) {
                            moveElementsOneField(pos);
                            ret = moveElementsOneField(pos);
                            if (ret) {
                                m_ringBufferSpareLocalIDs[(pos + 2) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, p_lid + 1);
                                m_ringBufferSpareLocalIDs[(pos + 3) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, rightBorderID);
                            }
                        } else {
                            ret = moveElementsOneField(pos);
                            if (ret) {
                                m_ringBufferSpareLocalIDs[(pos + 2) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSingleEntryIntervalID(p_lid + 1, rightIntervalSize);

                            }
                        }
                        if (ret) {
                            m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSpareLocalID(true, leftBorderID);
                            m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, p_lid - 1);
                        }
                        return ret;
                    } else {
                        if (rightIntervalSize > MAX_SINGLE_VALUE_INTERVAL_SIZE) {
                            ret = moveElementsOneField(pos);
                            if (ret) {
                                m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, p_lid + 1);
                                m_ringBufferSpareLocalIDs[(pos + 2) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSpareLocalID(true, rightBorderID);
                            }
                        } else {
                            m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = SpareLocalID.getSingleEntryIntervalID(p_lid + 1, rightIntervalSize);
                            ret = true;
                        }
                        if (ret) {
                            m_ringBufferSpareLocalIDs[pos] = SpareLocalID.getSingleEntryIntervalID(leftBorderID, leftIntervalSize);
                        }

                    }

                }
                ret = false;
            } else {
                moveElementsOneFieldBack((pos + 1) % m_ringBufferSpareLocalIDs.length);
                ret = true;
            }
            if (ret) {
                m_freeLIDs -= 1;

            }

            return ret;
        }

        /**
         * Checks for the lower lid for the current entry if it can be inserted there
         *
         * @param key          lower lid
         * @param entryPointer entry position
         * @return 0: nothing can extended
         * 1: entry can be extended left side with key
         * 2: entry can be extended right side with key
         * 3: entry can be combined right side with key and right neighbour
         */
        private int getExpandTypeOfPosition(long key, int entryPointer) {
            long entry = m_ringBufferSpareLocalIDs[entryPointer];
            long intervalMetaData = SpareLocalID.getEntryInfo(entry);
            intervalMetaData = intervalMetaData < 0 ? SpareLocalID.getSingleEntryIntervalSize(entry) : intervalMetaData;
            long entryID = SpareLocalID.getLID(entry);
            int nextLeftPointer = entryPointer == 0 ? 0 : (entryPointer - 1) % m_ringBufferSpareLocalIDs.length;
            long nextLeftEntry = m_ringBufferSpareLocalIDs[nextLeftPointer];
            long nextRightEntry = m_ringBufferSpareLocalIDs[(entryPointer + 1) % m_ringBufferSpareLocalIDs.length];
            long leftBorder = -1;
            long rightBorder = -1;

            if (intervalMetaData > 1) {
                //single value interval
                leftBorder = SpareLocalID.getLID(entry);
                rightBorder = leftBorder + intervalMetaData - 1;

            } else if (intervalMetaData == 1) {
                //two entry interval
                leftBorder = SpareLocalID.getLID(entry);
                rightBorder = SpareLocalID.getLID(m_ringBufferSpareLocalIDs[(entryPointer + 1) % m_ringBufferSpareLocalIDs.length]);
                nextRightEntry = m_ringBufferSpareLocalIDs[(entryPointer + 2) % m_ringBufferSpareLocalIDs.length];
            } else if (intervalMetaData == 0) {
                //single value
                leftBorder = entryID;
                rightBorder = entryID;


            }
            if (key >= leftBorder && key <= rightBorder) {
                return 5;
            }
            return checkNeighboursForExpand(key, leftBorder, rightBorder, nextLeftEntry, nextRightEntry);

        }

        /**
         * Checks the left and right neighbour of an entry in the buffer depending of key. Checking includes either the
         * entry can extended with only the key or a neighbour
         *
         * @param key            inserted lower lid
         * @param leftBorderID   lid of left border of current entry
         * @param rightBorderID  lid of right border of current entry
         * @param nextLeftEntry  left neighbour entry
         * @param nextRightEntry right neighbour entry
         * @return 0: nothing can extended
         * 1: entry can be extended left side with key
         * 2: entry can be extended right side with key
         * 3: entry can be combined right side with key and right neighbour
         * 4: entry can be written as single value left side
         */
        private int checkNeighboursForExpand(long key, long leftBorderID, long rightBorderID, long nextLeftEntry, long nextRightEntry) {
            long intervalMetaData = SpareLocalID.getEntryInfo(nextLeftEntry);
            intervalMetaData = intervalMetaData < 0 ? SpareLocalID.getSingleEntryIntervalSize(nextLeftEntry) : intervalMetaData;

            long nextLeftEntryID = intervalMetaData > 1 ? SpareLocalID.getLID(nextLeftEntry) + intervalMetaData - 1 : SpareLocalID.getLID(nextLeftEntry);
            long nextRightEntryID = SpareLocalID.getLID(nextRightEntry);

            if (leftBorderID - 1 == key && rightBorderID - leftBorderID - 1 != 1) {
                if (nextLeftEntryID == 0) {
                    //left entry is empty
                    return 1;
                } else {
                    if (nextLeftEntryID == key - 1) {
                        //can be ignored will be handled either with 1 oder 3
                        return 0;
                    } else {
                        //just extend current entry with key
                        return 1;

                    }
                }
            } else if (rightBorderID + 1 == key && rightBorderID - leftBorderID - 1 != 1) {
                if (nextRightEntryID == 0) {
                    //right entry is empty
                    return 2;

                } else {
                    if (nextRightEntryID == key + 1) {
                        //right border of the next left entry is next value of key (key -1) -> combine with left entry
                        return 3;

                    } else {
                        //just extend current entry with key
                        return 2;

                    }
                }
            }
            //now check if it should saved as single value left or right
            if (key > nextLeftEntryID && key < leftBorderID) {
                return 4;
            }

            return 0;
        }

        /**
         * Pushes all elements from pos one field
         *
         * @param pos starting position
         * @return True, if enough space is free. False, when buffer is full
         */
        private boolean moveElementsOneField(final int pos) {
            boolean ret;
            if (m_count + 1 < m_ringBufferSpareLocalIDs.length) {
                int til = m_putPosition;
                int bufferSize = m_ringBufferSpareLocalIDs.length;
                long p_lastLID = m_ringBufferSpareLocalIDs[pos];
                int currPointer = (pos + 1) % bufferSize;

                do {
                    long tmp = m_ringBufferSpareLocalIDs[currPointer];
                    m_ringBufferSpareLocalIDs[currPointer] = p_lastLID;
                    p_lastLID = tmp;
                    currPointer = (currPointer + 1) % bufferSize;

                } while (currPointer != til + 1);

                m_count++;
                ret = true;

            } else {
                LOGGER.error("Moving forewards failed!");
                ret = false;
            }
            m_overallCount++;
            return ret;
        }

        /**
         * Moves all elements from m_putPosition one field back to til (exclusive)
         *
         * @param til border
         */
        private void moveElementsOneFieldBack(final int til) {
            int bufferSize = m_ringBufferSpareLocalIDs.length;
            int currPointer = (m_putPosition - 1) % bufferSize;
            long p_lastLID = m_ringBufferSpareLocalIDs[currPointer];
            m_ringBufferSpareLocalIDs[currPointer] = 0;
            currPointer = (currPointer - 1) % bufferSize;
            if (currPointer != -1) {
                do {

                    long tmp = m_ringBufferSpareLocalIDs[currPointer];
                    m_ringBufferSpareLocalIDs[currPointer] = p_lastLID;
                    p_lastLID = tmp;
                    currPointer = (currPointer - 1) % bufferSize;
                } while (currPointer != til - 1);
            }
            m_putPosition = (m_putPosition - 1) % bufferSize;
            m_count--;
            m_overallCount--;
        }


        /**
         * Refill the store. This calls a deep search for zombie entries in the CIDTable
         *
         * @return True if zombie entries were found, false if none were found
         */
        private boolean refillStore() {
            return m_cidTable.getAndEliminateZombies(m_ownNodeId, m_ringBufferSpareLocalIDs, m_putPosition, m_getPosition,
                    m_ringBufferSpareLocalIDs.length - m_count) > 0;
        }

        /**
         * Moves the Put-Pointer
         *
         * @param p_step Number of steps to move
         */
        private void movePutPointer(final int p_step) {
            m_putPosition = (m_putPosition + p_step) % m_ringBufferSpareLocalIDs.length;
        }

        private void undoPuts(final int p_numberOfUndos) {
            m_ringBufferLock.lock();

            movePutPointer(-1 * p_numberOfUndos);

            m_ringBufferLock.unlock();
        }

        /**
         * Moves the Get-Pointer
         *
         * @param p_step Number of steps to move
         */
        private void moveGetPointer(final int p_step) {
            m_getPosition = (m_getPosition + p_step) % m_ringBufferSpareLocalIDs.length;
        }


        //--------------------------------------VALIDITATION-PRINTS----------------------------------------------------
        public void printRingBufferSpareLocalIDs() {
            LOGGER.trace("Logging Ring Buffer");
            for (int i = 0; i < m_count; i++) {
                int pos = i;//(m_putPosition - i) % m_ringBufferSpareLocalIDs.length;
                long value = m_ringBufferSpareLocalIDs[pos];
                long intervalInfo = SpareLocalID.getEntryInfo(m_ringBufferSpareLocalIDs[pos]);
                intervalInfo = intervalInfo < 0 ? SpareLocalID.getSingleEntryIntervalSize(m_ringBufferSpareLocalIDs[pos]) : intervalInfo;
                long id = SpareLocalID.getLID(m_ringBufferSpareLocalIDs[pos]);
                LOGGER.debug("Position: %d, Value: %d, Intervalinfo: %d, id: %d", pos, value, intervalInfo, id);
            }
            LOGGER.debug("M_freeLIDs: %d", m_freeLIDs);
            LOGGER.debug("m_count: %d", m_count);
            LOGGER.debug("m_overallCount: %d", m_overallCount);
        }

        public void writeRingBufferSpareLocalIDs() {
            LOGGER.trace("Logging Ring Buffer");
            try {
                BufferedWriter br = new BufferedWriter(new FileWriter("/home/voelz/ringBufferPrint5.txt"));
                for (int i = 0; i < m_count; i++) {
                    int pos = (m_putPosition - i) % m_ringBufferSpareLocalIDs.length;
                    long value = m_ringBufferSpareLocalIDs[pos];
                    long intervalInfo = m_ringBufferSpareLocalIDs[pos] >> 48;
                    intervalInfo = intervalInfo < 0 ? SpareLocalID.getSingleEntryIntervalSize(m_ringBufferSpareLocalIDs[pos]) : intervalInfo;
                    long id = SpareLocalID.getLID(m_ringBufferSpareLocalIDs[pos]);
                    br.write(String.format("Position: %d, Value: %d, isIntervall: %d, id: %d", pos, value, intervalInfo, id));
                    br.newLine();
                    br.flush();
                }
                br.write(String.format("M_freeLIDs: %d", m_freeLIDs));
                br.newLine();
                br.write(String.format("Put_Pos: %d", m_putPosition));
                br.newLine();
                br.write(String.format("Get_Pos: %d", m_getPosition));
                br.newLine();
                br.write(String.format("Overallcount: %d", m_overallCount));
                br.newLine();
                br.write(String.format("count: %d", m_count));
                br.flush();
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        /**
         * Getter
         * Only for testing
         *
         * @return the ring buffer, where unused local-ids are stored
         */
        public long[] getRingBufferSpareLocalIDs() {
            return m_ringBufferSpareLocalIDs;
        }
        //--------------------------------------EXPORT/IMPORT--------------------------------------------------------

        @Override
        public void exportObject(final Exporter p_exporter) {
            p_exporter.writeLongArray(m_ringBufferSpareLocalIDs);
            p_exporter.writeInt(m_getPosition);
            p_exporter.writeInt(m_putPosition);
            p_exporter.writeInt(m_count);
            p_exporter.writeLong(m_overallCount);
        }

        @Override
        public void importObject(final Importer p_importer) {
            m_ringBufferSpareLocalIDs = p_importer.readLongArray(m_ringBufferSpareLocalIDs);
            m_getPosition = p_importer.readInt(m_getPosition);
            m_putPosition = p_importer.readInt(m_putPosition);
            m_count = p_importer.readInt(m_count);
            m_overallCount = p_importer.readLong(m_overallCount);
        }

        @Override
        public int sizeofObject() {
            return ObjectSizeUtil.sizeofLongArray(m_ringBufferSpareLocalIDs) + Integer.BYTES * 3 + Long.BYTES;
        }


    }
}
