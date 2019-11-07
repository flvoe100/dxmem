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
import de.hhu.bsinfo.dxutils.serialization.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Stores free LocalIDs
 *
 * @author Florian Klein 30.04.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public final class LIDStore implements Importable, Exportable {
    private static final int STORE_CAPACITY = 10_000_000;
    private static final long LOCALID_BITMASK = 0x0000FFFFFFFFFFFFL;
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
            // getM_spareLIDStore().printRingBufferSpareLocalIDs();
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
        boolean ret = true;
        for (int i = 0; i < p_count; i++) {

            if (!this.put(p_lids[p_offset + i])) {
                ret = false;
            }
            ;
        }
        return ret;
    }

    public boolean put(final long p_lid) {
        assert p_lid <= ChunkID.MAX_LOCALID;
        long p_currentLocalIDCounter = m_localIDCounter.get();

        if (p_lid < p_currentLocalIDCounter) {
            //FIXME: we do not need the information if it is a remove. Just change getLIDPosition in such way, that its gives the position where it should inserted.
            // Then insert it there. Two possibilities: 1. Extend an interval 2. Insert a single value and move all elements one field
            int p_lidPos = m_spareLIDStore.getLIDPosition(p_lid);

            if (p_lidPos != -1) {
                LOGGER.trace("Putting lid is valid because its lid is free");
                //TODO: Create new function to insert
                //
                m_spareLIDStore.putLowerLID(p_lid, p_lidPos);
                //m_spareLIDStore.breakInterval(p_lidPos, p_lid);

                return true;
            }
            //TODO:

            LOGGER.error("Putting lid is unvalid because its lid is not free");

            return false;
        } else if (p_lid > p_currentLocalIDCounter) {
            //put lid into spare until p_lid
            if (p_currentLocalIDCounter == p_lid - 1) {
                m_spareLIDStore.put(m_localIDCounter.getAndSet(p_lid + 1));
                return true;
            }
            if (p_lid - p_currentLocalIDCounter == 2) {
                //do not save intervals with length 2, just 2 single values
                m_spareLIDStore.put(m_localIDCounter.getAndIncrement());
                m_spareLIDStore.put(m_localIDCounter.getAndIncrement());
                m_localIDCounter.incrementAndGet();
                return true;
            }

            m_spareLIDStore.putInterval(m_localIDCounter.get(), p_lid - 1);

            m_localIDCounter.set(p_lid + 1);
            return true;
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
                    boolean isInterval = (ret >> 48) == 1;
                    if (isInterval) {
                        long startIntervalId = ret & LOCALID_BITMASK;
                        long endIntervalId = m_ringBufferSpareLocalIDs[(m_getPosition + 1) % m_ringBufferSpareLocalIDs.length] & LOCALID_BITMASK;
                        ret = startIntervalId;
                        if (startIntervalId + 1 == endIntervalId) {
                            //only one element left
                            m_ringBufferSpareLocalIDs[m_getPosition] = 0;
                            m_ringBufferSpareLocalIDs[(m_getPosition + 1) % m_ringBufferSpareLocalIDs.length] = getSparseLocalID(false, endIntervalId);
                            m_count--;
                            m_overallCount--;
                            m_freeLIDs--;
                            m_getPosition = (m_getPosition + 1) % m_ringBufferSpareLocalIDs.length;
                        } else {
                            //just shrink interval
                            m_freeLIDs--;
                            m_ringBufferSpareLocalIDs[m_getPosition] = getSparseLocalID(true, startIntervalId + 1);
                        }
                    } else {
                        //just return single value

                        ret = ret & LOCALID_BITMASK;
                        m_count--;
                        m_overallCount--;
                        m_freeLIDs--;
                        m_getPosition = (m_getPosition + 1) % m_ringBufferSpareLocalIDs.length;
                    }
                }
                m_ringBufferLock.unlock();
            }
            return ret;
        }

        public long get(final long p_lid) {
            long ret = -1;

            if (m_overallCount > 0) {
                m_ringBufferLock.lock();

                if (m_count == 0 && m_overallCount > 0) {
                    // ignore return value
                    refillStore();
                }

                if (m_count > 0) {

                    int p_currentPos = m_getPosition;
                    while (p_currentPos > 0) {
                        long p_localSpareLocalID = m_ringBufferSpareLocalIDs[p_currentPos];
                        if (p_localSpareLocalID == p_lid) {
                            break;
                        }
                        p_currentPos = (p_currentPos - 1) % m_ringBufferSpareLocalIDs.length;
                    }

                    m_getPosition = (m_getPosition + 1) % m_ringBufferSpareLocalIDs.length;
                    m_freeLIDs--;
                    m_count--;
                    m_overallCount--;
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
                    boolean isInterval = (currSparseEntry >> 48) == 1;
                    if (isInterval) {
                        long startIntervalId = currSparseEntry & LOCALID_BITMASK;
                        long endIntervalId = m_ringBufferSpareLocalIDs[(m_getPosition + 1) % m_ringBufferSpareLocalIDs.length] & LOCALID_BITMASK;
                        long intervalSize = endIntervalId - startIntervalId + 1;

                        if (intervalSize > restCount) {
                  //just shrink
                            for (int i = 0; i < restCount; i++) {

                                p_lids[p_offset + i + counter] = startIntervalId + i;
                            }
                            if (intervalSize - restCount == 1) {
                                //only one value left
                                m_getPosition = (m_getPosition + 1) % m_ringBufferSpareLocalIDs.length;
                                m_ringBufferSpareLocalIDs[m_getPosition] = getSparseLocalID(false, endIntervalId);
                                m_count--;
                                m_overallCount--;
                            } else if (intervalSize - restCount == 2) {
                                //two single values left
                                m_ringBufferSpareLocalIDs[m_getPosition] = getSparseLocalID(false, startIntervalId + restCount);
                                m_ringBufferSpareLocalIDs[(m_getPosition + 1) % m_ringBufferSpareLocalIDs.length] = getSparseLocalID(false, endIntervalId);
                            } else {
                                //interval just shrinks left site
                                m_ringBufferSpareLocalIDs[m_getPosition] = getSparseLocalID(true, startIntervalId + restCount);
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
                            m_overallCount -= 2;
                            m_freeLIDs -= intervalSize;

                            m_getPosition = (m_getPosition + 2) % m_ringBufferSpareLocalIDs.length;
                        }

                    } else {
                        //just return single value
                        p_lids[p_offset + counter] = currSparseEntry & LOCALID_BITMASK;
                        restCount--;
                        counter++;
                        m_count--;
                        m_overallCount--;
                        m_freeLIDs--;
                        m_getPosition = (m_getPosition + 1) % m_ringBufferSpareLocalIDs.length;
                    }

                } while (counter != p_count && restCount <= m_freeLIDs && (m_overallCount > 0 || m_count > 0));
                m_ringBufferLock.unlock();
            }

            return counter;
        }

        /**
         * Refill the store. This calls a deep search for zombie entries in the CIDTable
         *
         * @return True if zombie entries were found, false if none were found
         */
        private boolean refillStore() {
            return m_cidTable.getAndEliminateZombies(m_ownNodeId, m_ringBufferSpareLocalIDs, m_putPosition,
                    m_ringBufferSpareLocalIDs.length - m_count) > 0;
        }

        public boolean putLowerLID(final long p_lid, final int p_pos) {

            m_ringBufferLock.lock();
            long p_sparseLidOfPos = m_ringBufferSpareLocalIDs[p_pos];
            boolean isInterval = p_sparseLidOfPos >> 48 != 0;
            long p_lidOfPos = p_sparseLidOfPos & LOCALID_BITMASK;
            if (!isInterval) {

                return true;
            }
            //extend interval
            long p_nextSparseLid = m_ringBufferSpareLocalIDs[(p_pos + 1) % m_ringBufferSpareLocalIDs.length];
            boolean isNextInterval = p_nextSparseLid >> 48 != 0;
            long p_nextLid = p_nextSparseLid & LOCALID_BITMASK;


            if (p_lidOfPos - 1 == p_lid) {
                m_ringBufferSpareLocalIDs[p_pos] = getSparseLocalID(true, p_lid);
                return true;
            }
            if (p_nextLid + 1 == p_lid) {
                m_ringBufferSpareLocalIDs[(p_pos + 1) % m_ringBufferSpareLocalIDs.length] = getSparseLocalID(true, p_lid);
            }

            m_ringBufferLock.unlock();
            return true;

        }


        public boolean putInterval(final long p_startLid, final long p_endLid) {
            boolean ret;
            // System.out.println("m_putPosition = " + m_putPosition);
            m_ringBufferLock.lock();
            if (m_count + 2 < m_ringBufferSpareLocalIDs.length) {

                m_ringBufferSpareLocalIDs[m_putPosition] = getSparseLocalID(true, p_startLid);
                m_count++;
                m_putPosition = (m_putPosition + 1) % m_ringBufferSpareLocalIDs.length;
                m_ringBufferSpareLocalIDs[m_putPosition] = getSparseLocalID(true, p_endLid);
                m_count++;
                m_putPosition = (m_putPosition + 1) % m_ringBufferSpareLocalIDs.length;
                m_freeLIDs += p_endLid - p_startLid + 1;

                ret = true;
            } else {
                ret = false;
            }
            m_overallCount += 2;
            m_ringBufferLock.unlock();
            return ret;
        }

        public boolean breakInterval(final int p_startPos, final long p_lid) {
            boolean ret;
            m_ringBufferLock.lock();
            //check space
            long p_startLid = m_ringBufferSpareLocalIDs[p_startPos] & LOCALID_BITMASK;
            long p_endLid = m_ringBufferSpareLocalIDs[(p_startPos + 1) % m_ringBufferSpareLocalIDs.length] & LOCALID_BITMASK;
            assert p_lid >= p_startLid && p_lid <= p_endLid;

            if (p_startLid == p_lid) {//left border
                m_ringBufferSpareLocalIDs[p_startPos] = getSparseLocalID(true, p_startLid + 1);
                ret = true;
            } else if (p_endLid == p_lid) {//right border
                m_ringBufferSpareLocalIDs[(p_startPos + 1) % m_ringBufferSpareLocalIDs.length] = getSparseLocalID(true, p_endLid - 1);
                ret = true;
            } else {//in between
                int p_numberOfFieldMoves = p_lid == p_startLid + 1 || p_lid == p_endLid - 1 ? 1 : 2;
                if (m_count + p_numberOfFieldMoves < m_ringBufferSpareLocalIDs.length) {
                    //first push all entries from end two field
                    if (p_numberOfFieldMoves == 2) {
                        moveElementsTwoFields(p_startPos, p_lid);
                    } else {
                        moveElementsOneField(p_startPos, p_lid);
                    }
                    ret = true;

                } else {
                    ret = false;

                }
            }
            m_ringBufferLock.unlock();

            return ret;
        }

        private void moveElementsOneField(int pos, long p_lid) {
            int pushPointer = (pos + 1) % m_ringBufferSpareLocalIDs.length;
            long p_lastLID = m_ringBufferSpareLocalIDs[pushPointer];
            do {
                pushPointer = (pushPointer + 1) % m_ringBufferSpareLocalIDs.length;
                long tmp = m_ringBufferSpareLocalIDs[pushPointer];
                m_ringBufferSpareLocalIDs[pushPointer] = p_lastLID;

                p_lastLID = tmp;
            } while (pushPointer <= m_putPosition);
            m_putPosition = (m_putPosition + 1) % m_ringBufferSpareLocalIDs.length;
            m_count++;
            if ((m_ringBufferSpareLocalIDs[pos] & LOCALID_BITMASK) + 1 == p_lid) {
                m_ringBufferSpareLocalIDs[pos] = getSparseLocalID(false, m_ringBufferSpareLocalIDs[pos] & LOCALID_BITMASK);
                m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = getSparseLocalID(true, p_lid + 1);

            } else {
                m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = getSparseLocalID(true, p_lid - 1);
                m_ringBufferSpareLocalIDs[(pos + 2) % m_ringBufferSpareLocalIDs.length] = getSparseLocalID(false, m_ringBufferSpareLocalIDs[(pos + 2) % m_ringBufferSpareLocalIDs.length] & LOCALID_BITMASK);

            }
        }

        private void moveElementsTwoFields(int pos, long p_lid) {
            int pushPointer = (m_putPosition - 1) % m_ringBufferSpareLocalIDs.length;
            long startIntervalId = m_ringBufferSpareLocalIDs[pos] & LOCALID_BITMASK;
            long endIntervalId = m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] & LOCALID_BITMASK;
            long leftIntervalSize = p_lid - startIntervalId;
            long rightIntervalSize = endIntervalId - p_lid;

            do {

                m_ringBufferSpareLocalIDs[pushPointer + 2] = m_ringBufferSpareLocalIDs[pushPointer];
                pushPointer = (pushPointer - 1) % m_ringBufferSpareLocalIDs.length;

            } while (pushPointer > pos);
            m_putPosition = (m_putPosition + 2) % m_ringBufferSpareLocalIDs.length;
            m_count += 2;
            m_ringBufferSpareLocalIDs[pos] = getSparseLocalID(!(leftIntervalSize == 2), m_ringBufferSpareLocalIDs[pos] & LOCALID_BITMASK);
            m_ringBufferSpareLocalIDs[(pos + 1) % m_ringBufferSpareLocalIDs.length] = getSparseLocalID(!(leftIntervalSize == 2), p_lid - 1);

            m_ringBufferSpareLocalIDs[(pos + 2) % m_ringBufferSpareLocalIDs.length] = getSparseLocalID(!(rightIntervalSize == 2), p_lid + 1);
            m_ringBufferSpareLocalIDs[(pos + 3) % m_ringBufferSpareLocalIDs.length] = getSparseLocalID(!(rightIntervalSize == 2), m_ringBufferSpareLocalIDs[(pos + 3) % m_ringBufferSpareLocalIDs.length] & LOCALID_BITMASK);
        }

        public void printRingBufferSpareLocalIDs() {
            LOGGER.trace("Logging Ring Buffer");
            for (int i = 1; i < m_count + 1; i++) {
                LOGGER.debug("Position: %d, Value: %d, isIntervall: %d, id: %d", (m_putPosition - i) % m_ringBufferSpareLocalIDs.length, m_ringBufferSpareLocalIDs[(m_putPosition - i) % m_ringBufferSpareLocalIDs.length], m_ringBufferSpareLocalIDs[(m_putPosition - i) % m_ringBufferSpareLocalIDs.length] >> 48, m_ringBufferSpareLocalIDs[(m_putPosition - i) % m_ringBufferSpareLocalIDs.length] & LOCALID_BITMASK);
            }
            LOGGER.debug("M_freeLIDs: %d", m_freeLIDs);
        }

        public void writeRingBufferSpareLocalIDs() {
            LOGGER.trace("Logging Ring Buffer");
            try {
                BufferedWriter br = new BufferedWriter(new FileWriter("/home/vlz/ringBufferPrint4.txt"));
                for (int i = 1; i < m_count + 1; i++) {
                    br.write(String.format("Position: %d, Value: %d, isIntervall: %d, id: %d", (m_putPosition - i) % m_ringBufferSpareLocalIDs.length, m_ringBufferSpareLocalIDs[(m_putPosition - i) % m_ringBufferSpareLocalIDs.length], m_ringBufferSpareLocalIDs[(m_putPosition - i) % m_ringBufferSpareLocalIDs.length] >> 48, m_ringBufferSpareLocalIDs[(m_putPosition - i) % m_ringBufferSpareLocalIDs.length] & LOCALID_BITMASK));
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
         * Put a LID to the store
         *
         * @param p_lid LID to add to the store
         * @return True if successful, false if store is full (callers has to treat chunk as a zombie)
         */
        public boolean put(final long p_lid) {
            boolean ret;
            m_ringBufferLock.lock();
            if (m_count + 1 < m_ringBufferSpareLocalIDs.length) {
                m_ringBufferSpareLocalIDs[m_putPosition] = getSparseLocalID(false, p_lid);
                m_count++;
                m_overallCount++;
                m_freeLIDs++;
                m_putPosition = (m_putPosition + 1) % m_ringBufferSpareLocalIDs.length;

                ret = true;
            } else {
                ret = false;
            }
            m_ringBufferLock.unlock();
            return ret;
        }

        /**
         * Put a LID to the store
         *
         * @param p_lids LID to add to the store
         * @return True if successful, false if store is full (callers has to treat chunk as a zombie)
         */
        public boolean put(final long[] p_lids) {
            boolean ret;

            m_ringBufferLock.lock();

            if (m_count + p_lids.length - 1 < m_ringBufferSpareLocalIDs.length) {
                for (long p_lid : p_lids) {
                    m_ringBufferSpareLocalIDs[m_putPosition] = p_lid;
                    m_putPosition = (m_putPosition + 1) % m_ringBufferSpareLocalIDs.length;
                    m_count++;
                }
                ret = true;
            } else {
                ret = false;
            }

            m_overallCount++;

            m_ringBufferLock.unlock();
            return ret;
        }


        public int getLIDPosition(final long p_lid) {
            if (m_overallCount > 0) {
                m_ringBufferLock.lock();
                if (m_count == 0 && m_overallCount > 0) {
                    // ignore return value
                    refillStore();
                }
                //TODO: extend to get Position when its not in an interval

                if (m_count > 0) {
                    int p_currentPos = m_getPosition;
                    long p_lastID = -1;
                    int p_lastIDPos = -1;
                    //TODO: Extend to binary search?

                    do {
                        long p_localSpareLocalID = m_ringBufferSpareLocalIDs[p_currentPos];
                        boolean p_isInterval = p_localSpareLocalID >> 48 != 0;
                        long p_curr_lid = p_localSpareLocalID & LOCALID_BITMASK;

                        if (p_lastID != -1 && (p_lid + 1 > p_lastID && p_lid < p_curr_lid - 1)) {
                            assert p_curr_lid - p_lastID > 1;
                            return p_lastIDPos;
                        }

                        if (p_isInterval) {
                            long p_endIntervalLid = m_ringBufferSpareLocalIDs[(p_currentPos + 1) % m_ringBufferSpareLocalIDs.length];
                            long p_endRealID = p_endIntervalLid & LOCALID_BITMASK;
                            if (p_lastID + 1 == p_lid && p_lid + 1 == p_curr_lid) {
                                return p_lastIDPos;
                            }

                            if (p_lid >= p_curr_lid - 1 && p_lid <= p_endRealID + 1) {
                                return p_currentPos;
                            }
                            p_lastID = p_endRealID;
                            p_lastIDPos = (p_currentPos + 1) % m_ringBufferSpareLocalIDs.length;
                        }

                        if (p_curr_lid == p_lid) {
                            return p_currentPos;
                        }


                        if (p_isInterval) {
                            p_currentPos = (p_currentPos + 1) % m_ringBufferSpareLocalIDs.length;
                        } else {
                            p_lastID = p_curr_lid;
                            p_lastIDPos = p_currentPos;
                        }
                        p_currentPos = (p_currentPos + 1) % m_ringBufferSpareLocalIDs.length;
                    } while (p_currentPos < m_putPosition);
                }
                m_ringBufferLock.unlock();
            }
            return -1;
        }

        private long getSparseLocalID(boolean isInterval, long lid) {
            return (long) (isInterval ? (short) 1 : (short) 0) << 48 | lid & LOCALID_BITMASK;

        }

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
