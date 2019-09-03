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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.LongStream;

/**
 * Stores free LocalIDs
 *
 * @author Florian Klein 30.04.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public final class LIDStore implements Importable, Exportable {
    private static final int STORE_CAPACITY = 100000;
    private static final long LOCALID_BITMASK = 0x0000FFFFFFFFFFFFL;

    private SpareLIDStore m_spareLIDStore;
    private AtomicLong m_localIDCounter;

    /**
     * Constructor for importing from memory dump
     */
    LIDStore() {
        m_spareLIDStore = new SpareLIDStore();
        m_localIDCounter = new AtomicLong(0);
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

    public void insert(final long p_lid) {
        assert p_lid <= ChunkID.MAX_LOCALID;

        if (p_lid < m_localIDCounter.get()) {
            if (m_spareLIDStore.containsLID(p_lid)) {
                //get the specific lid out of store
                //TODO
                m_spareLIDStore.get(p_lid);
            }
            //error lid is in use!
        } else if (p_lid > m_localIDCounter.get()) {
            //put lid into spare until p_lid
            long[] p_sparseLocalIDs = LongStream.range(m_localIDCounter.get(), p_lid).toArray();
            m_spareLIDStore.put(p_sparseLocalIDs);
            get();
        } else {
            get();
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

    /**
     * Puts a free LID back
     *
     * @param p_lid LID to put back
     * @return True if adding an entry to store was successful, false otherwise (full)
     */
    public boolean put(final long p_lid) {
        return m_spareLIDStore.put(p_lid);
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

                    m_getPosition = (m_getPosition + 1) % m_ringBufferSpareLocalIDs.length;
                    m_count--;
                    m_overallCount--;
                }

                m_ringBufferLock.unlock();
            }

            return ret;
        }

        private long getSparseLocalID(boolean isInterval, long lid) {
            return (long) (isInterval ? (short) 1 : (short) 0) << 48 | lid & LOCALID_BITMASK;
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
                    m_count--;
                    m_overallCount--;
                }

                m_ringBufferLock.unlock();
            }

            return ret;
        }

        public boolean containsLID(final long lid) {
            if (m_overallCount > 0) {
                m_ringBufferLock.lock();
                if (m_count == 0 && m_overallCount > 0) {
                    // ignore return value
                    refillStore();
                }

                if (m_count > 0) {
                    int p_currentPos = m_getPosition;
                    do {
                        long p_localSpareLocalID = m_ringBufferSpareLocalIDs[p_currentPos];
                        if (p_localSpareLocalID == lid) {
                            return true;
                        }
                        p_currentPos = (p_currentPos - 1) % m_ringBufferSpareLocalIDs.length;
                    } while (p_currentPos > 0);
                }
                m_ringBufferLock.unlock();
            }
            return false;
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

                while (counter < p_count && (m_overallCount > 0 || m_count > 0)) {
                    if (m_count == 0 && m_overallCount > 0) {
                        // store empty but there are still zombies in the tables
                        refillStore();
                    }

                    if (m_count > 0) {
                        p_lids[p_offset + counter] = m_ringBufferSpareLocalIDs[m_getPosition];

                        m_getPosition = (m_getPosition + 1) % m_ringBufferSpareLocalIDs.length;
                        m_count--;
                        m_overallCount--;

                        counter++;
                    }
                }

                m_ringBufferLock.unlock();
            }

            return counter;
        }

        /**
         * Put a LID to the store
         *
         * @param p_lid LID to add to the store
         * @return True if successful, false if store is full (callers has to treat chunk as a zombie)
         */
        public boolean put(final long p_lid) {
            boolean ret;
            System.out.println("p_lid = " + p_lid);
            m_ringBufferLock.lock();

            if (m_count < m_ringBufferSpareLocalIDs.length) {
                int p_latestPutPosition = (m_putPosition - 1) % m_ringBufferSpareLocalIDs.length;
                if (p_latestPutPosition >= 0) {
                    long diff = p_lid - m_ringBufferSpareLocalIDs[p_latestPutPosition];
                    if (diff > 1) {
                        m_ringBufferSpareLocalIDs[p_latestPutPosition] = getSparseLocalID(true, m_ringBufferSpareLocalIDs[p_latestPutPosition]);
                        m_putPosition = (m_putPosition + 1) % m_ringBufferSpareLocalIDs.length;
                        m_ringBufferSpareLocalIDs[m_putPosition] = getSparseLocalID(false, p_lid);

                    } else {
                        m_ringBufferSpareLocalIDs[m_putPosition] = getSparseLocalID(false, p_lid);
                    }
                } else {
                    //no entry yet -> when interval save 2
                    long diff = p_lid - 0; //0 is initial value
                    if (diff > 1) {
                        //save 1 bit for flag to print if interval or not
                        long startInterval = getSparseLocalID(true, 0);
                        m_ringBufferSpareLocalIDs[m_putPosition] = startInterval;
                        m_putPosition = (m_putPosition + 1) % m_ringBufferSpareLocalIDs.length;
                        m_ringBufferSpareLocalIDs[m_putPosition] = getSparseLocalID(false, p_lid);

                    } else {
                        m_ringBufferSpareLocalIDs[m_putPosition] = getSparseLocalID(false, p_lid);
                    }
                }
                //[m_putPosition] = p_lid;
                m_putPosition = (m_putPosition + 1) % m_ringBufferSpareLocalIDs.length;
                m_count++;

                ret = true;
            } else {
                ret = false;
            }

            m_overallCount++;

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


        /**
         * Refill the store. This calls a deep search for zombie entries in the CIDTable
         *
         * @return True if zombie entries were found, false if none were found
         */
        private boolean refillStore() {
            return m_cidTable.getAndEliminateZombies(m_ownNodeId, m_ringBufferSpareLocalIDs, m_putPosition,
                    m_ringBufferSpareLocalIDs.length - m_count) > 0;
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
