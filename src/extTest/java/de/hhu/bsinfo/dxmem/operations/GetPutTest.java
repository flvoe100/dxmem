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

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemTestUtils;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.TestVertixChunk;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxutils.RandomUtils;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

public class GetPutTest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(GetPutTest.class.getSimpleName());

    @Test
    public void getSimple() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);
        memory.create().create(ds);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        memory.get().get(ds);
        Assert.assertTrue(ds.isStateOk());

        memory.remove().remove(ds);
        Assert.assertTrue(ds.isStateOk());

        memory.shutdown();
    }

    @Test
    public void putGetSimple() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);
        memory.create().create(ds);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        ds.getData()[0] = (byte) 0xAA;

        memory.put().put(ds);
        Assert.assertTrue(ds.isStateOk());

        // clear data
        ds.getData()[0] = 0;

        memory.get().get(ds);
        Assert.assertTrue(ds.isStateOk());
        Assert.assertEquals((byte) 0xAA, ds.getData()[0]);

        memory.remove().remove(ds);
        Assert.assertTrue(ds.isStateOk());

        memory.shutdown();
    }

    @Test
    public void putGetSize2() {
        Configurator.setRootLevel(Level.TRACE);
        putGetSize(DXMemoryTestConstants.CHUNK_SIZE_2);
    }

    @Test
    public void putGetSize3() {
        Configurator.setRootLevel(Level.TRACE);
        putGetSize(DXMemoryTestConstants.CHUNK_SIZE_3);
    }

    @Test
    public void putGetSize4() {
        Configurator.setRootLevel(Level.TRACE);
        putGetSize(DXMemoryTestConstants.CHUNK_SIZE_4);
    }

    @Test
    public void putGetSize5() {
        Configurator.setRootLevel(Level.TRACE);
        putGetSize(DXMemoryTestConstants.CHUNK_SIZE_5);
    }

    @Test
    public void putGetSize6() {
        Configurator.setRootLevel(Level.TRACE);
        putGetSize(DXMemoryTestConstants.CHUNK_SIZE_6);
    }

    @Test
    public void putGetTestChunk1() {
        Configurator.setRootLevel(Level.TRACE);
        putGetTestChunk(1, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);
    }

    @Test
    public void putGetTestChunk2() {
        Configurator.setRootLevel(Level.TRACE);
        putGetTestChunk(10, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);
    }

    @Test
    public void putGetTestChunk3() {
        Configurator.setRootLevel(Level.TRACE);
        putGetTestChunk(100, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);
    }

    @Test
    public void putGetTestChunk4() {
        Configurator.setRootLevel(Level.INFO);
        putGetTestChunk(145050709, DXMemoryTestConstants.HEAP_SIZE_LARGE);
    }

    @Test
    public void putGetTestChunkRandom() {
        Configurator.setRootLevel(Level.TRACE);
        for (int i = 0; i < 20; i++) {
            putGetTestChunk(RandomUtils.getRandomValue(0, 1000), DXMemoryTestConstants.HEAP_SIZE_LARGE);
        }
    }

    private static void putGetSize(final int p_size) {
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID,
                p_size > DXMemoryTestConstants.HEAP_SIZE_SMALL * 0.8 ?
                        (long) ((long) 1024 * 1024 + p_size + p_size * 0.1) :
                        DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(p_size);
        memory.create().create(ds);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        for (int i = 0; i < p_size; i++) {
            ds.getData()[i] = (byte) i;
        }

        memory.put().put(ds);
        Assert.assertTrue(ds.isStateOk());

        // clear data
        for (int i = 0; i < p_size; i++) {
            ds.getData()[i] = 0;
        }

        memory.get().get(ds);
        Assert.assertTrue(ds.isStateOk());

        for (int i = 0; i < p_size; i++) {
            Assert.assertEquals((byte) i, ds.getData()[i]);
        }

        memory.remove().remove(ds);
        Assert.assertTrue(ds.isStateOk());

        memory.shutdown();
    }

    private static void putGetTestChunk(final int p_count, final long p_heapSize) {
        if (!DXMemTestUtils.sufficientMemoryForBenchmark(new StorageUnit(p_heapSize, "b"))) {
            LOGGER.warn("Skipping test due to insufficient memory available");
            return;
        }

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, p_heapSize);

        TestVertixChunk[] chunks = new TestVertixChunk[p_count];

        for (int i = 0; i < p_count; i++) {
            if(i % 10_000_000 == 0) {
                LOGGER.info(memory.create().getCIDStoreInfo().toString());
                LOGGER.info(""+i);
            }
            if(i % 1_000_000 == 0) {
                LOGGER.info("still running");
            }
            chunks[i] = new TestVertixChunk(i);
            memory.create().testCreate(chunks[i]);
            Assert.assertTrue(chunks[i].isStateOk());
            Assert.assertTrue(chunks[i].isIDValid());
        }

        /*
        memory.remove().remove(chunks[0].getID());
        memory.remove().remove(chunks[1].getID());

        chunks[0] = new TestChunk(true);
        chunks[1] = new TestChunk(true);

        memory.create().create(chunks[0]);
        memory.create().create(chunks[1]);


        Assert.assertTrue(memory.analyze().analyze());

        for (TestVertixChunk chunk : chunks) {
            memory.put().put(chunk);
            Assert.assertTrue(chunk.isStateOk());
           // chunk.clear();
        }

        for (TestVertixChunk chunk : chunks) {
            memory.get().get(chunk);
            Assert.assertTrue(chunk.isStateOk());
            //chunk.verifyContents();
        }

        for (TestVertixChunk chunk : chunks) {
            memory.remove().remove(chunk);
            Assert.assertTrue(chunk.isStateOk());
        }
*/
        memory.shutdown();
    }
}
