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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.TestStrChunk;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxmem.data.ChunkState;

public class ResizeTest {
    @Test
    public void resize() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_3);
        memory.create().create(ds);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        Assert.assertEquals(ChunkState.OK, memory.resize().resize(ds.getID(), DXMemoryTestConstants.CHUNK_SIZE_3 * 2));

        Assert.assertEquals(DXMemoryTestConstants.CHUNK_SIZE_3 * 2, memory.size().size(ds.getID()));

        memory.remove().remove(ds);
        Assert.assertTrue(ds.isStateOk());

        Assert.assertEquals(ChunkState.DOES_NOT_EXIST, memory.resize().resize(ds.getID(), 1));

        memory.shutdown();
    }

    @Test
    public void resizeShrink() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        Assert.assertTrue(memory.analyze().analyze());

        TestStrChunk t1 = new TestStrChunk();
        memory.create().create(t1);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(t1.isStateOk());

        Assert.assertTrue(memory.put().put(t1));

        Assert.assertTrue(memory.analyze().analyze());

        t1.setAbc("123123");
        memory.resize().resize(t1.getID(), t1.sizeofObject());

        Assert.assertTrue(memory.analyze().analyze());

        TestStrChunk t3 = new TestStrChunk();
        memory.create().create(t3);

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }
}
