package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.*;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class LidCreateTest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LidCreateTest.class.getSimpleName());

    @Test
    public void simpleLidCreate() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);
        TestChunk[] testChunk = new TestChunk[5];
        TestChunk dummy = new TestChunk(true);

        memory.create().create(dummy.sizeofObject(), 1, ChunkLockOperation.NONE);
        long id =    memory.create().create(dummy.sizeofObject(), 3, ChunkLockOperation.NONE);
        memory.create().create(dummy.sizeofObject(), 7, ChunkLockOperation.NONE);
        memory.create().getM_context().getLIDStore().getM_spareLIDStore().printRingBufferSpareLocalIDs();

        memory.remove().remove(id);

        memory.create().getM_context().getLIDStore().getM_spareLIDStore().printRingBufferSpareLocalIDs();

             /*
        memory.create().create(dummy.sizeofObject(), 10, ChunkLockOperation.NONE);
        memory.create().create(dummy.sizeofObject(), 41, ChunkLockOperation.NONE);
        memory.create().getM_context().getLIDStore().getM_spareLIDStore().printRingBufferSpareLocalIDs();

        memory.create().create(dummy.sizeofObject(), 48, ChunkLockOperation.NONE);
        memory.create().getM_context().getLIDStore().getM_spareLIDStore().printRingBufferSpareLocalIDs();

        memory.create().create(dummy.sizeofObject(), 50, ChunkLockOperation.NONE);
        memory.create().create(dummy.sizeofObject(), 59, ChunkLockOperation.NONE);


        for (int i = 0; i < 5; i++) {
            testChunk[i] = new TestChunk(true);
            memory.create().create(testChunk[i]);
            System.out.println("testChunkID = " + ChunkID.getLocalID(testChunk[i].getID()));

        }
       //memory.create().create(testChunk.sizeofObject(), 0, ChunkLockOperation.NONE);
        //memory.create().create(testChunk.sizeofObject(), 1, ChunkLockOperation.NONE);
        memory.create().create(testChunk.sizeofObject(), 2, ChunkLockOperation.NONE);
        memory.create().create(testChunk.sizeofObject(), 3, ChunkLockOperation.NONE);
        memory.create().create(testChunk.sizeofObject(), 4, ChunkLockOperation.NONE);*/

        //memory.create().create(testChunk.sizeofObject(), 5, ChunkLockOperation.NONE);
    }

    @Test
    public void singleNodeLoad() {
        Configurator.setRootLevel(Level.TRACE);
        if (!DXMemTestUtils.sufficientMemoryForBenchmark(new StorageUnit(DXMemoryTestConstants.HEAP_SIZE_LARGE, "b"))) {
            LOGGER.warn("Skipping test due to insufficient memory available");
            return;
        }
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_LARGE);

        String file = "/home/vlz/bsinfo/datasets/datagen-8_7-zf.v";

        int endLine = 1000;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            long vid;
            TestVertixChunk v;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                if (i == endLine) {
                    break;
                }
                vid = Long.parseLong(line.split("\\s")[0]);
                v = new TestVertixChunk(vid);
                memory.create().create(v.sizeofObject(), vid, ChunkLockOperation.NONE);
                i++;
                if (i % 10_000_000 == 0) {
                    System.out.println("i = " + i);
                }

            }
            memory.create().getM_context().getLIDStore().getM_spareLIDStore().printRingBufferSpareLocalIDs();
            TestVertixChunk chunk102 = new TestVertixChunk();
            chunk102.setID(102);
            boolean a =memory.get().get(chunk102);
            System.out.println("a = " + a);
            System.out.println("chunk102.getExternalId() = " + chunk102.getExternalId());

        } catch (IOException e) {
            e.printStackTrace();
        }
        memory.shutdown();
    }
}
