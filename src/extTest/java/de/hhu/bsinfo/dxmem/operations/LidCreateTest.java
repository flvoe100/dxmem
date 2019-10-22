package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.*;
import de.hhu.bsinfo.dxmem.data.ChunkID;
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
        TestChunk[] testChunk = new TestChunk[8];
        TestChunk dummy = new TestChunk(true);

        // memory.create().create(dummy.sizeofObject(), 0, ChunkLockOperation.NONE);
        long id = memory.create().create(dummy.sizeofObject(), 0, ChunkLockOperation.NONE);
        memory.create().create(dummy.sizeofObject(), 7, ChunkLockOperation.NONE);
        memory.create().create(dummy.sizeofObject(), 11, ChunkLockOperation.NONE);

        memory.create().getM_context().getLIDStore().getM_spareLIDStore().printRingBufferSpareLocalIDs();
        //LOGGER.debug("---------------------------------");
        for (int i = 0; i < testChunk.length; i++) {
            testChunk[i] = new TestChunk(true);
        }
        memory.create().create(0, 8, false, ChunkLockOperation.NONE, testChunk);
        for (int i = 0; i < testChunk.length; i++) {
            System.out.println("testChunk = " + ChunkID.getLocalID(testChunk[i].getID()));
        }


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

        String vfile = "/home/vlz/bsinfo/datasets/dota-league/dota-league_1.v";
        String efile = "/home/vlz/bsinfo/datasets/dota-league/dota-league_1.e";

        try {
            BufferedReader reader = new BufferedReader(new FileReader(vfile));
            String line;
            long vid;
            TestVertixChunk v;
            int i = 0;
            System.out.println("Reading vertices");
            while ((line = reader.readLine()) != null) {

                vid = Long.parseLong(line.split("\\s")[0]);
                v = new TestVertixChunk(vid);
                memory.create().create(v, vid, ChunkLockOperation.NONE);
                memory.put().put(v);
                i++;
                if (i % 1_000 == 0) {
                    System.out.println("i = " + i);
                }

            }
            reader.close();
            memory.create().getM_context().getLIDStore().getM_spareLIDStore().printRingBufferSpareLocalIDs();
            reader = new BufferedReader(new FileReader(efile));
            TestEdgeChunk e;
            i = 0;
            long from = 0;
            long to = 0;
            System.out.println("Reading edges");

            while ((line = reader.readLine()) != null) {

                String[] split = line.split("\\s");
                from = Long.parseLong(split[0]);
                to = Long.parseLong(split[1]);
                e = new TestEdgeChunk(from, to);
                memory.create().create(e);
                memory.put().put(e);
                i++;
                if (i % 100_000 == 0) {
                    System.out.println("i = " + i);
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        memory.shutdown();
    }
}
