package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemTestUtils;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.TestVertixChunk;
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

public class CIdMemoryTest {

    private static final Logger LOGGER = LogManager.getFormatterLogger(CIdMemoryTest.class.getSimpleName());

    @Test
    public void originalDatasetPut() {
        Configurator.setRootLevel(Level.INFO);
        if (!DXMemTestUtils.sufficientMemoryForBenchmark(new StorageUnit(DXMemoryTestConstants.HEAP_SIZE_LARGE, "b"))) {
            LOGGER.warn("Skipping test due to insufficient memory available");
            return;
        }
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_LARGE);

        String file = "/home/vlz/bsinfo/datasets/datagen-8_7-zf.v";
        LOGGER.info(memory.getM_context().getHeap().getStatus().toString());

        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            long vid;
            TestVertixChunk v;
            long start = System.nanoTime();
            int i = 0;
            while ((line = reader.readLine()) != null) {
                vid = Long.parseLong(line.split("\\s")[0]);
                v = new TestVertixChunk(vid);
                memory.create().testCreate(v.sizeofObject(), vid, ChunkLockOperation.NONE);
                i++;

            }
            long end = System.nanoTime();
            LOGGER.info(memory.create().getCIDStoreInfo().toString());
            LOGGER.info(memory.getM_context().getHeap().getStatus().toString());
            LOGGER.info("Executetime: %d nanosecs", end - start);
        } catch (IOException e) {
            e.printStackTrace();
        }
        memory.shutdown();

    }

    @Test
    public void preprocessedDatasetPut() {
        Configurator.setRootLevel(Level.INFO);
        if (!DXMemTestUtils.sufficientMemoryForBenchmark(new StorageUnit(DXMemoryTestConstants.HEAP_SIZE_LARGE, "b"))) {
            LOGGER.warn("Skipping test due to insufficient memory available");
            return;
        }
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_LARGE);

        String file = "/home/vlz/bsinfo/datasets/datagen-8_7-zf.pp.v";
        LOGGER.info(memory.getM_context().getHeap().getStatus().toString());

        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            long vid;
            TestVertixChunk v;
            long start = System.nanoTime();
            int i = 0;
            while ((line = reader.readLine()) != null) {
                vid = Long.parseLong(line.split("\\s")[0]);
                v = new TestVertixChunk(vid);
                memory.create().testCreate(v.sizeofObject(), vid, ChunkLockOperation.NONE);
                i++;
                if (i % 10_000_000 == 0) {
                    LOGGER.info(memory.create().getCIDStoreInfo().toString());
                    LOGGER.info("" + i);
                }
            }
            long end = System.nanoTime();

            LOGGER.info(memory.create().getCIDStoreInfo().toString());
            LOGGER.info(memory.getM_context().getHeap().getStatus().toString());

            LOGGER.info("Executetime: %d nanosecs", end - start);
        } catch (IOException e) {
            e.printStackTrace();
        }
        memory.shutdown();
    }
}
