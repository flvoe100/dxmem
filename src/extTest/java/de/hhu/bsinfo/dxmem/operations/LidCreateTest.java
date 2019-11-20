package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.*;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class LidCreateTest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LidCreateTest.class.getSimpleName());

    @Test
    public void simpleCreateAndPut() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        TestVertixChunk v = new TestVertixChunk();
        long[] p_lIDs = {
                1,
                2,
                4,
                5,
                6,
                7,
                9,
                14,
                16,
                17,
                18,
                19,
                22,
                28,
                30,
                32,
                33,
                36,
                38,
                43,
                46,
                55,
                56,
                60,
                61,
                65,
                69,
                85,
                87,
                88,
                94,
                96,
                97,
                99,
                105,
                108,
                110,
                113,
                114,
                117,
                118,
                124,
                129,
                132,
                156,
                157,
                169,
                173,
                176,
                182,
                192,
                194,
                203,
                210,
                220,
                222,
                223,
                225,
                241,
                270,
                285,
                292,
                300,
                302,
                306,
                327,
                329,
                343,
                349,
                350,
                372,
                384,
                391,
                405,
                414,
                417,
                456,
                466,
                474,
                476,
                487,
                488,
                491,
                496,
                529,
                546,
                548,
                601,
                603,
                612,
                650,
                674,
                699,
                702,
                706,
                707,
                709,
                714,
                719,
                721
        };
        Random rnd = new Random();
        long[] p_CIDs = Arrays.copyOf(p_lIDs, p_lIDs.length);
        memory.create().create(p_CIDs, 0, 100, v.sizeofObject(), false, true);

        for (int i = 0; i < p_CIDs.length; i++) {
            v = new TestVertixChunk(p_CIDs[i], p_lIDs[i]);
            memory.put().put(v);
        }
        for (int i = 0; i < p_CIDs.length; i++) {
            TestVertixChunk test = new TestVertixChunk();
            test.setID(ChunkID.getChunkID(DXMemoryTestConstants.NODE_ID, p_lIDs[i]));
            boolean succes = memory.get().get(test);
            System.out.println("succes = " + succes);
            System.out.println("test = " + test.getExternalId());
        }

    }

    @Test
    public void simpleLidCreate() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_LARGE);
        TestChunk[] testChunk = new TestChunk[12];
        TestChunk dummy = new TestChunk(true);
        long[] lids = {3, 65540};

        memory.create().create(lids, 0, lids.length, dummy.sizeofObject(), false, true);
        //  long id = memory.create().create(dummy.sizeofObject(), 0, ChunkLockOperation.NONE);
        //memory.create().create(dummy.sizeofObject(), 7, ChunkLockOperation.NONE);
        //memory.create().create(dummy.sizeofObject(), 11, ChunkLockOperation.NONE);

        memory.create().getM_context().getLIDStore().getM_spareLIDStore().printRingBufferSpareLocalIDs();


         memory.remove().remove(ChunkID.getChunkID(DXMemoryTestConstants.NODE_ID, 3));
        memory.create().getM_context().getLIDStore().getM_spareLIDStore().printRingBufferSpareLocalIDs();

        //LOGGER.debug("---------------------------------");
        /*
        long[] cids = new long[65535];
        memory.create().create(cids, 0, cids.length, dummy.sizeofObject(), false, false);
        memory.create().getM_context().getLIDStore().getM_spareLIDStore().printRingBufferSpareLocalIDs();
         */
    }

    @Test
    public void singleNodeLoad() {
        Configurator.setRootLevel(Level.INFO);
        if (!DXMemTestUtils.sufficientMemoryForBenchmark(new StorageUnit(DXMemoryTestConstants.HEAP_SIZE_LARGE, "b"))) {
            LOGGER.warn("Skipping test due to insufficient memory available");
            return;
        }
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_LARGE);

        String vfile = "/home/vlz/bsinfo/datasets/datagen-8_7-zf.v";
        String efile = "/home/vlz/bsinfo/datasets/dota-league/dota-league.e";

        try {
            BufferedReader reader = new BufferedReader(new FileReader(vfile));
            String line;
            long vid;
            TestVertixChunk v = new TestVertixChunk();
            int i = 0;
            System.out.println("Reading vertices");
            int maxVertices = 145050709;
            int processedV = 0;
            int packetSize = 10000;
            long[] p_cids = new long[packetSize];
            while ((line = reader.readLine()) != null) {

                vid = Long.parseLong(line.split("\\s")[0]);
                p_cids[i] = vid;
                i++;
                processedV++;
                if (i == packetSize) {
                    memory.create().create(p_cids, 0, p_cids.length, v.sizeofObject(), false, true);

                    i = 0;
                    /*
                    for (int j = 0; j < packetSize; j++) {
                        long lid = ChunkID.getLocalID(p_cids[j]);

                        TestVertixChunk vertic = new TestVertixChunk(p_cids[j], lid);
                        memory.put().put(vertic);

                    }

                     */
                    if (maxVertices - processedV < 0) {
                        p_cids = new long[maxVertices - processedV];
                    }
                }
            }
            reader.close();
            /*
            reader = new BufferedReader(new FileReader(vfile));
            int read = 0;
            while ((line = reader.readLine()) != null) {
                vid = Long.parseLong(line.split("\\s")[0]);
                read++;
                if (read > packetSize) {
                    break;
                }
                TestVertixChunk x = new TestVertixChunk();
                x.setID(ChunkID.getChunkID(DXMemoryTestConstants.NODE_ID, vid));
                memory.get().get(x);
                System.out.println("x.getExternalId() = " + x.getExternalId());
            }

             */
            memory.create().getM_context().getLIDStore().getM_spareLIDStore().writeRingBufferSpareLocalIDs();
            /*
            reader = new BufferedReader(new FileReader(efile));
            TestEdgeChunk e = new TestEdgeChunk();
            i = 0;
            System.out.println("Reading Edges");
            int maxEdges = 50870313;
            int processedEdges = 0;
            long from;
            long to;
            p_cids = new long[packetSize];
            while ((line = reader.readLine()) != null) {

                i++;
                processedEdges++;
                if (i == packetSize) {
                    memory.create().create(p_cids, 0, p_cids.length, e.sizeofObject(), false, false);

                    i = 0;

                    if (maxEdges - processedEdges < 0) {
                        p_cids = new long[maxEdges - processedEdges];
                    }

                }
            }
            //memory.create().getM_context().getLIDStore().getM_spareLIDStore().writeRingBufferSpareLocalIDs();
             */

            memory.shutdown();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
