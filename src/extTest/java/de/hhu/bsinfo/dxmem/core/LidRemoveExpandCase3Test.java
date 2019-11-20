package de.hhu.bsinfo.dxmem.core;

import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.data.SpareLocalID;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Before;
import org.junit.Test;

public class LidRemoveExpandCase3Test {

    @Before
    public void setUp(){
        Configurator.setRootLevel(Level.TRACE);
    }

    @Test
    public void expandSingleValueRightSideWithSingleValue(){
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 4};

        lidStore.put(lids);
        lidStore.getM_spareLIDStore().printRingBufferSpareLocalIDs();
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(false, 3);

        lidStore.put(2);
        lidStore.getM_spareLIDStore().printRingBufferSpareLocalIDs();

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSingleEntryIntervalID(1, 3);
        assert result[1] == 0;
    }

    @Test
    public void expandSingleValueRightSideWithSingleEntryInterval(){
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 5};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSingleEntryIntervalID(3, 2);

        lidStore.put(2);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSingleEntryIntervalID(1, 4);
        assert result[1] == 0;
    }

    @Test
    public void expandSingleValueRightSideWithSingleEntryInterval_2(){
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 65538};

        lidStore.put(lids);

        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSingleEntryIntervalID(3, 65535);

        lidStore.put(2);
        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 65537);
    }

    @Test
    public void expandSingleValueRightSideWithTwoEntryInterval(){
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 65539};

        lidStore.put(lids);

        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(true, 3);
        assert interimResult[2] == SpareLocalID.getSpareLocalID(true, 65538);

        lidStore.put(2);
        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 65538);
        assert result[2] == 0;

    }

    @Test
    public void expandSingleEntryIntervalRightSideWithSingleValue(){
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {5, 7};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSingleEntryIntervalID(1, 4);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(false, 6);

        lidStore.put(5);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSingleEntryIntervalID(1, 5);
        assert result[1] == 0;
    }

    @Test
    public void expandSingleEntryIntervalRightSideWithSingleValue_2(){
        //testing when single entry + 1 results to two enty interval
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {65536, 65538};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSingleEntryIntervalID(1, 65535);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(false, 65537);

        lidStore.put(65536);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 65537);
    }

    @Test
    public void expandSingleEntryIntervalRightSideWithSingleEntryInterval_1() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {5, 8};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSingleEntryIntervalID(1, 4);
        assert interimResult[1] == SpareLocalID.getSingleEntryIntervalID(6, 2);

        lidStore.put(5);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSingleEntryIntervalID(1, 7);
        assert result[1] == 0;
    }

    @Test
    public void expandSingleEntryIntervalRightSideWithSingleEntryInterval_2() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {5, 65541};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSingleEntryIntervalID(1, 4);
        assert interimResult[1] == SpareLocalID.getSingleEntryIntervalID(6, 65535);

        lidStore.put(5);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 65540);

    }

    @Test
    public void expandSingleEntryIntervalRightSideWithTwoEntryInterval() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {5, 65542};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSingleEntryIntervalID(1, 4);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(true, 6);
        assert interimResult[2] == SpareLocalID.getSpareLocalID(true, 65541);

        lidStore.put(5);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 65541);
        assert result[2] == 0;
    }

    @Test
    public void expandTwoEntryIntervalRightSideWithSingleValue() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {65537, 65539};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(true, 65536);
        assert interimResult[2] == SpareLocalID.getSpareLocalID(false, 65538);

        lidStore.put(65537);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 65538);
        assert result[2] == 0;
    }

    @Test
    public void expandTwoEntryIntervalRightSideWithSingleEntryInterval() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {65537, 65540};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(true, 65536);
        assert interimResult[2] == SpareLocalID.getSingleEntryIntervalID(65538, 2);

        lidStore.put(65537);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 65539);
        assert result[2] == 0;
    }
    @Test
    public void expandTwoEntryIntervalRightSideWithTwoEntryInterval() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {65537, 131074};

        lidStore.put(lids);

        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(true, 65536);
        assert interimResult[2] == SpareLocalID.getSpareLocalID(true, 65538);
        assert interimResult[3] == SpareLocalID.getSpareLocalID(true, 131073);

        lidStore.put(65537);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(true, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 131073);
        assert result[2] == 0;
        assert result[3] == 0;
    }
}
