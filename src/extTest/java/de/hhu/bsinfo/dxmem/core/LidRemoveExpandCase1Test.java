package de.hhu.bsinfo.dxmem.core;

import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.data.SpareLocalID;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Before;
import org.junit.Test;

public class LidRemoveExpandCase1Test {
    @Before
    public void setUp(){
        Configurator.setRootLevel(Level.TRACE);
    }

    @Test
    public void expandSingleValueLeftSideWithSingleValue() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 3, 5};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(false, 4);

        lidStore.put(3);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert result[1] == SpareLocalID.getSingleEntryIntervalID(3, 2);
    }

    @Test
    public void expandSingleValueLeftSideWithSingleEntryInterval() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 3, 6};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSingleEntryIntervalID(4, 2);

        lidStore.put(3);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert result[1] == SpareLocalID.getSingleEntryIntervalID(3, 3);
    }

    @Test
    public void expandSingleValueLeftSideWithSingleEntryInterval_2() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 3, 65539};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSingleEntryIntervalID(4, 65535);

        lidStore.put(3);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 3);
        assert result[2] == SpareLocalID.getSpareLocalID(true, 65538);

    }

    @Test
    public void expandSingleValueLeftSideWithTwoEntryInterval() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 3, 65540};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(true, 4);
        assert interimResult[2] == SpareLocalID.getSpareLocalID(true, 65539);

        lidStore.put(3);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 3);
        assert result[2] == SpareLocalID.getSpareLocalID(true, 65539);
    }

}
