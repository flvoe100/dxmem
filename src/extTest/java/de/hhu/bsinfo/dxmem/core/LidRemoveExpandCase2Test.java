package de.hhu.bsinfo.dxmem.core;

import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.data.SpareLocalID;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Before;
import org.junit.Test;

public class LidRemoveExpandCase2Test {

    @Before
    public void setUp(){
        Configurator.setRootLevel(Level.TRACE);
    }

    //-----------------------------------------SINGLE VALUE-------------------------------------------------------------
    @Test
    public void expandSingleValueRightSideWithSingleValue() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 4};

        lidStore.put(lids);

        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(false, 3);

        lidStore.put(4);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert result[1] == SpareLocalID.getSingleEntryIntervalID(3, 2);
    }

    @Test
    public void expandSingleValueRightSideWithSingleEntryInterval() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 5};

        lidStore.put(lids);

        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSingleEntryIntervalID(3, 2);

        lidStore.put(5);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert result[1] == SpareLocalID.getSingleEntryIntervalID(3, 3);
    }

    @Test
    public void expandSingleValueRightSideWithSingleEntryInterval_2() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 65538};

        lidStore.put(lids);

        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSingleEntryIntervalID(3, 65535);

        lidStore.put(65538);

        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 3);
        assert result[2] == SpareLocalID.getSpareLocalID(true, 65538);

    }

    @Test
    public void expandSingleValueRightSideWithTwoEntryInterval() {
        Heap heap = new Heap(DXMemoryTestConstants.HEAP_SIZE_SMALL);
        CIDTable cidTable = new CIDTable(DXMemoryTestConstants.NODE_ID, heap);
        LIDStore lidStore = new LIDStore(DXMemoryTestConstants.NODE_ID, cidTable);
        long[] lids = {2, 65539};

        lidStore.put(lids);
        long[] interimResult = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert interimResult[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert interimResult[1] == SpareLocalID.getSpareLocalID(true, 3);
        assert interimResult[2] == SpareLocalID.getSpareLocalID(true, 65538);


        lidStore.put(65539);
        long[] result = lidStore.getM_spareLIDStore().getRingBufferSpareLocalIDs();
        assert result[0] == SpareLocalID.getSpareLocalID(false, 1);
        assert result[1] == SpareLocalID.getSpareLocalID(true, 3);
        assert result[2] == SpareLocalID.getSpareLocalID(true, 65539);
    }



}
