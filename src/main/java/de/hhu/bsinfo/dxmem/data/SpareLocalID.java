package de.hhu.bsinfo.dxmem.data;

public class SpareLocalID {
    private static final long LOCALID_BITMASK = 0x0000FFFFFFFFFFFFL;
    private static final long INTERVAL_VALUE_BORDER = 32768;


    public static long getLID(long sparseLID) {
        return sparseLID & LOCALID_BITMASK;
    }

    public static long getEntryInfo(long sparseLID) {
        return sparseLID >> 48;
    }

    /**
     * Creates a spare LID. A sparse LID identifies itself that the first 16 Bits are reserved for a flag. The flag
     * shows if the entry is a part of an interval or not. The remaining 48 Bits are for the Local-ID.
     *
     * @param isInterval True, if lid is part of an interval
     * @param lid        Local-ID to save
     * @return Translated LID
     */
    public static long getSpareLocalID(boolean isInterval, long lid) {
        return (long) (isInterval ? (short) 1 : (short) 0) << 48 | lid & LOCALID_BITMASK;
    }

    /**
     * Creates a single Interval ID. A single Interval ID identifies itself that the first 16 Bits are reserved for
     * the size of the interval. Max. size is 65535. The remaining 48 Bits are for the Start-LID (inclusive).
     *
     * @param startLID       Start-LID of the interval (inclusive)
     * @param intervalLength Size of the interval (inclusive Start-LID)
     * @return Translated LID
     */
    public static long getSingleEntryIntervalID(long startLID, long intervalLength) {
        return intervalLength << 48 | startLID & LOCALID_BITMASK;
    }

    /**
     * Extracts the size of a SingleEntryIntervalID.
     *
     * @param intervalID SingleEntryIntervalID
     * @return Size of the interval
     */
    public static long getSingleEntryIntervalSize(long intervalID) {
        long info = intervalID >> 48;
        //16 Bits can hold 65536 numbers. Till 32768 is consecutive. Then it goes from -32768 to 0. 0 is not used!
        //That's why the interval can hold "only" 65535 and not 65536.
        return info > 0 ? info : INTERVAL_VALUE_BORDER + INTERVAL_VALUE_BORDER + info;
    }
}
