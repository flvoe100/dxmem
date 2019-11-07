package de.hhu.bsinfo.dxmem;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class TestVertixChunk extends TestVertix {
    private long externalId;

    public TestVertixChunk() {
    }

    public TestVertixChunk(long externalId) {
        this.externalId = externalId;
    }

    public TestVertixChunk(long p_chunkID, long externalId) {
        super(p_chunkID);
        this.externalId = externalId;
    }

    public long getExternalId() {
        return externalId;
    }

    public void setExternalId(long externalId) {
        this.externalId = externalId;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(this.externalId);

    }

    @Override
    public void importObject(Importer p_importer) {
        this.externalId = p_importer.readLong(this.externalId);

    }

    @Override
    public int sizeofObject() {
        return Long.BYTES;
    }
}
