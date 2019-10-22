package de.hhu.bsinfo.dxmem;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class TestEdgeChunk extends AbstractChunk {
    private long from;
    private long to;

    public TestEdgeChunk() {
    }

    public TestEdgeChunk(long from, long to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(from);
        p_exporter.writeLong(to);
    }

    @Override
    public void importObject(Importer p_importer) {
        from = p_importer.readLong(from);
        to = p_importer.readLong(to);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES * 2;
    }
}
