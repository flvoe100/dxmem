package de.hhu.bsinfo.dxmem.core;

import java.io.File;
import java.io.FileNotFoundException;

import de.hhu.bsinfo.dxutils.serialization.RandomAccessFileImExporter;

public class MemoryLoader {
    private Heap m_heap;
    private CIDTable m_table;
    private LIDStore m_lidStore;

    public MemoryLoader() {
        m_heap = new Heap();
        m_table = new CIDTable();
        m_lidStore = new LIDStore();
    }

    public void load(final String p_file) {
        assert p_file != null;

        File file = new File(p_file);

        if (!file.exists()) {
            throw new MemoryRuntimeException("Cannot load mem dump " + p_file + ": file does not exist");
        }

        m_table = new CIDTable();

        RandomAccessFileImExporter importer;

        try {
            importer = new RandomAccessFileImExporter(file);
        } catch (final FileNotFoundException e) {
            // cannot happen
            throw new MemoryRuntimeException("Illegal state", e);
        }

        importer.importObject(m_heap);
        importer.importObject(m_table);
        importer.importObject(m_lidStore);

        importer.close();

        m_heap = m_table.m_heap;
    }

    public Heap getHeap() {
        return m_heap;
    }

    public CIDTable getCIDTable() {
        return m_table;
    }

    public LIDStore getLIDStore() {
        return m_lidStore;
    }
}
