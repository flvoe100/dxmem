/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.core.CIDTableStatus;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.HeapStatus;
import de.hhu.bsinfo.dxmem.core.LIDStoreStatus;

/**
 * Get stats from various components of DXMem
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public class Stats {
    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public Stats(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Gets the status of the heap
     *
     * @return the status
     */
    public HeapStatus getHeapStatus() {
        return m_context.getHeap().getStatus();
    }

    /**
     * Get the current status of the CIDTable
     *
     * @return Current status of CIDTable
     */
    public CIDTableStatus getCIDTableStatus() {
        return m_context.getCIDTable().getStatus();
    }

    /**
     * Get the status of the LIDSTore
     *
     * @return Status object
     */
    public LIDStoreStatus getLIDStoreStatus() {
        return m_context.getLIDStore().getStatus();
    }
}
