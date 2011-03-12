package org.apache.cassandra.utils;

import com.sun.jna.Memory;

public class FreeableMemory extends Memory
{
    public FreeableMemory(long size)
    {
        super(size);
    }

    public void free()
    {
        assert peer != 0;
        finalize(); // calls free and sets peer to zero
    }

    /**
     * avoid re-freeing already-freed memory
     */
    @Override
    protected void finalize()
    {
        if (peer != 0)
            super.finalize();
    }
}
