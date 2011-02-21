/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.    See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.    The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An allocator for ByteBuffers that allocates from large slabs, possibly off heap.
 * Adapted from Todd Lipcon's patch on https://issues.apache.org/jira/browse/HBASE-3455
 */
public final class SlabAllocator implements Allocator
{
    private static final Logger logger = LoggerFactory.getLogger(SlabAllocator.class);

    final static int DEFAULT_SLAB_BYTES = 2048 * 1024;
    final static boolean DEFAULT_DIRECT = false;

    private AtomicReference<Slab> curSlab = new AtomicReference<Slab>();
    private AtomicLong allocated = new AtomicLong(0);

    private final boolean direct;
    private final int byteSlabSize;
    // allocs bigger than this don't go through allocator
    private final int sizeThresholdBytes;

    public SlabAllocator()
    {
        this(direct, DEFAULT_SLAB_BYTES);
    }

    public SlabAllocator(boolean direct, int byteSlabSize)
    {
        this.direct = direct;
        this.byteSlabSize = byteSlabSize;
        this.sizeThresholdBytes = byteSlabSize >> 2;
    }

    private static ByteBuffer allocate(boolean direct, int size)
    {
        if (direct)
        {
            try
            {
                return ByteBuffer.allocateDirect(size);
            }
            catch (RuntimeException e)
            {
                logger.warn("Failed to allocate offheap buffer of size {}: {}", size, e);
            }
        }
        return ByteBuffer.allocate(size);
    }

    /** @return The total number of bytes allocated by this Allocator. */
    public long allocated()
    {
        return allocated.get();
    }

    public ByteBuffer allocate(int size)
    {
        allocated.addAndGet(size);
        if (size > sizeThresholdBytes)
            return allocate(direct, size);
        
        while (true)
        {
            Slab c = getOrMakeSlab();
            
            int allocOffset = c.alloc(size);
            if (allocOffset != -1)
            {
                ByteBuffer buf = c.data.duplicate();
                buf.position(allocOffset);
                buf.mark();
                buf.limit(allocOffset + size);
                return buf;
            }
            // not enough space!
            // try to retire this slab
            if (curSlab.compareAndSet(c, null))
                // we uninstalled it
                logger.debug("Retired slab {}", c);
        }
    }

    private Slab getOrMakeSlab()
    {
        while (true)
        {
            Slab c = curSlab.get();
            if (c != null)
                return c;
            // race to set slab
            c = new Slab(direct, byteSlabSize);
            if (curSlab.compareAndSet(null, c))
            {
                // we won race
                c.init();
                logger.debug("Allocated new slab {}", c);
                return c;
            }
            // someone else won race, try again
        }
    }

    @Override
    public String toString()
    {
        StringBuilder buff = new StringBuilder("SlabAllocator@");
        buff.append(System.identityHashCode(this));
        buff.append('(').append(direct ? "direct, " : "heap, ");
        buff.append(allocated).append(" allocated, ");
        buff.append(curSlab).append(" last)");
        return buff.toString();
    }

    private static final class Slab
    {
        private ByteBuffer data;
        private final AtomicInteger curOffset = new AtomicInteger();
        private final AtomicInteger allocCount = new AtomicInteger();
        private volatile boolean initialized = false;
        private final boolean direct;
        private final int size;
        
        private Slab(boolean direct, int size)
        {
            this.direct = direct;
            this.size = size;
        }
        
        /**
         * The creator of a slab will lazily initialize it once it is clear that
         * it will be used: consumers should loop checking initialized until the
         * slab is available.
         */
        private void init()
        {
            assert !initialized;
            data = allocate(direct, size);
            initialized = true;
        }
        
        private int alloc(int size)
        {
            while (!initialized)
                // wait for the creator of this slab to initialize it
                Thread.yield();
            while (true)
            {
                int oldOffset = curOffset.get();
                if (oldOffset + size > data.remaining())
                    return -1; // alloc doesn't fit
                if (curOffset.compareAndSet(oldOffset, oldOffset + size))
                {
                    // we got the alloc
                    allocCount.incrementAndGet();
                    return oldOffset;
                }
                // we raced and lost alloc, try again
            }
        }

        @Override
        public String toString()
        {
            StringBuilder buff = new StringBuilder("Slab@");
            buff.append(System.identityHashCode(this));
            buff.append('(').append(allocCount.get()).append(" allocs, ");
            buff.append(data.remaining() - curOffset.get()).append(" wasted)");
            return buff.toString();
        }
    }
}
