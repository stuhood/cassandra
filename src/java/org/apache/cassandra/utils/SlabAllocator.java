/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FreeableMemory;

/**
 * A slabbed Allocator.
 * <p/>
 * The SlabAllocator is a bump-the-pointer allocator that allocates
 * large (2MB by default) regions and then doles them out to threads that request
 * slices into the array.
 * <p/>
 * The purpose of this class is to combat heap fragmentation in long lived
 * objects: by ensuring that all allocations with similar lifetimes
 * only to large regions of contiguous memory, we ensure that large blocks
 * get freed up at the same time.
 * <p/>
 * Otherwise, variable length byte arrays allocated end up
 * interleaved throughout the heap, and the old generation gets progressively
 * more fragmented until a stop-the-world compacting collection occurs.
 */
public class SlabAllocator extends Allocator
{
    private final AtomicReference<Region> currentRegion = new AtomicReference<Region>();
    private final Collection<Region> filledRegions = new LinkedBlockingQueue<Region>();

    private final boolean direct;
    private final int maxSlabAllocation;
    private final int regionSize;

    public SlabAllocator(boolean direct, int size)
    {
        this.direct = direct;
        regionSize = size;
        maxSlabAllocation = regionSize >> 2;
    }

    public ByteBuffer allocate(int size)
    {
        assert size >= 0;
        if (size == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        // satisfy large allocations directly from JVM since they don't cause fragmentation
        // as badly, and fill up our regions quickly
        if (size > maxSlabAllocation)
            return ByteBuffer.allocate(size);

        while (true)
        {
            Region region = getRegion();

            // Try to allocate from this region
            ByteBuffer cloned = region.allocate(size);
            if (cloned != null)
                return cloned;

            // not enough space!
            tryRetireRegion(region);
        }
    }
    
    /**
     * Try to retire the current region if it is still <code>region</code>.
     * Postcondition is that curRegion.get() != region
     */
    private void tryRetireRegion(Region region)
    {
        // retire the region regardless, but only record direct buffers as filled
        if (currentRegion.compareAndSet(region, null) && direct)
            filledRegions.add(region);
    }

    /**
     * Get the current region, or, if there is no current region, allocate a new one
     */
    private Region getRegion()
    {
        while (true)
        {
            // Try to get the region
            Region region = currentRegion.get();
            if (region != null)
                return region;

            // No current region, so we want to allocate one. We race
            // against other allocators to CAS in an uninitialized region
            // (which is cheap to allocate)
            region = new Region(direct, regionSize);
            if (currentRegion.compareAndSet(null, region))
            {
                // we won race - now we need to actually do the expensive allocation step
                region.init();
                return region;
            }
            // someone else won race - that's fine, we'll try to grab theirs
            // in the next iteration of the loop.
        }
    }

    public void free()
    {
        if (!direct)
            // noop
            return;
        Region last = currentRegion.get();
        if (last != null)
            filledRegions.add(last);
        for (Region region : filledRegions)
            region.free();
    }

    /**
     * A region of memory out of which allocations are sliced.
     *
     * This serves two purposes:
     *  - to provide a step between initialization and allocation, so that racing to CAS a
     *    new region in is harmless
     *  - encapsulates the allocation offset
     */
    private static class Region
    {
        private static final int UNINITIALIZED = -1;

        /**
         * Actual underlying data
         */
        private ByteBuffer data;

        private FreeableMemory memory;

        /**
         * Offset for the next allocation, or the sentinel value -1
         * which implies that the region is still uninitialized.
         */
        private final AtomicInteger nextFreeOffset = new AtomicInteger(UNINITIALIZED);

        /**
         * Total number of allocations satisfied from this buffer
         */
        private final AtomicInteger allocCount = new AtomicInteger();

        /**
         * True if this allocation should be made offheap.
         */
        private final boolean direct;

        /**
         * Size of region in bytes
         */
        private final int size;

        /**
         * Create an uninitialized region. Note that memory is not allocated yet, so
         * this is cheap.
         *
         * @param size in bytes
         */
        private Region(boolean direct, int size)
        {
            this.direct = direct;
            this.size = size;
        }

        /**
         * Actually claim the memory for this region. This should only be called from
         * the thread that constructed the region. It is thread-safe against other
         * threads calling alloc(), who will block until the allocation is complete.
         */
        public void init()
        {
            assert nextFreeOffset.get() == UNINITIALIZED;
            if (direct)
            {
                memory = new FreeableMemory(size);
                data = memory.getByteBuffer(0, size);
            }
            else
            {
                data = ByteBuffer.allocate(size);
            }

            assert data.remaining() == data.capacity();
            // Mark that it's ready for use
            boolean initted = nextFreeOffset.compareAndSet(UNINITIALIZED, 0);
            // We should always succeed the above CAS since only one thread calls init()!
            Preconditions.checkState(initted, "Multiple threads tried to init same region");
        }

        /**
         * Try to allocate <code>size</code> bytes from the region.
         *
         * @return the successful allocation, or null to indicate not-enough-space
         */
        public ByteBuffer allocate(int size)
        {
            while (true)
            {
                int oldOffset = nextFreeOffset.get();
                if (oldOffset == UNINITIALIZED)
                {
                    // The region doesn't have its data allocated yet.
                    // Since we found this in currentRegion, we know that whoever
                    // CAS-ed it there is allocating it right now. So spin-loop
                    // shouldn't spin long!
                    Thread.yield();
                    continue;
                }

                if (oldOffset + size > data.capacity()) // capacity == remaining
                    return null;

                // Try to atomically claim this region
                if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size))
                {
                    // we got the alloc
                    allocCount.incrementAndGet();
                    return (ByteBuffer) data.duplicate().position(oldOffset).limit(oldOffset + size);
                }
                // we raced and lost alloc, try again
            }
        }

        @Override
        public String toString()
        {
            return "Region@" + System.identityHashCode(this) +
                   " allocs=" + allocCount.get() + "waste=" +
                   (data.capacity() - nextFreeOffset.get());
        }

        public void free()
        {
            if (memory != null)
                memory.free();
        }
    }
}
