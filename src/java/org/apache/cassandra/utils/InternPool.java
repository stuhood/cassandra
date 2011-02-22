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

import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * A bounded pool of interned ByteBuffers..
 */
public class InternPool
{
    private final NonBlockingHashMap<ByteBuffer, ByteBuffer> pool = new NonBlockingHashMap<ByteBuffer, ByteBuffer>();
    private final int cutoff;
    public InternPool(int cutoff)
    {
        this.cutoff = cutoff;
    }

    public ByteBuffer internOrTrim(ByteBuffer data, Allocator allocator)
    {
        if (pool.size() >= cutoff)
            return ByteBufferUtil.trim(data, allocator);
        return intern(data);
    }

    public ByteBuffer maybeIntern(ByteBuffer data)
    {
        if (pool.size() >= cutoff)
            return data;
        return intern(data);
    }

    private ByteBuffer intern(ByteBuffer data)
    {
        ByteBuffer internedData = pool.get(data);
        if (internedData != null)
            return internedData;
        // intern the data outside of slabs: it should live to a healthy old age
        internedData = ByteBufferUtil.trim(data, HeapAllocator.instance);
        ByteBuffer concurrentData = pool.putIfAbsent(internedData, internedData);
        if (concurrentData != null)
            return concurrentData;
        return internedData;
    }
}
