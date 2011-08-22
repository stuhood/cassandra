/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.marshal;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.utils.NodeId;
import org.apache.cassandra.utils.HeapAllocator;

public class CounterColumnTypeTest
{
    private static final int WINDOW = 100000;
    private static final int NODES = 10;
    private static final int COUNT = 10000;

    private static final CounterContext CC = CounterContext.instance();
    private static final Descriptor DESC = new Descriptor(Descriptor.CURRENT_VERSION, new File(""), "ks", "cf", 1, false);
    // sets of contexts containing deltas and non deltas for 2 nodes
    private static final List<ByteBuffer> SIMPLE = new ArrayList<ByteBuffer>();
    private static final List<ByteBuffer> COMPLEX = new ArrayList<ByteBuffer>();
    private static final List<ByteBuffer> BOTH = new ArrayList<ByteBuffer>();
    private static final List<ByteBuffer> LOTS = new ArrayList<ByteBuffer>();
    static
    {
        ByteBuffer one = CC.create(NodeId.fromInt(1), 1, 1, false);
        ByteBuffer oned = CC.create(NodeId.fromInt(1), 1, 1, true);
        ByteBuffer two = CC.create(NodeId.fromInt(2), 1, 1, false);
        ByteBuffer twod = CC.create(NodeId.fromInt(2), 1, 1, true);
        // simple contexts
        SIMPLE.add(one);
        SIMPLE.add(oned);
        SIMPLE.add(two);
        SIMPLE.add(twod);
        // contexts containing both nodes
        COMPLEX.add(CC.merge(one, two, HeapAllocator.instance));
        COMPLEX.add(CC.merge(one, twod, HeapAllocator.instance));
        COMPLEX.add(CC.merge(oned, two, HeapAllocator.instance));
        COMPLEX.add(CC.merge(oned, twod, HeapAllocator.instance));
        // all elements
        BOTH.addAll(SIMPLE);
        BOTH.addAll(COMPLEX);

        // generate a bunch of random count/nodes within a window
        Random rand = new Random();
        for (int i = 0; i < COUNT; i++)
            LOTS.add(CC.create(NodeId.fromInt(rand.nextInt(NODES)),
                               i,
                               rand.nextInt(WINDOW),
                               rand.nextBoolean()));
    };

    private ByteBuffer compress(List<ByteBuffer> contexts) throws Exception
    {
        ByteBuffer buff = CounterColumnType.instance.compress(DESC, contexts, null);
        buff.limit(buff.position()).rewind();
        return buff;
    }

    private void assertRoundTrip(List<ByteBuffer> contexts) throws Exception
    {
        List<ByteBuffer> output = new ArrayList<ByteBuffer>();
        // compress/decompress
        CounterColumnType.instance.decompress(DESC, compress(contexts), output);
        assertEquals(contexts, output);
    }

    @Test
    public void testSimple() throws Exception
    {
        assertRoundTrip(SIMPLE);
    }

    @Test
    public void testComplex() throws Exception
    {
        assertRoundTrip(COMPLEX);
    }

    @Test
    public void testBoth() throws Exception
    {
        assertRoundTrip(BOTH);
    }

    @Test
    public void testLots() throws Exception
    {
        assertRoundTrip(LOTS);
    }
}
