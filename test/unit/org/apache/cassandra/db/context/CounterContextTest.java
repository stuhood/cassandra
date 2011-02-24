/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.db.context;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.context.IContext.ContextRelationship;
import org.apache.cassandra.utils.*;

/**
 * Note: these tests assume IPv4 (4 bytes) is used for id.
 *       if IPv6 (16 bytes) is used, tests will fail (but the code will work).
 *       however, it might be pragmatic to modify the code to just use
 *       the IPv4 portion of the IPv6 address-space.
 */
public class CounterContextTest
{
    private static final CounterContext cc = new CounterContext();

    private static final InetAddress idAddress;
    private static final byte[] id;
    private static final int idLength;
    private static final int clockLength;
    private static final int countLength;

    private static final int stepLength;
    private static final int defaultEntries;

    static
    {
        idAddress      = FBUtilities.getLocalAddress();
        id             = idAddress.getAddress();
        idLength       = 4; // size of int
        clockLength    = 8; // size of long
        countLength    = 8; // size of long
        stepLength     = idLength + clockLength + countLength;

        defaultEntries = 10;
    }

    /** Allocates 1 byte from a new SlabAllocator and returns it. */
    private SlabAllocator bumpedSlab(boolean direct)
    {
        SlabAllocator allocator = new SlabAllocator(direct);
        allocator.allocate(1);
        return allocator;
    }

    /** @return The _absolute_ offset in the given buffer for the given step. */
    private int stepOff(ByteBuffer buff, int step)
    {
        return buff.position() + step * stepLength;
    }

    @Test
    public void testCreate()
    {
        runCreate(HeapAllocator.instance);
        runCreate(bumpedSlab(false));
        runCreate(bumpedSlab(true));
    }

    private void runCreate(Allocator allocator)
    {
        ByteBuffer bytes = cc.create(4, allocator);
        assertEquals(stepLength, bytes.remaining());

        // id
        ByteBuffer idInContext = bytes.duplicate();
        idInContext.position(stepOff(bytes, 0)).limit(stepOff(bytes, 0) + idLength);
        assertEquals(ByteBuffer.wrap(id), idInContext);
        // clock
        assertEquals(1L, bytes.getLong(stepOff(bytes, 0) + idLength));
        // value
        assertEquals(4L, bytes.getLong(stepOff(bytes, 0) + idLength + clockLength));
    }

    @Test
    public void testDiff()
    {
        runDiff(HeapAllocator.instance);
        runDiff(bumpedSlab(false));
        runDiff(bumpedSlab(true));
    }

    private void runDiff(Allocator allocator)
    {
        ByteBuffer left = allocator.allocate(3 * stepLength);
        ByteBuffer right;

        // equality: equal nodes, all counts same
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9), 1L, 0L);
        right = ByteBufferUtil.clone(left);

        assert ContextRelationship.EQUAL ==
            cc.diff(left, right);

        // greater than: left has superset of nodes (counts equal)
        left = allocator.allocate(4 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3),  3L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6),  2L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9),  1L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 3), FBUtilities.toByteArray(12), 0L, 0L);

        right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9), 1L, 0L);

        assert ContextRelationship.GREATER_THAN ==
            cc.diff(left, right);
        
        // less than: left has subset of nodes (counts equal)
        left = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9), 1L, 0L);

        right = allocator.allocate(4 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3),  3L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6),  2L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9),  1L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 3), FBUtilities.toByteArray(12), 0L, 0L);

        assert ContextRelationship.LESS_THAN ==
            cc.diff(left, right);

        // greater than: equal nodes, but left has higher counts
        left = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9), 3L, 0L);

        right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9), 1L, 0L);

        assert ContextRelationship.GREATER_THAN ==
            cc.diff(left, right);

        // less than: equal nodes, but right has higher counts
        left = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9), 3L, 0L);

        right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3), 3L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6), 9L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9), 3L, 0L);

        assert ContextRelationship.LESS_THAN ==
            cc.diff(left, right);

        // disjoint: right and left have disjoint node sets
        left = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(4), 1L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9), 1L, 0L);

        right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6), 1L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        left = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(4), 1L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9), 1L, 0L);

        right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(2),  1L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6),  1L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(12), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        // disjoint: equal nodes, but right and left have higher counts in differing nodes
        left = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6), 3L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9), 1L, 0L);

        right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6), 1L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9), 5L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        left = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 2L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6), 3L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9), 1L, 0L);

        right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3), 1L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6), 9L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9), 5L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        // disjoint: left has more nodes, but lower counts
        left = allocator.allocate(4 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3),  2L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6),  3L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9),  1L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 3), FBUtilities.toByteArray(12), 1L, 0L);

        right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3), 4L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6), 9L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9), 5L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);
        
        // disjoint: left has less nodes, but higher counts
        left = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 5L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6), 3L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9), 2L, 0L);

        right = allocator.allocate(4 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3),  4L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6),  3L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9),  2L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 3), FBUtilities.toByteArray(12), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        // disjoint: mixed nodes and counts
        left = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 5L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(9), 2L, 0L);

        right = allocator.allocate(4 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3),  4L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6),  3L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9),  2L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 3), FBUtilities.toByteArray(12), 1L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);

        left = allocator.allocate(4 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(3), 5L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(6), 2L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(7), 2L, 0L);
        cc.writeElementAtOffset(left, stepOff(left, 3), FBUtilities.toByteArray(9), 2L, 0L);

        right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(3), 4L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(6), 3L, 0L);
        cc.writeElementAtOffset(right, stepOff(right, 2), FBUtilities.toByteArray(9), 2L, 0L);

        assert ContextRelationship.DISJOINT ==
            cc.diff(left, right);
    }

    @Test
    public void testMerge()
    {
        runMerge(HeapAllocator.instance);
        runMerge(bumpedSlab(false));
        runMerge(bumpedSlab(true));
    }

    private void runMerge(Allocator allocator)
    {
        // note: local counts aggregated; remote counts are reconciled (i.e. take max)
        ByteBuffer left = allocator.allocate(4 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(1), 1L, 1L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(2), 2L, 2L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(4), 6L, 3L);
        cc.writeElementAtOffset(
            left,
            stepOff(left, 3),
            FBUtilities.getLocalAddress().getAddress(),
            7L,
            3L);

        ByteBuffer right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(4), 4L, 4L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(5), 5L, 5L);
        cc.writeElementAtOffset(
            right,
            stepOff(right, 2),
            FBUtilities.getLocalAddress().getAddress(),
            2L,
            9L);

        ByteBuffer merged = cc.merge(left, right, allocator);

        assertEquals(5 * stepLength, merged.remaining());
        // local node id's counts are aggregated
        assertEquals(0, ByteBufferUtil.compareSubArrays(
            ByteBuffer.wrap(FBUtilities.getLocalAddress().getAddress()),
            0,
            merged,
            stepOff(merged, 4),
            4));
        assertEquals(  9L, merged.getLong(stepOff(merged, 4) + idLength));
        assertEquals(12L,  merged.getLong(stepOff(merged, 4) + idLength + clockLength));

        // remote node id counts are reconciled (i.e. take max)
        assertEquals( 4,   merged.getInt( stepOff(merged, 2)));
        assertEquals( 6L,  merged.getLong(stepOff(merged, 2) + idLength));
        assertEquals( 3L,  merged.getLong(stepOff(merged, 2) + idLength + clockLength));

        assertEquals( 5,   merged.getInt( stepOff(merged, 3)));
        assertEquals( 5L,  merged.getLong(stepOff(merged, 3) + idLength));
        assertEquals( 5L,  merged.getLong(stepOff(merged, 3) + idLength + clockLength));

        assertEquals( 2,   merged.getInt( stepOff(merged, 1)));
        assertEquals( 2L,  merged.getLong(stepOff(merged, 1) + idLength));
        assertEquals( 2L,  merged.getLong(stepOff(merged, 1) + idLength + clockLength));

        assertEquals( 1,   merged.getInt( stepOff(merged, 0)));
        assertEquals( 1L,  merged.getLong(stepOff(merged, 0) + idLength));
        assertEquals( 1L,  merged.getLong(stepOff(merged, 0) + idLength + clockLength));
    }

    @Test
    public void testTotal()
    {
        runTotal(HeapAllocator.instance);
        runTotal(bumpedSlab(false));
        runTotal(bumpedSlab(true));
    }

    private void runTotal(Allocator allocator)
    {
        ByteBuffer left = allocator.allocate(4 * stepLength);
        cc.writeElementAtOffset(left, stepOff(left, 0), FBUtilities.toByteArray(1), 1L, 1L);
        cc.writeElementAtOffset(left, stepOff(left, 1), FBUtilities.toByteArray(2), 2L, 2L);
        cc.writeElementAtOffset(left, stepOff(left, 2), FBUtilities.toByteArray(4), 3L, 3L);
        cc.writeElementAtOffset(
            left,
            stepOff(left, 3),
            FBUtilities.getLocalAddress().getAddress(),
            3L,
            3L);

        ByteBuffer right = allocator.allocate(3 * stepLength);
        cc.writeElementAtOffset(right, stepOff(right, 0), FBUtilities.toByteArray(4), 4L, 4L);
        cc.writeElementAtOffset(right, stepOff(right, 1), FBUtilities.toByteArray(5), 5L, 5L);
        cc.writeElementAtOffset(
            right,
            stepOff(right, 2),
            FBUtilities.getLocalAddress().getAddress(),
            9L,
            9L);

        ByteBuffer merged = cc.merge(left, right, HeapAllocator.instance);

        // 127.0.0.1: 12 (3+9)
        // 0.0.0.1:    1
        // 0.0.0.2:    2
        // 0.0.0.4:    4
        // 0.0.0.5:    5

        assertEquals(24L, cc.total(merged));
    }

    @Test
    public void testCleanNodeCounts() throws UnknownHostException
    {
        runCleanNodeCounts(HeapAllocator.instance);
        runCleanNodeCounts(bumpedSlab(false));
        runCleanNodeCounts(bumpedSlab(true));
    }

    private void runCleanNodeCounts(Allocator allocator) throws UnknownHostException
    {
        ByteBuffer bytes = allocator.allocate(4 * stepLength);
        cc.writeElementAtOffset(bytes, stepOff(bytes, 0), FBUtilities.toByteArray(1), 1L, 1L);
        cc.writeElementAtOffset(bytes, stepOff(bytes, 1), FBUtilities.toByteArray(2), 2L, 2L);
        cc.writeElementAtOffset(bytes, stepOff(bytes, 2), FBUtilities.toByteArray(4), 3L, 3L);
        cc.writeElementAtOffset(bytes, stepOff(bytes, 3), FBUtilities.toByteArray(8), 4L, 4L);

        assertEquals(4, bytes.getInt(stepOff(bytes, 2)));
        assertEquals(3L, bytes.getLong(stepOff(bytes, 2) + idLength));

        bytes = cc.cleanNodeCounts(bytes, InetAddress.getByAddress(FBUtilities.toByteArray(4)));

        // node: 0.0.0.4 should be removed
        assertEquals(3 * stepLength, bytes.remaining());

        // other nodes should be unaffected
        assertEquals(1,  bytes.getInt(stepOff(bytes, 0)));
        assertEquals(1L, bytes.getLong(stepOff(bytes, 0) + idLength));

        assertEquals(2,  bytes.getInt(stepOff(bytes, 1)));
        assertEquals(2L, bytes.getLong(stepOff(bytes, 1) + idLength));

        assertEquals(8,  bytes.getInt(stepOff(bytes, 2)));
        assertEquals(4L, bytes.getLong(stepOff(bytes, 2) + idLength));
    }
}
