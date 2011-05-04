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
package org.apache.cassandra.db.marshal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import static org.apache.cassandra.db.context.CounterContext.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.BoundedBitSet;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NodeId;

public class CounterColumnType extends AbstractCommutativeType
{
    public static final CounterColumnType instance = new CounterColumnType();

    CounterColumnType() {} // singleton

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        if (o1 == null)
            return null == o2 ?  0 : -1;

        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    public String getString(ByteBuffer bytes)
    {
        return ByteBufferUtil.bytesToHex(bytes);
    }

    public String toString(Long l)
    {
        return l.toString();
    }

    /**
     * create commutative column
     */
    public Column createColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        return new CounterUpdateColumn(name, value, timestamp);
    }

    public ByteBuffer fromString(String source)
    {
        return ByteBufferUtil.hexToBytes(source);
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 8 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 8 or 0 byte long (%d)", bytes.remaining()));
    }

    /**
     * Counter contexts are compressed as:
     *   1) a short count of nodes
     *   2) "count" nodeids
     *   3) a bitset containing 1 or 2 bits that indicate whether a value is present for a particular counter:
     *      1 : a delta value is present
     *      01: a non-delta value is present
     *      00: no value is present
     *   4) an array of clocks
     *   5) an array of values
     * The presence/type bits, clocks and values are each ordered by counter index, and then by nodeid.
     */
    @Override
    public ByteBuffer compress(Descriptor desc, final List<ByteBuffer> from, ByteBuffer to) throws IOException
    {
        assert !desc.isFromTheFuture();

        // one pass to collect and sort nodeids
        TreeSet<NodeId> nodes = new TreeSet<NodeId>();
        List<ContextState> contexts = new ArrayList<ContextState>(from.size());
        for (ByteBuffer buff : from)
        {
            ContextState context = new ContextState(buff);
            while (context.hasRemaining())
            {
                nodes.add(context.getNodeId());
                context.moveToNext();
            }
            context.reset();
            contexts.add(context);
        }

        // create output array(s)/bitset large enough to hold the worst case
        int worst = nodes.size() * from.size();
        BoundedBitSet bits = new BoundedBitSet(2 * worst);
        final long[] clocks = new long[worst];
        final long[] values = new long[worst];
        // append contexts in order to the output
        int valCount = 0;
        for (ContextState context : contexts)
        {
            // each item in the complete list of nodes means entries in the bitset
            for (NodeId node : nodes)
            {
                int comp = context.hasRemaining() ? node.compareTo(context.getNodeId()) : -1;
                if (comp < 0)
                {
                    // the context does not have a value for this node
                    bits.unset().unset(); // 00
                    continue;
                }
                assert comp == 0 : "Missed a node in the first pass?";
                // the context has a value for this node: add the content
                if (context.isDelta())
                    bits.set(); // 1
                else
                    bits.unset().set(); // 01
                clocks[valCount] = context.getClock();
                values[valCount] = context.getCount();
                valCount++;
                context.moveToNext();
            }
        }

        // store the interesting part of each collection (valCount values)
        assert nodes.size() <= Short.MAX_VALUE;
        to = ByteBufferUtil.ensureRemaining(to, 2 + (nodes.size() * NodeId.LENGTH), false);
        to.putShort((short)nodes.size());
        for (NodeId node : nodes)
            to.put(node.bytes());
        to = LongType.encode(bits.asLongCollection(), to);
        to = LongType.encode(new LongType.LongCollection(valCount)
        {
            public long get(int i)
            {
                return clocks[i];
            }
        }, to);
        to = LongType.encode(new LongType.LongCollection(valCount)
        {
            public long get(int i)
            {
                return values[i];
            }
        }, to);
        return to;
    }

    @Override
    public void decompress(Descriptor desc, ByteBuffer from, Collection<ByteBuffer> to) throws IOException
    {
        assert !desc.isFromTheFuture();
        // consume nodeids into a (implicitly sorted) list
        int nodeIdCount = from.getShort();
        int nodeIdsLength = nodeIdCount * NodeId.LENGTH;
        assert 0 < nodeIdCount && nodeIdCount <= Short.MAX_VALUE;
        List<NodeId> nodes = new ArrayList<NodeId>();
        for (int i = 0; i < nodeIdCount; i++)
            // NodeId.wrap() duplicates the given ByteBuffer
            nodes.add(NodeId.wrap(from, from.position() + (i * NodeId.LENGTH)));
        from.position(from.position() + nodeIdsLength);
        // decode counter content
        BoundedBitSet bits = BoundedBitSet.from(LongType.decode(from));
        long[] clocks = LongType.decode(from);
        long[] values = LongType.decode(from);

        // decode each counter into worst-case-size buffers, and then copy to fit
        ByteBuffer header = ByteBuffer.allocate(HEADER_ELT_LENGTH * nodeIdCount);
        ByteBuffer body = ByteBuffer.allocate(STEP_LENGTH * nodeIdCount);
        to.clear();
        for (int bitidx = 0, cvidx = 0; bitidx < bits.index();)
        {
            short elts = 0;
            short deltas = 0;
            header.position(0).limit(header.capacity());
            body.position(0).limit(body.capacity());
            for (NodeId node : nodes)
            {
                // for this counter, is there a value for this node?
                if (bits.get(bitidx++))
                {
                    // 1: a delta value is present: mark it in the header
                    header.putShort(elts);
                    deltas++;
                }
                else if (!bits.get(bitidx++))
                    // 00: no value is present
                    continue;
                // else: 01: a non-delta value is present

                // write the elt in the body, and increment the position
                writeElementAtOffset(body,
                                     body.position(),
                                     node,
                                     clocks[cvidx],
                                     values[cvidx]);
                body.position(body.position() + STEP_LENGTH);
                cvidx++;
                elts++;
            }

            // concat into an output buffer
            header.flip();
            body.flip();
            ByteBuffer output = ByteBuffer.allocate(HEADER_SIZE_LENGTH + header.remaining() + body.remaining());
            output.putShort(deltas).put(header).put(body).rewind();
            to.add(output);
        }
    }
}
