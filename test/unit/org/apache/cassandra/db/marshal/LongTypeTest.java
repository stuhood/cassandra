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

import org.junit.Test;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LongTypeTest
{
    private static final Descriptor DESC = new Descriptor(Descriptor.CURRENT_VERSION, new File(""), "ks", "cf", 1, false);

    private ByteBuffer compress(List<ByteBuffer> contexts) throws Exception
    {
        ByteBuffer buff = LongType.instance.compress(DESC, contexts, null);
        buff.limit(buff.position()).rewind();
        return buff;
    }

    private void assertRoundTrip(List<ByteBuffer> contexts) throws Exception
    {
        List<ByteBuffer> output = new ArrayList<ByteBuffer>();
        // compress/decompress
        LongType.instance.decompress(DESC, compress(contexts), output);
        assertEquals(contexts, output);
    }

    private List<ByteBuffer> l(long... values)
    {
        ArrayList<ByteBuffer> out = new ArrayList<ByteBuffer>(values.length);
        for (long value : values)
            out.add(ByteBufferUtil.bytes(value));
        return out;
    }

    @Test
    public void testSimple() throws Exception
    {
        assertRoundTrip(l(1, 2, 3, 4, 5));
    }

    @Test
    public void testLimits() throws Exception
    {
        assertRoundTrip(l(Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE));
    }

    @Test
    public void testZero() throws Exception
    {
        assertRoundTrip(l(0, 0, 0, 0));
    }
}
