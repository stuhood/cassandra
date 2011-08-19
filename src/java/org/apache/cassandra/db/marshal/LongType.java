package org.apache.cassandra.db.marshal;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LongType extends AbstractType<Long>
{
    public static final LongType instance = new LongType();

    LongType() {} // singleton

    public Long compose(ByteBuffer bytes)
    {
        return ByteBufferUtil.toLong(bytes);
    }

    public ByteBuffer decompose(Long value)
    {
        return ByteBufferUtil.bytes(value);
    }

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        if (o1.remaining() == 0)
        {
            return o2.remaining() == 0 ? 0 : -1;
        }
        if (o2.remaining() == 0)
        {
            return 1;
        }

        int diff = o1.get(o1.position()) - o2.get(o2.position());
        if (diff != 0)
            return diff;
        
       
        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    public String getString(ByteBuffer bytes)
    {
        if (bytes.remaining() == 0)
        {
            return "";
        }
        if (bytes.remaining() != 8)
        {
            throw new MarshalException("A long is exactly 8 bytes: "+bytes.remaining());
        }
        
        return String.valueOf(bytes.getLong(bytes.position()));
    }

    public String toString(Long l)
    {
        return l.toString();
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        long longType;

        try
        {
            longType = Long.parseLong(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("unable to make long from '%s'", source), e);
        }

        return decompose(longType);
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 8 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 8 or 0 byte long (%d)", bytes.remaining()));
    }

    public Class<Long> getType()
    {
        return Long.class;
    }

    public boolean isSigned()
    {
        return true;
    }

    public boolean isCaseSensitive()
    {
        return false;
    }

    public boolean isCurrency()
    {
        return false;
    }

    public int getPrecision(Long obj)
    {
        return obj.toString().length();
    }

    public int getScale(Long obj)
    {
        return 0;
    }

    public int getJdbcType()
    {
        return Types.INTEGER;
    }

    public boolean needsQuotes()
    {
        return false;
    }

    @Override
    public ByteBuffer compress(Descriptor desc, final List<ByteBuffer> from, ByteBuffer to) throws IOException
    {
        // write via the LongType primitive
        return LongType.encode(new LongType.LongCollection(from.size())
        {
            public long get(int i)
            {
                return ByteBufferUtil.toLong(from.get(i));
            }
        }, to);
    }

    @Override
    public void decompress(Descriptor desc, ByteBuffer from, Collection<ByteBuffer> to) throws IOException
    {
        to.clear();
        for (long val : LongType.decode(from))
            to.add(ByteBufferUtil.bytes(val));
    }

    /** Delta and varint encoding: a primitive for use in other implementations. */
    public static ByteBuffer encode(LongCollection from, ByteBuffer to) throws IOException
    {
        // allocate (or reuse) a buffer large enough for the worst case size
        to = ByteBufferUtil.ensureRemaining(to, (from.size() + 1) * 10, true);
        // encode
        byte[] buff = to.array();
        int pos = encodeLong(from.size(), buff, to.position());
        long previous = 0;
        for (int i = 0; i < from.size(); i++)
        {
            long cur = from.get(i);
            pos = encodeLong(cur - previous, buff, pos);
            previous = cur;
        }
        // set the buffer position to the length of the written data
        to.position(pos);
        return to;
    }

    /** Delta and varint encoding: a primitive for use in other implementations. */
    public static long[] decode(ByteBuffer from) throws IOException
    {
        long count = decodeLong(from);
        if (count < 0 || count > Integer.MAX_VALUE)
            throw new IOException("Invalid size for array: " + count);
        long[] to = new long[(int)count];
        long previous = 0;
        for (int i = 0; i < count; i++)
        {
            long cur = decodeLong(from);
            to[i] = cur + previous;
            previous = to[i];
        }
        return to;
    }

    /** TODO: This is exposed as BinaryData.encodeLong in Avro 1.5 */
    private static int encodeLong(long n, byte[] b, int pos)
    {
        n = (n << 1) ^ (n >> 63); // move sign to low-order bit
        while ((n & ~0x7F) != 0) {
            b[pos++] = (byte)((n & 0x7f) | 0x80);
            n >>>= 7;
        }
        b[pos++] = (byte) n;
        return pos;
    }

    /** From Avro's decoder implementation: symmetrical to BinaryData.encodeLong. */
    private static long decodeLong(ByteBuffer in) throws IOException
    {
        long n = 0;
        byte b;
        int shift = 0;
        do
        {
            b = in.get();
            n |= (b & 0x7FL) << shift;
            if ((b & 0x80) == 0)
                return (n >>> 1) ^ -(n & 1); // back to two's-complement
            shift += 7;
        }
        while (shift < 64);
        throw new IOException("Invalid long encoding");
    }

    /** A primitive collection supertype to avoid boxing. */
    public static abstract class LongCollection
    {
        private final int size;
        public LongCollection(int size)
        {
            this.size = size;
        }

        public abstract long get(int i);

        public int size()
        {
            return size;
        }
    }
}
