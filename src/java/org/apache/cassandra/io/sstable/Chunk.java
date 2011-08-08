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

package org.apache.cassandra.io.sstable;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.BoundedBitSet;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MurmurHash;

/**
 * Implements lazy decoding of a chunk of content from a Span. Not thread safe.
 */
public final class Chunk
{
    public static final byte ENTRY_NULL = 0x0;
    public static final byte ENTRY_NAME = 0x1;
    public static final byte ENTRY_PARENT = 0x2;
    public static final byte ENTRY_RANGE_BEGIN = 0x3;
    public static final byte ENTRY_RANGE_END = 0x4;
    public static final byte ENTRY_RANGE_BEGIN_NULL = 0x5;
    public static final byte ENTRY_RANGE_END_NULL = 0x6;
    public static final byte ENTRY_DELETED = 0x7;
    public static final byte ENTRY_EXPIRING = 0x8;
    public static final byte ENTRY_STANDARD = 0x9;
    public static final byte ENTRY_COUNTER = 0x10;

    // written at the beginning of every chunk
    public static final byte[] MAGIC = {
        0xf, 0x2, 0xc, 0x5, 0x3, 0x4, 0xc, 0xe, 0x3, 0x3, 0x9, 0x9, 0xa, 0x1, 0xc, 0x3
    };

    // FIXME: allocate chunks with a maximum capacity
    private static final int ENTRIES = 256;

    // the version/type of values in this chunk
    public final Descriptor desc;
    public final AbstractType type;

    private ByteBuffer encoded = null;

    private boolean decoded = true;
    // a type byte per entry in this chunk
    private ByteBuffer metadata = ByteBuffer.allocate(ENTRIES);
    // lazily (en/de)coded tuples
    private final List<ByteBuffer> decodedValues = new ArrayList<ByteBuffer>(ENTRIES);
    // client and local timestamps with null bitsets representing MIN_VALUE
    private List<Long> clientTimestamps = new ArrayList<Long>(ENTRIES);
    private BoundedBitSet clientTimestampNulls = new BoundedBitSet(ENTRIES);
    private List<Integer> localTimestamps = new ArrayList<Integer>(ENTRIES);
    private BoundedBitSet localTimestampNulls = new BoundedBitSet(ENTRIES);

    Chunk(Descriptor desc, AbstractType type)
    {
        this.desc = desc;
        this.type = type;
    }

    public void clear()
    {
        metadata.clear();
        decodedValues.clear();
        clientTimestamps.clear();
        clientTimestampNulls.clear();
        localTimestamps.clear();
        localTimestampNulls.clear();
    }

    public List<ByteBuffer> values()
    {
        maybeDecode();
        return decodedValues;
    }

    public List<Long> clientTimestamps()
    {
        maybeDecode();
        return clientTimestamps;
    }

    public BoundedBitSet clientTimestampNulls()
    {
        maybeDecode();
        return clientTimestampNulls;
    }

    /**
     * Helper that marks MIN_VALUEs as null to avoid throwing off the LongType
     * delta encoding with magic values that are way out of the normal range.
     */
    public void clientTimestampAdd(long timestamp)
    {
        if (timestamp == Long.MIN_VALUE)
            clientTimestampNulls.unset();
        else
        {
            clientTimestampNulls.set();
            clientTimestamps.add(timestamp);
        }
    }

    public List<Integer> localTimestamps()
    {
        maybeDecode();
        return localTimestamps;
    }

    public BoundedBitSet localTimestampNulls()
    {
        maybeDecode();
        return localTimestampNulls;
    }

    /** @see clientTimestampAdd */
    public void localTimestampAdd(int timestamp)
    {
        if (timestamp == Integer.MIN_VALUE)
            localTimestampNulls.unset();
        else
        {
            localTimestampNulls.set();
            localTimestamps.add(timestamp);
        }
    }

    public ByteBuffer metadata()
    {
        return metadata;
    }

    /**
     * Helper that does bounds checking and adds metadata.
     */
    public void metadataAdd(byte meta)
    {
        metadata = ByteBufferUtil.ensureRemaining(metadata, 1, false);
        metadata.put(meta);
    }

    private void maybeDecode()
    {
        if (decoded)
            return;
        try
        {
            // metadata
            // pass: was eagerly decoded
            // values
            decodedValues.clear();
            type.decompress(desc, encoded, decodedValues);
            // client timestamps
            clientTimestampNulls = BoundedBitSet.from(LongType.decode(encoded));
            clientTimestamps.clear();
            for (long l : LongType.decode(encoded))
                clientTimestamps.add(l);
            // local timestamps
            localTimestampNulls = BoundedBitSet.from(LongType.decode(encoded));
            localTimestamps.clear();
            for (long l : LongType.decode(encoded))
            {
                assert l <= Integer.MAX_VALUE : "local timestamps are seconds since epoch";
                localTimestamps.add((int)l);
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
        decoded = true;
    }

    /** Chunks will usually be appended a span at a time: see ChunkAppender. */
    void append(SequentialWriter file) throws IOException
    {
        /** encode into a single output buffer */
        if (encoded == null)
            encoded = ByteBuffer.allocate(ENTRIES << 3);
        else
            encoded.clear();
        // metadata
        metadata.flip();
        encoded = ByteBufferUtil.ensureRemaining(encoded, metadata.remaining(), false);
        encoded.putInt(metadata.remaining());
        encoded.put(metadata);
        // values
        encoded = type.compress(desc, decodedValues, encoded);
        // client timestamps (with nulls)
        encoded = LongType.encode(clientTimestampNulls.asLongCollection(), encoded);
        encoded = LongType.encode(new LongType.LongCollection(clientTimestamps.size())
        {
            public long get(int i)
            {
                return clientTimestamps.get(i);
            }
        }, encoded);
        // local timestamps (upcast to longs, with nulls)
        encoded = LongType.encode(localTimestampNulls.asLongCollection(), encoded);
        encoded = LongType.encode(new LongType.LongCollection(localTimestamps.size())
        {
            public long get(int i)
            {
                return localTimestamps.get(i);
            }
        }, encoded);

        /** write magic, flush, and append checksum */
        encoded.flip();
        file.write(MAGIC);
        ByteBufferUtil.writeWithLength(encoded, file.stream);
        file.stream.writeInt(MurmurHash.hash32(encoded,
                                               encoded.position(),
                                               encoded.remaining(),
                                               0));
    }

    /** Chunks will usually be read a span at a time: see Chunks.cursor.nextSpan(). */
    void next(DataInput input)
    {
        decoded = false;
        try
        {
            readMagic(input);
            encoded = ByteBufferUtil.readWithLength(input);
            // confirm the checksum
            int hash = input.readInt();
            if (MurmurHash.hash32(encoded, encoded.position(), encoded.remaining(), 0) != hash)
                throw new IOException("Checksum mismatch");
            // eagerly separate the metadata: leave the rest to be decoded later
            int mlen = encoded.getInt();
            int mend = encoded.position() + mlen;
            metadata = encoded.duplicate();
            metadata.limit(mend);
            encoded.position(mend);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private void readMagic(DataInput input) throws IOException
    {
        // TODO: implement resync'ing by scanning for magic
        byte[] magic = new byte[MAGIC.length];
        input.readFully(magic);
        if (!Arrays.equals(magic, MAGIC))
            throw new IOException("Not positioned at a valid block");
    }
}
