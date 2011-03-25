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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.sstable.Descriptor;
import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Specifies a Comparator for a specific type of ByteBuffer.
 *
 * Note that empty ByteBuffer are used to represent "start at the beginning"
 * or "stop at the end" arguments to get_slice, so the Comparator
 * should always handle those values even if they normally do not
 * represent a valid ByteBuffer for the type being compared.
 */
public abstract class AbstractType<T> implements Comparator<ByteBuffer>
{
    public final Comparator<IndexInfo> indexComparator;
    public final Comparator<IndexInfo> indexReverseComparator;
    public final Comparator<IColumn> columnComparator;
    public final Comparator<IColumn> columnReverseComparator;
    public final Comparator<ByteBuffer> reverseComparator;

    protected AbstractType()
    {
        indexComparator = new Comparator<IndexInfo>()
        {
            public int compare(IndexInfo o1, IndexInfo o2)
            {
                return AbstractType.this.compare(o1.lastName, o2.lastName);
            }
        };
        indexReverseComparator = new Comparator<IndexInfo>()
        {
            public int compare(IndexInfo o1, IndexInfo o2)
            {
                return AbstractType.this.compare(o1.firstName, o2.firstName);
            }
        };
        columnComparator = new Comparator<IColumn>()
        {
            public int compare(IColumn c1, IColumn c2)
            {
                return AbstractType.this.compare(c1.name(), c2.name());
            }
        };
        columnReverseComparator = new Comparator<IColumn>()
        {
            public int compare(IColumn c1, IColumn c2)
            {
                return AbstractType.this.compare(c2.name(), c1.name());
            }
        };
        reverseComparator = new Comparator<ByteBuffer>()
        {
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

                return -AbstractType.this.compare(o1, o2);
            }
        };
    }
    
    public abstract T compose(ByteBuffer bytes);
    
    public abstract ByteBuffer decompose(T value);

    /** get a string representation of a particular type. */
    public abstract String toString(T t);

    /** get a string representation of the bytes suitable for log messages */
    public abstract String getString(ByteBuffer bytes);

    /** get a byte representation of the given string.
     *  defaults to unsupportedoperation so people deploying custom Types can update at their leisure. */
    public ByteBuffer fromString(String source) throws MarshalException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Compresses a group.
     * This default implementation uses LZF compression.
     * Both compress and decompress must be overriden if either is.
     * @param desc A sstable Descriptor for versioning
     * @param from A collection to compress values from
     * @param to A ByteBuffer to append to, or null
     * @return A compressed buffer positioned at its limit, with content starting at 0
     */
    public ByteBuffer compress(Descriptor desc, final List<ByteBuffer> from, ByteBuffer to) throws IOException
    {
        assert !desc.isFromTheFuture();
        // copy input buffers into a contiguous buffer, and compress them
        long length = 0;
        for (ByteBuffer f : from)
            length += f.remaining();
        assert length <= Integer.MAX_VALUE : "Total size of input buffers must be less than 2GB";
        byte[] uncompressed = new byte[(int)length];
        for (int fromidx = 0, toidx = 0; fromidx < from.size(); fromidx++)
        {
            ByteBuffer cur = from.get(fromidx);
            ByteBufferUtil.arrayCopy(cur, cur.position(), uncompressed, toidx, cur.remaining());
            toidx += cur.remaining();
        }
        final byte[] compressed = LZFEncoder.encode(uncompressed);
        // delta encode individual buffer lengths and the compressed total length
        to = LongType.encode(new LongType.LongCollection(from.size() + 1)
        {
            public long get(int i)
            {
                return i == from.size() ? compressed.length : from.get(i).remaining();
            }
        }, to);
        // append compressed data
        to = ByteBufferUtil.ensureRemaining(to, compressed.length, false);
        to.put(compressed);
        return to;
    }

    /**
     * Decompresses a group.
     * Both compress and decompress must be overriden if either is.
     * @param desc A sstable Descriptor for versioning
     * @param from A buffer to decompress values from: will be consumed
     * @param to An output collection: buffers will _not_ reused
     */
    public void decompress(Descriptor desc, ByteBuffer from, Collection<ByteBuffer> to) throws IOException
    {
        assert !desc.isFromTheFuture();
        // read individual buffer lengths and compressed total length
        long[] lengths = LongType.decode(from);
        int lcount = lengths.length - 1;
        assert lengths[lcount] < Integer.MAX_VALUE :
            "Invalid buffer lengths: " + Arrays.toString(lengths);
        // copy into compressed buffer
        byte[] compressed = new byte[(int)lengths[lcount]];
        from.get(compressed);
        byte[] uncompressed = LZFDecoder.decode(compressed);
        // create a new ByteBuffer for each of the individual lengths
        to.clear();
        int offset = 0;
        for (int i = 0; i < lcount; i++)
        {
            long length = lengths[i];
            if (length < 0 || length > Integer.MAX_VALUE)
                throw new IOException("Invalid buffer lengths: " + Arrays.toString(lengths));
            to.add(ByteBuffer.wrap(uncompressed, offset, (int)length));
            offset += length;
        }
        assert offset == uncompressed.length : "Invalid buffer length.";
    }

    /* validate that the byte array is a valid sequence for the type we are supposed to be comparing */
    public abstract void validate(ByteBuffer bytes) throws MarshalException;

    /** @deprecated; use reverseComparator field instead */
    public Comparator<ByteBuffer> getReverseComparator()
    {
        return reverseComparator;
    }

    /* convenience method */
    public String getString(Collection<ByteBuffer> names)
    {
        StringBuilder builder = new StringBuilder();
        for (ByteBuffer name : names)
        {
            builder.append(getString(name)).append(",");
        }
        return builder.toString();
    }

    /* convenience method */
    public String getColumnsString(Collection<IColumn> columns)
    {
        StringBuilder builder = new StringBuilder();
        for (IColumn column : columns)
        {
            builder.append(column.getString(this)).append(",");
        }
        return builder.toString();
    }

    public boolean isCommutative()
    {
        return false;
    }
    
    /** returns the class this AbstractType represents. */
    public abstract Class<T> getType();

    //
    // JDBC metadata
    //

    public abstract boolean isSigned();
    public abstract boolean isCaseSensitive();
    public abstract boolean isCurrency();
    public abstract int getPrecision(T obj);
    public abstract int getScale(T obj);
    public abstract int getJdbcType();
    public abstract boolean needsQuotes();

    public static AbstractType parseDefaultParameters(AbstractType baseType, TypeParser parser) throws ConfigurationException
    {
        Map<String, String> parameters = parser.getKeyValueParameters();
        String reversed = parameters.get("reversed");
        if (reversed != null && (reversed.isEmpty() || reversed.equals("true")))
        {
            return ReversedType.getInstance(baseType);
        }
        else
        {
            return baseType;
        }
    }

    /**
     * This must be overriden by subclasses if necessary so that for any
     * AbstractType, this == TypeParser.parse(toString()).
     */
    @Override
    public String toString()
    {
        return getClass().getName();
    }
}
