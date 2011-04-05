package org.apache.cassandra.db;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;

import org.apache.cassandra.io.AbstractCompactedRow;
import org.apache.cassandra.io.sstable.Observer;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;

/**
 * A CompactedRow implementation that just echos the original row bytes without deserializing.
 */
public class EchoedRow extends AbstractCompactedRow
{
    private final SSTableIdentityIterator row;

    public EchoedRow(SSTableIdentityIterator row)
    {
        super(row.getKey());
        this.row = row;
    }

    public ColumnFamily getMetadata()
    {
        return row.getColumnFamily();
    }

    public void write(RandomAccessFile out, Observer observer) throws IOException
    {
        assert row.dataSize > 0;
        out.writeLong(row.dataSize);
        row.echoData(out);
    }

    public void update(MessageDigest digest)
    {
        // EchoedRow is not used in anti-entropy validation
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty()
    {
        return !row.hasNext();
    }

    public int columnCount()
    {
        return row.columnCount;
    }
}
