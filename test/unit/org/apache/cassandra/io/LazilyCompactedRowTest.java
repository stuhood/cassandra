package org.apache.cassandra.io;
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


import static junit.framework.Assert.assertEquals;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.Observer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.MappedFileDataInput;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import org.junit.Test;


public class LazilyCompactedRowTest extends CleanupHelper
{
    private void assertBytes(ColumnFamilyStore cfs, int gcBefore, boolean major) throws IOException
    {
        Collection<SSTableReader> sstables = cfs.getSSTables();
        CompactionController controller = new CompactionController(cfs, sstables, major, gcBefore, false);
        CompactionIterator ci1 = new PreCompactingIterator(sstables, controller);
        CompactionIterator ci2 = new LazyCompactionIterator(sstables, controller);
 
        // observe all content via size threshold of 1 byte (would otherwise be used for indexing)
        int depth = cfs.metadata.cfType == ColumnFamilyType.Super ? 3 : 2;
        Observer obs1 = new AllObserver(depth);
        Observer obs2 = new AllObserver(depth);

        while (true)
        {
            if (!ci1.hasNext())
            {
                assert !ci2.hasNext();
                break;
            }

            AbstractCompactedRow row1 = ci1.next();
            AbstractCompactedRow row2 = ci2.next();

            // flush each row to a temporary file
            File tmpFile1 = File.createTempFile("lcrt1", null);
            File tmpFile2 = File.createTempFile("lcrt2", null);
            tmpFile1.deleteOnExit();
            tmpFile2.deleteOnExit();
            RandomAccessFile out1 = new RandomAccessFile(tmpFile1, "rw");
            RandomAccessFile out2 = new RandomAccessFile(tmpFile2, "rw");
            row1.write(out1, obs1);
            row2.write(out2, obs2);
            long out1Length = out1.length();
            long out2Length = out2.length();
            out1.close();
            out2.close();

            // and reopen as mapped
            MappedFileDataInput in1 = new MappedFileDataInput(new FileInputStream(tmpFile1), tmpFile1.getAbsolutePath(), 0);
            MappedFileDataInput in2 = new MappedFileDataInput(new FileInputStream(tmpFile2), tmpFile2.getAbsolutePath(), 0);

            // key isn't part of what CompactedRow writes, that's done by SSTW.append

            // row size can differ b/c of bloom filter counts being different
            long rowSize1 = SSTableReader.readRowSize(in1, sstables.iterator().next().descriptor);
            long rowSize2 = SSTableReader.readRowSize(in2, sstables.iterator().next().descriptor);
            assertEquals(out1Length, rowSize1 + 8);
            assertEquals(out2Length, rowSize2 + 8);
            // bloom filter
            IndexHelper.defreezeBloomFilter(in1, rowSize1, false);
            IndexHelper.defreezeBloomFilter(in2, rowSize2, false);
            // index
            int indexSize1 = in1.readInt();
            int indexSize2 = in2.readInt();
            assertEquals(indexSize1, indexSize2);

            ByteBuffer bytes1 = in1.readBytes(indexSize1);
            ByteBuffer bytes2 = in2.readBytes(indexSize2);

            assert bytes1.equals(bytes2);

            // cf metadata
            ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
            ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
            ColumnFamily.serializer().deserializeFromSSTableNoColumns(cf1, in1);
            ColumnFamily.serializer().deserializeFromSSTableNoColumns(cf2, in2);
            assert cf1.getLocalDeletionTime() == cf2.getLocalDeletionTime();
            assert cf1.getMarkedForDeleteAt() == cf2.getMarkedForDeleteAt();   
            // columns
            int columns = in1.readInt();
            assert columns == in2.readInt();
            for (int i = 0; i < columns; i++)
            {
                IColumn c1 = cf1.getColumnSerializer().deserialize(in1);
                IColumn c2 = cf2.getColumnSerializer().deserialize(in2);
                assert c1.equals(c2);
            }
            // that should be everything
            assert in1.available() == 0;
            assert in2.available() == 0;
        }
        assertEquals(obs1.keys, obs2.keys);
        assertEquals(obs1.names, obs2.names);
        // offsets can differ because of bloom filter counts being different
        // assertEquals(obs1.offsets, obs2.offsets);
    }
    
    private void assertDigest(ColumnFamilyStore cfs, int gcBefore, boolean major) throws IOException, NoSuchAlgorithmException
    {
        Collection<SSTableReader> sstables = cfs.getSSTables();
        CompactionController controller = new CompactionController(cfs, sstables, major, gcBefore, false);
        CompactionIterator ci1 = new PreCompactingIterator(sstables, controller);
        CompactionIterator ci2 = new LazyCompactionIterator(sstables, controller);

        while (true)
        {
            if (!ci1.hasNext())
            {
                assert !ci2.hasNext();
                break;
            }

            AbstractCompactedRow row1 = ci1.next();
            AbstractCompactedRow row2 = ci2.next();
            MessageDigest digest1 = MessageDigest.getInstance("MD5");
            MessageDigest digest2 = MessageDigest.getInstance("MD5");

            row1.update(digest1);
            row2.update(digest2);

            assert MessageDigest.isEqual(digest1.digest(), digest2.digest());
        }
    }

    @Test
    public void testOneRow() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE, true);
        assertDigest(cfs, Integer.MAX_VALUE, true);
    }

    @Test
    public void testOneRowTwoColumns() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("d")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE, true);
        assertDigest(cfs, Integer.MAX_VALUE, true);
    }

    @Test
    public void testOneRowManyColumns() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        ByteBuffer key = ByteBuffer.wrap("k".getBytes());
        RowMutation rm = new RowMutation("Keyspace1", key);
        for (int i = 0; i < 1000; i++)
            rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        DataOutputBuffer out = new DataOutputBuffer();
        RowMutation.serializer().serialize(rm, out, MessagingService.version_);
        assert out.getLength() > DatabaseDescriptor.getColumnIndexSize();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE, true);
        assertDigest(cfs, Integer.MAX_VALUE, true);
    }

    @Test
    public void testTwoRows() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE, true);
        assertDigest(cfs, Integer.MAX_VALUE, true);
    }

    @Test
    public void testTwoRowsTwoColumns() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("d")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE, true);
        assertDigest(cfs, Integer.MAX_VALUE, true);
    }

    @Test
    public void testManyRows() throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");

        final int ROWS_PER_SSTABLE = 10;
        for (int j = 0; j < (DatabaseDescriptor.getIndexInterval() * 3) / ROWS_PER_SSTABLE; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(i % 2));
                RowMutation rm = new RowMutation("Keyspace1", key);
                rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes(String.valueOf(i / 2))), ByteBufferUtil.EMPTY_BYTE_BUFFER, j * ROWS_PER_SSTABLE + i);
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        assertBytes(cfs, Integer.MAX_VALUE, true);
        assertDigest(cfs, Integer.MAX_VALUE, true);
    }

    @Test
    public void testTwoRowSuperColumn() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open("Keyspace4");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Super5");

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace4", key);
        ByteBuffer scKey = ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress())));
        rm.add(new QueryPath("Super5", scKey , ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm.apply();
        cfs.forceBlockingFlush();

        assertBytes(cfs, Integer.MAX_VALUE, true);
    }

    private static class AllObserver extends Observer
    {
        public AllObserver(int depth)
        {
            super(depth, 128, 1, 0);
        }
        
        public boolean shouldAdd(int depth, boolean last)
        {
            return true;
        }
    }

    private static class LazyCompactionIterator extends CompactionIterator
    {
        public LazyCompactionIterator(Iterable<SSTableReader> sstables, CompactionController controller) throws IOException
        {
            super(sstables, controller);
        }

        @Override
        protected AbstractCompactedRow getCompactedRow()
        {
            return new LazilyCompactedRow(controller, rows);
        }
    }

    private static class PreCompactingIterator extends CompactionIterator
    {
        public PreCompactingIterator(Iterable<SSTableReader> sstables, CompactionController controller) throws IOException
        {
            super(sstables, controller);
        }

        @Override
        protected AbstractCompactedRow getCompactedRow()
        {
            return new PrecompactedRow(controller, rows);
        }
    }
}
