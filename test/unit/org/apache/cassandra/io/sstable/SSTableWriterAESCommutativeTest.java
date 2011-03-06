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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SSTableWriterAESCommutativeTest extends CleanupHelper
{
    private static final CounterContext cc = new CounterContext();
    private static final CounterColumnType ctype = CounterColumnType.instance;

    @Test
    public void testRecoverAndOpenAESCommutative() throws IOException, ExecutionException, InterruptedException, UnknownHostException
    {
        String keyspace = "Keyspace1";
        String cfname   = "Counter1";

        Map<ByteBuffer, ByteBuffer> entries = new HashMap<ByteBuffer, ByteBuffer>();
        Map<ByteBuffer, ByteBuffer> cleanedEntries = new HashMap<ByteBuffer, ByteBuffer>();

        DataOutputBuffer buffer;

        ColumnFamily cf = ColumnFamily.create(keyspace, cfname);
        byte[] context;

        // key: k
        context = Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(),
                FBUtilities.toByteArray(9L),
                FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(2),  FBUtilities.toByteArray(4L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(4),  FBUtilities.toByteArray(3L), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(8),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(4L)
            );
        cf.addColumn(new CounterColumn(
            ByteBufferUtil.bytes("x"),
            ByteBuffer.wrap(context),
            0L));
        context = Util.concatByteArrays(
            FBUtilities.toByteArray(1),  FBUtilities.toByteArray(7L), FBUtilities.toByteArray(12L),
            FBUtilities.getLocalAddress().getAddress(),
                FBUtilities.toByteArray(5L),
                FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(3),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(33L),
            FBUtilities.toByteArray(9),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(24L)
            );
        cf.addColumn(new CounterColumn(
            ByteBufferUtil.bytes("y"),
            ByteBuffer.wrap(context),
            0L));

        buffer = new DataOutputBuffer();
        ColumnFamily.serializer().serializeWithIndexes(cf, buffer);
        entries.put(
            ByteBufferUtil.bytes("k"),
            ByteBuffer.wrap(Arrays.copyOf(buffer.getData(), buffer.getLength()))
            );

        ctype.cleanContext(cf, FBUtilities.getLocalAddress());
        buffer = new DataOutputBuffer();
        ColumnFamily.serializer().serializeWithIndexes(cf, buffer);
        cleanedEntries.put(
            ByteBufferUtil.bytes("k"),
            ByteBuffer.wrap(Arrays.copyOf(buffer.getData(), buffer.getLength()))
            );
        
        cf.clear();

        // key: l
        context = Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(),
                FBUtilities.toByteArray(9L),
                FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(2),  FBUtilities.toByteArray(4L), FBUtilities.toByteArray(2L),
            FBUtilities.toByteArray(4),  FBUtilities.toByteArray(3L), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(8),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(4L)
            );
        cf.addColumn(new CounterColumn(
            ByteBufferUtil.bytes("x"),
            ByteBuffer.wrap(context),
            0L));
        context = Util.concatByteArrays(
            FBUtilities.toByteArray(1),  FBUtilities.toByteArray(7L), FBUtilities.toByteArray(12L),
            FBUtilities.toByteArray(3),  FBUtilities.toByteArray(2L), FBUtilities.toByteArray(33L),
            FBUtilities.toByteArray(9),  FBUtilities.toByteArray(1L), FBUtilities.toByteArray(24L)
            );
        cf.addColumn(new CounterColumn(
            ByteBufferUtil.bytes("y"),
            ByteBuffer.wrap(context),
            0L));

        buffer = new DataOutputBuffer();
        ColumnFamily.serializer().serializeWithIndexes(cf, buffer);
        entries.put(
            ByteBufferUtil.bytes("l"),
            ByteBuffer.wrap(Arrays.copyOf(buffer.getData(), buffer.getLength()))
            );

        ctype.cleanContext(cf, FBUtilities.getLocalAddress());
        buffer = new DataOutputBuffer();
        ColumnFamily.serializer().serializeWithIndexes(cf, buffer);
        cleanedEntries.put(
            ByteBufferUtil.bytes("l"),
            ByteBuffer.wrap(Arrays.copyOf(buffer.getData(), buffer.getLength()))
            );

        cf.clear();

        // write out unmodified CF
        SSTableReader orig = SSTableUtils.prepare().ks(keyspace).cf(cfname).generation(0).writeRaw(entries);

        // whack the index to trigger the recover
        FileUtils.deleteWithConfirm(orig.descriptor.filenameFor(Component.PRIMARY_INDEX));
        FileUtils.deleteWithConfirm(orig.descriptor.filenameFor(Component.FILTER));

        // re-build inline
        ColumnFamilyStore cfs = Table.open(keyspace).getColumnFamilyStore(cfname);
        SSTableReader rebuilt = SSTableReader.open(CompactionManager.instance.submitSSTableBuild(
            cfs,
            orig.descriptor,
            Component.INDEX_TYPES,
            OperationType.AES
            ).get().left);

        // write out cleaned CF
        SSTableReader cleaned = SSTableUtils.prepare().ks(keyspace).cf(cfname).generation(0).writeRaw(cleanedEntries);

        // verify
        BufferedRandomAccessFile origFile    = new BufferedRandomAccessFile(orig.descriptor.filenameFor(SSTable.COMPONENT_DATA), "r", 8 * 1024 * 1024);
        BufferedRandomAccessFile cleanedFile = new BufferedRandomAccessFile(cleaned.descriptor.filenameFor(SSTable.COMPONENT_DATA), "r", 8 * 1024 * 1024);

        while(origFile.getFilePointer() < origFile.length() && cleanedFile.getFilePointer() < cleanedFile.length())
        {
            assert origFile.readByte() == cleanedFile.readByte();
        }
        assert origFile.getFilePointer() == origFile.length();
        assert cleanedFile.getFilePointer() == cleanedFile.length();
    }
}
