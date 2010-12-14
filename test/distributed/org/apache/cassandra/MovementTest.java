/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.WrappedRunnable;

import org.apache.cassandra.CassandraServiceController.Failure;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class MovementTest extends TestBase
{
    @Test
    public void testLoadbalance() throws Exception
    {
        final String keyspace = "TestLoadbalance";
        final int N = 1000;
        addKeyspace(keyspace, 1);
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));
        client.set_keyspace(keyspace);

        ColumnParent     cp = new ColumnParent("Standard1");
        ConsistencyLevel cl = ConsistencyLevel.ONE;
        Column col1 = new Column(
            ByteBuffer.wrap("c1".getBytes()),
            ByteBuffer.wrap("v1".getBytes()),
            0
            );
        Column col2 = new Column(
            ByteBuffer.wrap("c2".getBytes()),
            ByteBuffer.wrap("v2".getBytes()),
            0
            );

        // add N keys
        Set<ByteBuffer> keys = new HashSet<ByteBuffer>();
        for (int i = 0; i < N; i++)
        {
            String rawKey = String.format("test.key.%d", i);
            ByteBuffer key = ByteBuffer.wrap(rawKey.getBytes());
            client.insert(key, cp, col1, cl);
            client.insert(key, cp, col2, cl);
            keys.add(key);
        }
        Thread.sleep(100);

        // ask a node to move to a new location
        controller.nodetool("loadbalance", hosts.get(0));
        // trigger cleanup on all nodes
        for (InetAddress host : hosts)
            controller.nodetool("cleanup", host);

        // check that all keys still exist
        for (ByteBuffer key : keys)
        {
            // verify get
            ColumnPath cpath = new ColumnPath("Standard1");
            cpath.setColumn(col1.getName());
            assertEquals(
                client.get(key, cpath, cl).column,
                col1
                );

            // verify slice
            SlicePredicate sp = new SlicePredicate();
            sp.setSlice_range(
                new SliceRange(
                    ByteBuffer.wrap(new byte[0]),
                    ByteBuffer.wrap(new byte[0]),
                    false,
                    1000
                    )
                );
            List<ColumnOrSuperColumn> coscs = new LinkedList<ColumnOrSuperColumn>();
            coscs.add((new ColumnOrSuperColumn()).setColumn(col1));
            coscs.add((new ColumnOrSuperColumn()).setColumn(col2));
            assertEquals(
                client.get_slice(key, cp, sp, cl),
                coscs
                );
        }
    }
}
