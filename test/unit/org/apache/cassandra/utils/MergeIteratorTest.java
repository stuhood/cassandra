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
package org.apache.cassandra.utils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;

import org.junit.Before;
import org.junit.Test;

public class MergeIteratorTest
{
    CLI<Long> all = null, a = null, b = null, c = null, d = null;

    @Before
    public void clear()
    {
        all = new CLI(1, 2, 3, 4, 5, 6, 7, 8, 9);
        a = new CLI(1, 3, 5);
        b = new CLI(2, 4, 6);
        c = new CLI(7, 8, 9);
        d = new CLI();
    }

    @Test
    public void testWithComparator() throws Exception
    {
        MergeIterator<Long> smi = new MergeIterator<Long>(Arrays.asList(a, b, c, d),
                                                          Ordering.<Long>natural());
        assert Iterators.elementsEqual(all, smi);
        smi.close();
        assert a.closed && b.closed && c.closed && d.closed;
    }

    // closeable list iterator
    public static class CLI<E> extends AbstractIterator<E> implements CloseableIterator<E>
    {
        Iterator<E> iter;
        boolean closed = false;
        public CLI(E... items)
        {
            this.iter = Arrays.asList(items).iterator();
        }
        
        protected E computeNext()
        {
            if (!iter.hasNext()) return endOfData();
            return iter.next();
        }
        
        public void close()
        {
            assert !this.closed;
            this.closed = true;
        }
    }
}
