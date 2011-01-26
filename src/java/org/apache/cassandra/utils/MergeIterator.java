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

import java.io.IOException;
import java.io.IOError;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Ordering;

public class MergeIterator<E> extends AbstractIterator<E> implements CloseableIterator<E>
{
    public final Comparator<E> comp;
    public final List<? extends CloseableIterator<E>> iterators;
    // a queue for return: all candidates must be open and have at least one item
    protected final PriorityQueue<Candidate> queue;
    // the last returned candidate, so that we can lazily call 'advance()'
    protected Candidate candidate;

    public MergeIterator(List<? extends CloseableIterator<E>> iters)
    {
        this(iters, (Comparator<E>)Ordering.natural());
    }

    public MergeIterator(List<? extends CloseableIterator<E>> iters, Comparator<E> comp)
    {
        this.iterators = iters;
        this.comp = comp;
        this.queue = new PriorityQueue<Candidate>(Math.max(1, iters.size()));
        for (CloseableIterator<E> iter : iters)
        {
            Candidate candidate = new Candidate(iter);
            if (!candidate.advance())
                // was empty: is now closed
                continue;
            this.queue.add(candidate);
        }
    }

    protected E computeNext()
    {
        if (candidate != null && candidate.advance())
            // has more items
            this.queue.add(candidate);
        candidate = this.queue.poll();
        if (candidate == null)
            return endOfData();
        return candidate.item;
    }

    public void close()
    {
        for (CloseableIterator<E> iterator : this.iterators)
        {
            try
            {
                iterator.close();
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    // Holds and is comparable by the head item of an iterator it owns
    protected final class Candidate implements Comparable<Candidate>
    {
        private final CloseableIterator<E> iter;
        private E item;
        public Candidate(CloseableIterator<E> iter)
        {
            this.iter = iter;
        }

        /** @return True if our iterator had an item, and it is now available */
        public boolean advance()
        {
            if (!iter.hasNext())
                return false;
            item = iter.next();
            return true;
        }

        public int compareTo(Candidate that)
        {
            return comp.compare(this.item, that.item);
        }
    }
}
