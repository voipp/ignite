/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.eviction;

import org.apache.ignite.internal.util.typedef.internal.A;
import org.jsr166.LongAdder8;

/**
 *
 */
public abstract class AbstractEvictionPolicy<K, V> implements EvictionPolicy<K, V> {

    /** Memory size occupied by elements in container. */
    protected final LongAdder8 memSize = new LongAdder8();
    /** Max memory size occupied by elements in container. */
    protected volatile long maxMemSize;
    /** Maximum elements in container. */
    protected volatile int max;
    /** Batch size. */
    protected volatile int batchSize = 1;

    /**
     * Shrinks backed container to maximum allowed size.
     */
    protected void shrink() {
        long maxMem = this.maxMemSize;

        if (maxMem > 0) {
            long startMemSize = memSize.longValue();

            if (startMemSize >= maxMem)
                for (long i = maxMem; i < startMemSize && memSize.longValue() > maxMem; ) {
                    int size = shrink0();

                    if (size == -1)
                        break;

                    i += size;
                }
        }

        int max = this.max;

        if (max > 0) {
            int startSize = getContainerSize();

            if (startSize >= max + (maxMem > 0 ? 1 : this.batchSize))
                for (int i = max; i < startSize && getContainerSize() > max; i++)
                    if (shrink0() == -1)
                        break;
        }
    }

    /** {@inheritDoc} */
    @Override public void onEntryAccessed(boolean rmv, EvictableEntry<K, V> entry) {
        if (!rmv) {
            if (!entry.isCached())
                return;

            if (touch(entry))
                shrink();
        }
        else {
            Object node = entry.removeMeta();

            if (node != null) {
                removeMeta(node);
                addToMemorySize(-entry.size());
            }

        }
    }

    /**
     * @param x changing memory size by adding the value
     */
    protected void addToMemorySize(int x) {
        memSize.add(x);
    }

    /**
     *
     * @return size of the container
     */
    protected abstract int getContainerSize();

    /**
     *
     * @return size of the memory which was shrinked0
     */
    protected abstract int shrink0();

    /**
     *
     * @param meta meta-information shipped to an entry
     * @return {@code True} if meta was successfully removed from the container
     */
    protected abstract boolean removeMeta(Object meta);

    /**
     * @param entry Entry to touch.
     * @return {@code True} if container has been changed by this call.
     */
    protected abstract boolean touch(EvictableEntry<K, V> entry);

    /** {@inheritDoc} */
    public void setMaxMemorySize(long maxMemSize) {
        A.ensure(maxMemSize >= 0, "maxMemSize >= 0");

        this.maxMemSize = maxMemSize;
    }

    /** {@inheritDoc} */
    public long getMaxMemorySize() {
        return maxMemSize;
    }

    /** {@inheritDoc} */
    public long getCurrentMemorySize() {
        return memSize.longValue();
    }

    /**
     * Sets maximum allowed size of cache before entry will start getting evicted.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    public void setMaxSize(int max) {
        A.ensure(max >= 0, "max >= 0");

        this.max = max;
    }

    /** {@inheritDoc} */
    public void setBatchSize(int batchSize) {
        A.ensure(batchSize > 0, "batchSize > 0");

        this.batchSize = batchSize;
    }


}
