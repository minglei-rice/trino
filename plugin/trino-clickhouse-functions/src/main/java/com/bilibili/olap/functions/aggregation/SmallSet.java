/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bilibili.olap.functions.aggregation;

import io.airlift.slice.Slice;

import java.util.function.Consumer;

public abstract class SmallSet
{
    protected int capacity;
    protected int size;

    public SmallSet(int capacity)
    {
        this.capacity = capacity;
    }

    public boolean full()
    {
        return size == capacity;
    }

    public boolean empty()
    {
        return size == 0;
    }

    public int getCapacity()
    {
        return capacity;
    }

    public int getSize()
    {
        return size;
    }

    public abstract <T> T getRawArray(Class<T> clazz);

    public abstract int[] toIntArray();

    public abstract int find(Object key);

    public abstract void insert(Object key);

    public abstract <T> void forEach(Class<T> clazz, Consumer<T> consumer);

    public long getRetainedSizeInBytes()
    {
        return 2 * Integer.BYTES;
    }

    public void read(Slice slice)
    {
        int cardinality = slice.getByte(0);

        if (cardinality > getCapacity()) {
            throw new RuntimeException("The input slice size exceeds the current capacity " + getCapacity());
        }
        size = cardinality;
        doRead(slice.slice(1, slice.length() - 1));
    }

    protected abstract void doRead(Slice slice);
}
