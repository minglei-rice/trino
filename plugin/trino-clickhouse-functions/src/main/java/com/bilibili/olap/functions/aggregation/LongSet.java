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

import static io.airlift.slice.SizeOf.sizeOf;

public class LongSet
        extends SmallSet
{
    long[] longs;

    public LongSet(int capacity)
    {
        super(capacity);
        longs = new long[capacity];
    }

    @Override
    public <T> T getRawArray(Class<T> clazz)
    {
        return clazz.cast(longs);
    }

    @Override
    public int[] toIntArray()
    {
        int[] newInts = new int[size];
        for (int i = 0; i < newInts.length; i++) {
            newInts[i] = (int) longs[i];
        }
        return newInts;
    }

    public long[] toArray()
    {
        long[] newLongs = new long[size];
        System.arraycopy(longs, 0, newLongs, 0, size);
        return newLongs;
    }

    @Override
    public int find(Object key)
    {
        long v = (long) key;
        return find(v);
    }

    @Override
    public void insert(Object key)
    {
        long v = (long) key;
        insert(v);
    }

    @Override
    public <T> void forEach(Class<T> clazz, Consumer<T> consumer)
    {
        for (int i = 0; i < size; i++) {
            consumer.accept(clazz.cast(longs[i]));
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return super.getRetainedSizeInBytes() + sizeOf(longs);
    }

    @Override
    protected void doRead(Slice slice)
    {
        slice.toByteBuffer().asLongBuffer().get(longs, 0, size);
    }

    public int find(long key)
    {
        for (int i = 0; i < size; i++) {
            if (key == longs[i]) {
                return i;
            }
        }
        return size;
    }

    public void insert(long key)
    {
        int pos = find(key);

        if (pos < size) {
            // inserted
            return;
        }
        longs[pos] = key;
        size++;
    }
}
