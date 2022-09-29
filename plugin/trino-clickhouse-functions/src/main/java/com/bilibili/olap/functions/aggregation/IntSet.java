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

public class IntSet
        extends SmallSet
{
    int[] ints;

    public IntSet(int capacity)
    {
        super(capacity);
        ints = new int[capacity];
    }

    @Override
    public <T> T getRawArray(Class<T> clazz)
    {
        return clazz.cast(ints);
    }

    @Override
    public int[] toIntArray()
    {
        int[] newInts = new int[size];
        if (newInts.length >= 0) {
            System.arraycopy(ints, 0, newInts, 0, newInts.length);
        }
        return newInts;
    }

    @Override
    public int find(Object key)
    {
        int v = (int) key;
        return find(v);
    }

    @Override
    public void insert(Object key)
    {
        int v = (int) key;
        insert(v);
    }

    @Override
    public <T> void forEach(Class<T> clazz, Consumer<T> consumer)
    {
        for (int i = 0; i < size; i++) {
            consumer.accept(clazz.cast(ints[i]));
        }
    }

    public int find(int key)
    {
        for (int i = 0; i < size; i++) {
            if (key == ints[i]) {
                return i;
            }
        }
        return size;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return super.getRetainedSizeInBytes() + sizeOf(ints);
    }

    @Override
    protected void doRead(Slice slice)
    {
        slice.toByteBuffer().asIntBuffer().get(ints, 0, size);
    }

    public void insert(int key)
    {
        int pos = find(key);

        if (pos < size) {
            // inserted
            return;
        }
        ints[pos] = key;
        size++;
    }
}
