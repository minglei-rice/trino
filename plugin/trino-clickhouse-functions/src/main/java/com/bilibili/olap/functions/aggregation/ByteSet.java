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

public class ByteSet
        extends SmallSet
{
    byte[] bytes;

    public ByteSet(int capacity)
    {
        super(capacity);
        bytes = new byte[capacity];
    }

    @Override
    public <T> T getRawArray(Class<T> clazz)
    {
        return clazz.cast(bytes);
    }

    @Override
    public int[] toIntArray()
    {
        int[] newInts = new int[size];
        for (int i = 0; i < newInts.length; i++) {
            newInts[i] = bytes[i];
        }
        return newInts;
    }

    @Override
    public int find(Object key)
    {
        byte v = (byte) key;
        return find(v);
    }

    @Override
    public void insert(Object key)
    {
        byte v = (byte) key;
        insert(v);
    }

    @Override
    public <T> void forEach(Class<T> clazz, Consumer<T> consumer)
    {
        for (int i = 0; i < size; i++) {
            consumer.accept(clazz.cast(bytes[i]));
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return super.getRetainedSizeInBytes() + sizeOf(bytes);
    }

    @Override
    public void doRead(Slice slice)
    {
        slice.getBytes(0, bytes, 0, size);
    }

    public int find(byte key)
    {
        for (int i = 0; i < size; i++) {
            if (key == bytes[i]) {
                return i;
            }
        }
        return size;
    }

    public void insert(byte key)
    {
        int pos = find(key);

        if (pos < size) {
            // inserted
            return;
        }
        bytes[pos] = key;
        size++;
    }
}
