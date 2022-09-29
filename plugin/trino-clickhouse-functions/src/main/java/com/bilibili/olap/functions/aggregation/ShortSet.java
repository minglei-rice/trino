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

public class ShortSet
        extends SmallSet
{
    short[] shorts;

    public ShortSet(int capacity)
    {
        super(capacity);
        shorts = new short[capacity];
    }

    @Override
    public <T> T getRawArray(Class<T> clazz)
    {
        return clazz.cast(shorts);
    }

    @Override
    public int[] toIntArray()
    {
        int[] newShorts = new int[size];
        for (int i = 0; i < newShorts.length; i++) {
            newShorts[i] = shorts[i];
        }
        return newShorts;
    }

    @Override
    public int find(Object key)
    {
        short v = (short) key;
        return find(v);
    }

    @Override
    public void insert(Object key)
    {
        short v = (short) key;
        insert(v);
    }

    @Override
    public <T> void forEach(Class<T> clazz, Consumer<T> consumer)
    {
        for (int i = 0; i < size; i++) {
            consumer.accept(clazz.cast(shorts[i]));
        }
    }

    public int find(short key)
    {
        for (int i = 0; i < size; i++) {
            if (key == shorts[i]) {
                return i;
            }
        }
        return size;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return super.getRetainedSizeInBytes() + sizeOf(shorts);
    }

    @Override
    protected void doRead(Slice slice)
    {
        slice.toByteBuffer().asShortBuffer().get(shorts, 0, size);
    }

    public void insert(short key)
    {
        int pos = find(key);

        if (pos < size) {
            // inserted
            return;
        }
        shorts[pos] = key;
        size++;
    }
}
