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

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import static com.google.common.base.Preconditions.checkArgument;

public class LongRoaringBitmapData
        implements RoaringBitmapData
{
    Roaring64NavigableMap roaringBitmap;

    public LongRoaringBitmapData(long... values)
    {
        roaringBitmap = Roaring64NavigableMap.bitmapOf(values);
    }

    public LongRoaringBitmapData(Roaring64NavigableMap roaringBitmap)
    {
        checkArgument(roaringBitmap != null, "roaringBitMap is null");
        this.roaringBitmap = roaringBitmap;
    }

    @Override
    public void merge(RoaringBitmapData other)
    {
        if (roaringBitmap == null) {
            roaringBitmap = Roaring64NavigableMap.bitmapOf();
        }
        roaringBitmap.or(other.getData(Roaring64NavigableMap.class));
    }

    @Override
    public long cardinality()
    {
        return roaringBitmap.getLongCardinality();
    }

    @Override
    public void add(Object value)
    {
        if (value instanceof Integer) {
            add((int) value);
        }
        else {
            add((long) value);
        }
    }

    @Override
    public <T> T getData(Class<T> clazz)
    {
        return clazz.cast(roaringBitmap);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return roaringBitmap.getLongSizeInBytes();
    }

    public void add(int value)
    {
        roaringBitmap.addInt(value);
    }

    public void add(long value)
    {
        roaringBitmap.addLong(value);
    }
}
