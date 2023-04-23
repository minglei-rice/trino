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

import org.roaringbitmap.RoaringBitmap;

import static com.google.common.base.Preconditions.checkArgument;

public class IntRoaringBitmapData
        implements RoaringBitmapData
{
    RoaringBitmap roaringBitmap;

    public IntRoaringBitmapData(int... values)
    {
        roaringBitmap = RoaringBitmap.bitmapOf(values);
    }

    public IntRoaringBitmapData(RoaringBitmap roaringBitmap)
    {
        checkArgument(roaringBitmap != null, "roaringBitmap is null");
        this.roaringBitmap = roaringBitmap;
    }

    @Override
    public void merge(RoaringBitmapData other)
    {
        if (roaringBitmap == null) {
            roaringBitmap = RoaringBitmap.bitmapOf();
        }
        roaringBitmap.or(other.getData(RoaringBitmap.class));
    }

    @Override
    public long cardinality()
    {
        return roaringBitmap.getCardinality();
    }

    @Override
    public void add(Object value)
    {
        add((int) value);
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
        roaringBitmap.add(value);
    }

    public void add(long value)
    {
        roaringBitmap.add(Long.valueOf(value).intValue());
    }
}
