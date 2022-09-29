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

import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BitmapWithSmallSet
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BitmapWithSmallSet.class).instanceSize();
    private final ClickHouseDataTypeMapping dataType;
    private SmallSet smallSet;
    private RoaringBitmapData bitmapData;

    public BitmapWithSmallSet(ClickHouseDataTypeMapping dataType)
    {
        this.dataType = dataType;
        switch (dataType) {
            case INT8:
            case UINT8:
                smallSet = new ByteSet(32);
                break;
            case INT16:
            case UINT16:
                smallSet = new ShortSet(32);
                break;
            case INT32:
            case UINT32:
                smallSet = new IntSet(32);
                break;
            case INT64:
            case UINT64:
                smallSet = new LongSet(32);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized clickhouse data type " + dataType.name());
        }
    }

    public BitmapWithSmallSet(RoaringBitmapData bitmapData, ClickHouseDataTypeMapping dataType)
    {
        requireNonNull(bitmapData, "bitmapData is null");
        requireNonNull(dataType, "dataType is null");
        checkArgument(dataType.byteLength() >= 8 && bitmapData instanceof LongRoaringBitmapData ||
                        dataType.byteLength() < 8 && bitmapData instanceof IntRoaringBitmapData,
                format("dataType %s has length %s expecting an instance of %s, but found %s!",
                        dataType.name(), dataType.byteLength(),
                        dataType.byteLength() >= 8 ? LongRoaringBitmapData.class.getName() : IntRoaringBitmapData.class.getName(),
                        bitmapData.getClass().getName()));

        this.dataType = dataType;
        this.bitmapData = bitmapData;
    }

    public ClickHouseDataTypeMapping getDataTypeMapping()
    {
        return dataType;
    }

    public SmallSet getSmallSet()
    {
        return smallSet;
    }

    public RoaringBitmapData getBitmapData()
    {
        return bitmapData;
    }

    public boolean isSmall()
    {
        return null != smallSet;
    }

    public boolean isLarge()
    {
        return smallSet == null;
    }

    public long estimatedMemorySize()
    {
        return INSTANCE_SIZE +
                (smallSet == null ? 0 : smallSet.getRetainedSizeInBytes()) +
                (bitmapData == null ? 0 : bitmapData.getRetainedSizeInBytes());
    }

    public void add(byte value)
    {
        add((Byte) value);
    }

    public void add(short value)
    {
        add((Short) value);
    }

    public void add(int value)
    {
        add((Integer) value);
    }

    public void add(long value)
    {
        add((Long) value);
    }

    private void add(Object value)
    {
        if (isSmall()) {
            if (smallSet.find(value) == smallSet.getSize()) {
                if (!smallSet.full()) {
                    smallSet.insert(value);
                }
                else {
                    toLarge();
                    bitmapData.add(value);
                }
            }
        }
        else {
            bitmapData.add(value);
        }
    }

    public void merge(BitmapWithSmallSet other)
    {
        if (this == other) {
            return;
        }
        if (other.isLarge()) {
            if (isSmall()) {
                toLarge();
            }
            bitmapData.merge(other.bitmapData);
        }
        else {
            other.smallSet.forEach(Object.class, this::add);
        }
    }

    public long cardinality()
    {
        if (isSmall()) {
            return smallSet.getSize();
        }
        else {
            return bitmapData.cardinality();
        }
    }

    private void toLarge()
    {
        if (dataType.byteLength() >= 8) {
            bitmapData = new LongRoaringBitmapData(smallSet.getRawArray(long[].class));
        }
        else {
            bitmapData = new IntRoaringBitmapData(smallSet.toIntArray());
        }
        smallSet = null;
    }
}
