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

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

public class BitmapStateFactory
        implements AccumulatorStateFactory<BitmapState>
{
    @Override
    public BitmapState createSingleState()
    {
        return new SingleBitmapState();
    }

    @Override
    public BitmapState createGroupedState()
    {
        return new GroupedBitmapState();
    }

    public static class GroupedBitmapState
            extends AbstractGroupedAccumulatorState implements BitmapState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BitmapLongAgg.class).instanceSize();
        private final ObjectBigArray<BitmapWithSmallSet> bitmaps = new ObjectBigArray<>();
        private long size;

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + bitmaps.sizeOf();
        }

        @Override
        public void ensureCapacity(long size)
        {
            bitmaps.ensureCapacity(size);
        }

        @Override
        public BitmapWithSmallSet getBitmap()
        {
            return bitmaps.get(getGroupId());
        }

        @Override
        public void setBitmap(BitmapWithSmallSet bitmap)
        {
            bitmaps.set(getGroupId(), bitmap);
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }
    }

    public static class SingleBitmapState
            implements BitmapState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleBitmapState.class).instanceSize();
        private BitmapWithSmallSet bitmap;

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (bitmap != null) {
                estimatedSize += bitmap.estimatedMemorySize();
            }
            return estimatedSize;
        }

        @Override
        public BitmapWithSmallSet getBitmap()
        {
            return bitmap;
        }

        @Override
        public void setBitmap(BitmapWithSmallSet bitmap)
        {
            this.bitmap = bitmap;
        }

        @Override
        public void addMemoryUsage(long value)
        {
            // noops
        }
    }
}
