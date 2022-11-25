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
package com.bilibili.olap.functions;

import com.bilibili.olap.functions.aggregation.BitmapWithSmallSet;
import com.bilibili.olap.functions.aggregation.LongRoaringBitmapData;
import com.bilibili.olap.functions.aggregation.RoaringBitmapData;
import org.roaringbitmap.longlong.ClickHouseRoaring64NavigableMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class ClickHouseBitmapAdapter
{
    private ClickHouseBitmapAdapter() {}

    public static BitmapWithSmallSet toClickHouseBitmap(BitmapWithSmallSet bitmap)
    {
        if (bitmap != null && bitmap.isLarge() && bitmap.getBitmapData() instanceof LongRoaringBitmapData) {
            Roaring64NavigableMap internalBitmap = bitmap.getBitmapData().getData(Roaring64NavigableMap.class);
            if (!(internalBitmap instanceof ClickHouseRoaring64NavigableMap)) {
                Roaring64NavigableMap ckBitmap = ClickHouseRoaring64NavigableMap.bitmapOf(internalBitmap);
                RoaringBitmapData bitmapData = new LongRoaringBitmapData(ckBitmap);
                return new BitmapWithSmallSet(bitmapData, bitmap.getDataTypeMapping());
            }
        }
        return bitmap;
    }
}
