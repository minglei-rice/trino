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
package com.bilibili.olap.functions.scalar;

import com.bilibili.olap.functions.aggregation.BitmapStateSerializer;
import com.bilibili.olap.functions.type.BitmapType;
import com.bilibili.olap.functions.type.ClickHouseBitmapType;
import io.airlift.slice.Slice;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.function.OperatorType.CAST;

public final class BitmapOperators
{
    private BitmapOperators() {}

    @ScalarOperator(CAST)
    @SqlType(BitmapType.NAME)
    public static Slice castToBitmapFromVarbinary(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType(ClickHouseBitmapType.NAME)
    public static Slice castToCKBitmapFromVarbinary(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return castToCKBitmapFromBitmap(slice);
    }

    @ScalarOperator(CAST)
    @SqlType(ClickHouseBitmapType.NAME)
    public static Slice castToCKBitmapFromBitmap(@SqlType(BitmapType.NAME) Slice slice)
    {
        return slice.slice(BitmapStateSerializer.HEADER_SIZE_IN_BYTES, slice.length() - BitmapStateSerializer.HEADER_SIZE_IN_BYTES);
    }
}
