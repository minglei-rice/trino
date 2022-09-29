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
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

public final class BitmapFunctions
{
    private static final BitmapStateSerializer SERIALIZER = new BitmapStateSerializer();

    private BitmapFunctions() {}

    @ScalarFunction
    @Description("Compute the cardinality of a Bitmap instance")
    @SqlType(StandardTypes.BIGINT)
    public static long cardinality(@SqlType(BitmapType.NAME) Slice value)
    {
        return SERIALIZER.deserialize(value).cardinality();
    }
}
