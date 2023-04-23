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

import com.bilibili.olap.functions.type.BitmapType;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

@AggregationFunction("groupBitmap")
public final class BitmapShortAgg
{
    private BitmapShortAgg() {}

    private static final BitmapStateSerializer SERIALIZER = new BitmapStateSerializer();

    @InputFunction
    public static void input(@AggregationState BitmapState state, @SqlType(StandardTypes.SMALLINT) long value)
    {
        if (state.getBitmap() == null) {
            state.setBitmap(new BitmapWithSmallSet(ClickHouseDataTypeMapping.INT16));
        }
        state.getBitmap().add((short) value);
    }

    @CombineFunction
    public static void combine(@AggregationState BitmapState state, @AggregationState BitmapState otherState)
    {
        BitmapWithSmallSet input = otherState.getBitmap();

        BitmapWithSmallSet previous = state.getBitmap();
        if (previous == null) {
            state.setBitmap(input);
            state.addMemoryUsage(input.estimatedMemorySize());
        }
        else {
            state.addMemoryUsage(-previous.estimatedMemorySize());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedMemorySize());
        }
    }

    @OutputFunction(BitmapType.NAME)
    public static void evaluate(@AggregationState BitmapState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
