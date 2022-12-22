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
package io.trino.plugin.iceberg.functions.operator.aggregation;

import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.functions.operator.aggregation.state.AggIndexAccumulatorState;
import io.trino.spi.aggindex.AggIndex;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.iceberg.aggindex.CountDistinctAccumulator;

import static io.trino.spi.type.BigintType.BIGINT;

@AggregationFunction("cube_count_distinct")
public final class AggIndexCountDistinctAggregations
{
    private AggIndexCountDistinctAggregations() {}

    @InputFunction
    public static void input(@AggregationState AggIndexAccumulatorState state, @SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        state.merge(BIGINT, slice, AggIndex.AggFunctionType.COUNT_DISTINCT);
    }

    @CombineFunction
    public static void combine(@AggregationState AggIndexAccumulatorState state, @AggregationState AggIndexAccumulatorState otherState)
    {
        state.merge(BIGINT, otherState, AggIndex.AggFunctionType.COUNT_DISTINCT);
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState AggIndexAccumulatorState state, BlockBuilder out)
    {
        CountDistinctAccumulator countDistinctAccumulator = (CountDistinctAccumulator) state.getPartialAggAccumulator(BIGINT, AggIndex.AggFunctionType.COUNT_DISTINCT);
        BIGINT.writeLong(out, countDistinctAccumulator.getResult());
    }
}
