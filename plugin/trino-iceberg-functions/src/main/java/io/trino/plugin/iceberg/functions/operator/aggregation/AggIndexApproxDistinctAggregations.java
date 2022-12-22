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
import io.airlift.stats.cardinality.HyperLogLog;
import io.trino.plugin.iceberg.functions.operator.aggregation.state.AggIndexAccumulatorState;
import io.trino.spi.aggindex.AggIndex;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import org.apache.iceberg.aggindex.HyperLogLogAccumulator;

import static io.trino.spi.type.BigintType.BIGINT;

@AggregationFunction("cube_approx_distinct")
public final class AggIndexApproxDistinctAggregations
{
    private AggIndexApproxDistinctAggregations() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, @SqlType("T") Slice slice)
    {
        state.merge(type, slice, AggIndex.AggFunctionType.APPROX_COUNT_DISTINCT);
    }

    @CombineFunction
    @TypeParameter("T")
    public static void combine(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, @AggregationState AggIndexAccumulatorState otherState)
    {
        state.merge(type, otherState, AggIndex.AggFunctionType.APPROX_COUNT_DISTINCT);
    }

    @OutputFunction(StandardTypes.BIGINT)
    @TypeParameter("T")
    public static void output(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, BlockBuilder out)
    {
        HyperLogLogAccumulator hyperLogLogAccumulator = (HyperLogLogAccumulator) state.getPartialAggAccumulator(type, AggIndex.AggFunctionType.APPROX_COUNT_DISTINCT);
        if (hyperLogLogAccumulator.resultIsNull()) {
            BIGINT.writeLong(out, 0);
        }
        else {
            BIGINT.writeLong(out, ((HyperLogLog) state.getResult(BIGINT, AggIndex.AggFunctionType.APPROX_COUNT_DISTINCT)).cardinality());
        }
    }
}
