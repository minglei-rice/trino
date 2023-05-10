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

import io.airlift.slice.Slices;
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
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import org.apache.iceberg.aggindex.PartialAggAccumulator;

import java.math.BigDecimal;

import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

@AggregationFunction("cube_avg_decimal_pre")
public final class AggIndexDecimalAveragePreAggregations
{
    private AggIndexDecimalAveragePreAggregations() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, @SqlType("T") Object value)
    {
        // TODO LongDecimal
        checkCondition(false, INVALID_FUNCTION_ARGUMENT, "Long decimal is not supported for now");
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, @SqlType("T") long value)
    {
        PartialAggAccumulator aggAccumulator = state.getPartialAggAccumulator(type, AggIndex.AggFunctionType.AVG);
        aggAccumulator.accumulate(BigDecimal.valueOf(value, ((DecimalType) type).getScale()));
    }

    @CombineFunction
    @TypeParameter("T")
    public static void combine(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, @AggregationState AggIndexAccumulatorState otherState)
    {
        AggIndexDecimalAverageAggregations.combine(type, state, otherState);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    @TypeParameter("T")
    public static void output(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, BlockBuilder out)
    {
        if (state.resultIsNull()) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, Slices.wrappedBuffer(state.getIntermediateResult(type, AggIndex.AggFunctionType.AVG)));
        }
    }
}
