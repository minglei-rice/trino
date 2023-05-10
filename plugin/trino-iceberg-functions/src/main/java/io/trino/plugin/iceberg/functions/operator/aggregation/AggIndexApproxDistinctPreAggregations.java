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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.aggindex.PartialAggAccumulator;

@AggregationFunction("cube_approx_distinct_pre")
public final class AggIndexApproxDistinctPreAggregations
{
    private AggIndexApproxDistinctPreAggregations() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(@AggregationState AggIndexAccumulatorState state, @SqlType("T") long value)
    {
        inputValue(BigintType.BIGINT, state, value);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@AggregationState AggIndexAccumulatorState state, @SqlType("T") double value)
    {
        inputValue(DoubleType.DOUBLE, state, value);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@AggregationState AggIndexAccumulatorState state, @SqlType("T") float value)
    {
        inputValue(RealType.REAL, state, value);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@AggregationState AggIndexAccumulatorState state, @SqlType("T") int value)
    {
        inputValue(IntegerType.INTEGER, state, value);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, @SqlType("T") Object value)
    {
        Object transValue = value;
        if (type instanceof VarcharType) {
            transValue = ((Slice) value).toStringUtf8();
        }
        inputValue(type, state, transValue);
    }

    @CombineFunction
    public static void combine(@AggregationState AggIndexAccumulatorState state, @AggregationState AggIndexAccumulatorState otherState)
    {
        AggIndexApproxDistinctAggregations.combine(otherState.getType(), state, otherState);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState AggIndexAccumulatorState state, BlockBuilder out)
    {
        if (state.resultIsNull()) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, Slices.wrappedBuffer(state.getIntermediateResult(state.getType(), AggIndex.AggFunctionType.APPROX_COUNT_DISTINCT)));
        }
    }

    private static void inputValue(Type type, AggIndexAccumulatorState state, Object value)
    {
        PartialAggAccumulator accumulator = state.getPartialAggAccumulator(type, AggIndex.AggFunctionType.APPROX_COUNT_DISTINCT);
        accumulator.accumulate(value);
    }
}
