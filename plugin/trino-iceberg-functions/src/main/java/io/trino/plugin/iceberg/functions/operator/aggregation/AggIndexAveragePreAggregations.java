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
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import org.apache.iceberg.aggindex.PartialAggAccumulator;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;

@AggregationFunction("cube_avg_double_pre")
public final class AggIndexAveragePreAggregations
{
    private AggIndexAveragePreAggregations() {}

    @InputFunction
    public static void input(@AggregationState AggIndexAccumulatorState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        inputValue(DOUBLE, state, value);
    }

    @InputFunction
    public static void input(@AggregationState AggIndexAccumulatorState state, @SqlType(StandardTypes.REAL) float value)
    {
        inputValue(REAL, state, value);
    }

    @InputFunction
    public static void input(@AggregationState AggIndexAccumulatorState state, @SqlType(StandardTypes.INTEGER) int value)
    {
        inputValue(INTEGER, state, value);
    }

    @InputFunction
    public static void input(@AggregationState AggIndexAccumulatorState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        inputValue(BIGINT, state, value);
    }

    @CombineFunction
    public static void combine(@AggregationState AggIndexAccumulatorState state, @AggregationState AggIndexAccumulatorState otherState)
    {
        AggIndexDoubleAverageAggregations.combine(DOUBLE, state, otherState);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState AggIndexAccumulatorState state, BlockBuilder out)
    {
        if (state.resultIsNull()) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, Slices.wrappedBuffer(state.getIntermediateResult(DOUBLE, AggIndex.AggFunctionType.AVG)));
        }
    }

    private static void inputValue(Type type, AggIndexAccumulatorState state, Number value)
    {
        PartialAggAccumulator accumulator = state.getPartialAggAccumulator(type, AggIndex.AggFunctionType.AVG);
        accumulator.accumulate(value);
    }
}
