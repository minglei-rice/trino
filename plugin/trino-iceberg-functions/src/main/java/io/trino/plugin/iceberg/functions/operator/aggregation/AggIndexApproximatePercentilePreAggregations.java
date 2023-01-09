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
import io.airlift.stats.TDigest;
import io.trino.plugin.iceberg.functions.operator.aggregation.state.AggIndexPercentileAccumulatorState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.VarbinaryType;
import org.apache.iceberg.aggindex.PercentileAccumulator;

@AggregationFunction("cube_approx_percentile_pre")
public final class AggIndexApproximatePercentilePreAggregations
{
    private AggIndexApproximatePercentilePreAggregations() {}

    @InputFunction
    public static void input(@AggregationState AggIndexPercentileAccumulatorState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        inputValue(state, value, 1.0, percentile);
    }

    @InputFunction
    public static void input(@AggregationState AggIndexPercentileAccumulatorState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        inputValue(state, value, 1.0, percentile);
    }

    @InputFunction
    public static void weightedInput(@AggregationState AggIndexPercentileAccumulatorState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        inputValue(state, value, weight, percentile);
    }

    @InputFunction
    public static void weightedInput(@AggregationState AggIndexPercentileAccumulatorState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        inputValue(state, value, weight, percentile);
    }

    @CombineFunction
    public static void combine(@AggregationState AggIndexPercentileAccumulatorState state, AggIndexPercentileAccumulatorState otherState)
    {
        AggIndexApproximateDoublePercentileAggregations.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState AggIndexPercentileAccumulatorState state, BlockBuilder out)
    {
        TDigest tDigest = state.getResult();
        if (tDigest == null) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, Slices.wrappedBuffer(state.getIntermediateResult()));
        }
    }

    private static void inputValue(AggIndexPercentileAccumulatorState state, Number value, double weight, double percentile)
    {
        state.setWeight(weight);
        state.setPercentile(percentile);
        PercentileAccumulator accumulator = state.getPercentileAccumulator();
        accumulator.accumulate(value);
    }
}
