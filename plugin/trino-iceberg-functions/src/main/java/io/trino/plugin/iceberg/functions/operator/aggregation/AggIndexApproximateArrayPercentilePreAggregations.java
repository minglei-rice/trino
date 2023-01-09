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
import io.trino.plugin.iceberg.functions.operator.aggregation.state.AggIndexPercentileArrayAccumulatorState;
import io.trino.spi.block.Block;
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

import static io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateDoublePercentileArrayAggregations.initializePercentilesArray;

@AggregationFunction("cube_approx_percentile_pre")
public final class AggIndexApproximateArrayPercentilePreAggregations
{
    private AggIndexApproximateArrayPercentilePreAggregations() {}

    @InputFunction
    public static void input(@AggregationState AggIndexPercentileArrayAccumulatorState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        inputArrayValue(state, value, 1.0, percentilesArrayBlock);
    }

    @InputFunction
    public static void input(@AggregationState AggIndexPercentileArrayAccumulatorState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        inputArrayValue(state, value, 1.0, percentilesArrayBlock);
    }

    @InputFunction
    public static void weightedInput(@AggregationState AggIndexPercentileArrayAccumulatorState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        inputArrayValue(state, value, weight, percentilesArrayBlock);
    }

    @InputFunction
    public static void weightedInput(@AggregationState AggIndexPercentileArrayAccumulatorState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        inputArrayValue(state, value, weight, percentilesArrayBlock);
    }

    @CombineFunction
    public static void combine(@AggregationState AggIndexPercentileArrayAccumulatorState state, AggIndexPercentileArrayAccumulatorState otherState)
    {
        AggIndexApproximateDoublePercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState AggIndexPercentileArrayAccumulatorState state, BlockBuilder out)
    {
        TDigest tDigest = state.getResult();
        if (tDigest == null) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, Slices.wrappedBuffer(state.getIntermediateResult()));
        }
    }

    private static void inputArrayValue(AggIndexPercentileArrayAccumulatorState state, Number value, double weight, Block percentilesArrayBlock)
    {
        state.setWeight(weight);
        initializePercentilesArray(state, percentilesArrayBlock);
        PercentileAccumulator accumulator = state.getPercentileAccumulator();
        accumulator.accumulate(value);
    }
}
