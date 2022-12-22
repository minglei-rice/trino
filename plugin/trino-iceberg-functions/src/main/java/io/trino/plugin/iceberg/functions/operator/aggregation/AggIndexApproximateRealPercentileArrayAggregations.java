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

import java.util.List;

import static io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateLongPercentileArrayAggregations.valuesAtPercentiles;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;

@AggregationFunction("cube_approx_float_percentile")
public final class AggIndexApproximateRealPercentileArrayAggregations
{
    private AggIndexApproximateRealPercentileArrayAggregations() {}

    @InputFunction
    public static void input(@AggregationState AggIndexPercentileArrayAccumulatorState state, @SqlType(StandardTypes.VARBINARY) Slice value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        AggIndexApproximateDoublePercentileArrayAggregations.input(state, value, percentilesArrayBlock);
    }

    @InputFunction
    public static void weightedInput(@AggregationState AggIndexPercentileArrayAccumulatorState state, @SqlType(StandardTypes.VARBINARY) Slice value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        AggIndexApproximateDoublePercentileArrayAggregations.weightedInput(state, value, weight, percentilesArrayBlock);
    }

    @CombineFunction
    public static void combine(@AggregationState AggIndexPercentileArrayAccumulatorState state, AggIndexPercentileArrayAccumulatorState otherState)
    {
        AggIndexApproximateDoublePercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction("array(real)")
    public static void output(@AggregationState AggIndexPercentileArrayAccumulatorState state, BlockBuilder out)
    {
        TDigest tDigest = state.getResult();
        if (tDigest == null) {
            out.appendNull();
        }
        else {
            List<Double> percentiles = state.getPercentiles();
            BlockBuilder blockBuilder = out.beginBlockEntry();

            List<Double> valuesAtPercentiles = valuesAtPercentiles(tDigest, percentiles);
            for (double value : valuesAtPercentiles) {
                REAL.writeLong(blockBuilder, floatToRawIntBits((float) value));
            }

            out.closeEntry();
        }
    }
}
