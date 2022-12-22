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

import com.google.common.collect.ImmutableList;
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

import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateLongPercentileArrayAggregations.valuesAtPercentiles;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DoubleType.DOUBLE;

@AggregationFunction("cube_approx_double_percentile")
public final class AggIndexApproximateDoublePercentileArrayAggregations
{
    private AggIndexApproximateDoublePercentileArrayAggregations() {}

    @InputFunction
    public static void input(@AggregationState AggIndexPercentileArrayAccumulatorState state, @SqlType(StandardTypes.VARBINARY) Slice value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        initializePercentilesArray(state, percentilesArrayBlock);
        state.merge(value, 1.0);
    }

    @InputFunction
    public static void weightedInput(@AggregationState AggIndexPercentileArrayAccumulatorState state, @SqlType(StandardTypes.VARBINARY) Slice value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        initializePercentilesArray(state, percentilesArrayBlock);
        state.merge(value, weight);
    }

    @CombineFunction
    public static void combine(@AggregationState AggIndexPercentileArrayAccumulatorState state, AggIndexPercentileArrayAccumulatorState otherState)
    {
        state.setWeight(otherState.getWeight());
        state.merge(otherState);
        state.setPercentiles(otherState.getPercentiles());
    }

    @OutputFunction("array(double)")
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
                DOUBLE.writeDouble(blockBuilder, value);
            }

            out.closeEntry();
        }
    }

    private static void initializePercentilesArray(@AggregationState AggIndexPercentileArrayAccumulatorState state, Block percentilesArrayBlock)
    {
        if (state.getPercentiles() == null) {
            ImmutableList.Builder<Double> percentilesListBuilder = ImmutableList.builder();

            for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
                checkCondition(!percentilesArrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "Percentile cannot be null");
                double percentile = DOUBLE.getDouble(percentilesArrayBlock, i);
                checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
                percentilesListBuilder.add(percentile);
            }

            state.setPercentiles(percentilesListBuilder.build());
        }
    }
}
