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

import com.google.common.primitives.Doubles;
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

import static io.trino.spi.type.BigintType.BIGINT;

@AggregationFunction("cube_approx_long_percentile")
public final class AggIndexApproximateLongPercentileArrayAggregations
{
    private AggIndexApproximateLongPercentileArrayAggregations() {}

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

    @OutputFunction("array(bigint)")
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
                BIGINT.writeLong(blockBuilder, Math.round(value));
            }

            out.closeEntry();
        }
    }

    public static List<Double> valuesAtPercentiles(TDigest digest, List<Double> percentiles)
    {
        int[] indexes = new int[percentiles.size()];
        double[] sortedPercentiles = new double[percentiles.size()];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
            sortedPercentiles[i] = percentiles.get(i);
        }

        it.unimi.dsi.fastutil.Arrays.quickSort(0, percentiles.size(), (a, b) -> Doubles.compare(sortedPercentiles[a], sortedPercentiles[b]), (a, b) -> {
            double tempPercentile = sortedPercentiles[a];
            sortedPercentiles[a] = sortedPercentiles[b];
            sortedPercentiles[b] = tempPercentile;

            int tempIndex = indexes[a];
            indexes[a] = indexes[b];
            indexes[b] = tempIndex;
        });

        List<Double> valuesAtPercentiles = digest.valuesAt(Doubles.asList(sortedPercentiles));
        double[] result = new double[valuesAtPercentiles.size()];
        for (int i = 0; i < valuesAtPercentiles.size(); i++) {
            result[indexes[i]] = valuesAtPercentiles.get(i);
        }

        return Doubles.asList(result);
    }
}
