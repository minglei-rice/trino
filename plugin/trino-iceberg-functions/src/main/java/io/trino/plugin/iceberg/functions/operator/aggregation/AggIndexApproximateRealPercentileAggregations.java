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
import io.trino.plugin.iceberg.functions.operator.aggregation.state.AggIndexPercentileAccumulatorState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;

@AggregationFunction("cube_approx_float_percentile")
public final class AggIndexApproximateRealPercentileAggregations
{
    private AggIndexApproximateRealPercentileAggregations() {}

    @InputFunction
    public static void input(@AggregationState AggIndexPercentileAccumulatorState state, @SqlType(StandardTypes.VARBINARY) Slice value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        AggIndexApproximateDoublePercentileAggregations.input(state, value, percentile);
    }

    @InputFunction
    public static void weightedInput(@AggregationState AggIndexPercentileAccumulatorState state, @SqlType(StandardTypes.VARBINARY) Slice value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        AggIndexApproximateDoublePercentileAggregations.weightedInput(state, value, weight, percentile);
    }

    @CombineFunction
    public static void combine(@AggregationState AggIndexPercentileAccumulatorState state, AggIndexPercentileAccumulatorState otherState)
    {
        AggIndexApproximateDoublePercentileAggregations.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.REAL)
    public static void output(@AggregationState AggIndexPercentileAccumulatorState state, BlockBuilder out)
    {
        TDigest tDigest = state.getResult();
        if (tDigest == null) {
            out.appendNull();
        }
        else {
            checkState(state.getPercentile() != null, "Percentile is missing");
            checkCondition(0 <= state.getPercentile() && state.getPercentile() <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            REAL.writeLong(out, (floatToRawIntBits((float) (tDigest).valueAt(state.getPercentile()))));
        }
    }
}
