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
import io.trino.spi.type.Decimals;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import java.math.BigDecimal;

import static io.trino.spi.type.Decimals.writeShortDecimal;

@AggregationFunction("cube_avg_decimal")
public final class AggIndexDecimalAverageAggregations
{
    private AggIndexDecimalAverageAggregations() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, @SqlType("T") Slice slice)
    {
        state.merge(type, slice, AggIndex.AggFunctionType.AVG);
    }

    @CombineFunction
    @TypeParameter("T")
    public static void combine(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, @AggregationState AggIndexAccumulatorState otherState)
    {
        state.merge(type, otherState, AggIndex.AggFunctionType.AVG);
    }

    @OutputFunction(StandardTypes.DECIMAL)
    @TypeParameter("T")
    public static void output(@TypeParameter("T") Type type, @AggregationState AggIndexAccumulatorState state, BlockBuilder out)
    {
        if (state.resultIsNull()) {
            out.appendNull();
        }
        else {
            DecimalType decimalType = (DecimalType) type;
            BigDecimal result = (BigDecimal) state.getResult(type, AggIndex.AggFunctionType.AVG);
            if (decimalType.isShort()) {
                long value = Decimals.encodeShortScaledValue(result, decimalType.getScale());
                writeShortDecimal(out, value);
            }
            else {
                decimalType.writeObject(out, Decimals.encodeScaledValue(result, decimalType.getScale()));
            }
        }
    }
}
