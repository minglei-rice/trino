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
package io.trino.plugin.iceberg.functions.operator.aggregation.state;

import io.airlift.slice.Slice;
import io.trino.spi.aggindex.AggIndex;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;
import io.trino.spi.type.Type;
import org.apache.iceberg.aggindex.PartialAggAccumulator;

@AccumulatorStateMetadata(stateFactoryClass = AggIndexAccumulatorStateFactory.class, stateSerializerClass = AggIndexAccumulatorStateSerializer.class)
public interface AggIndexAccumulatorState
        extends AccumulatorState
{
    void merge(Type type, Slice slice, AggIndex.AggFunctionType functionType);

    void merge(Type type, AggIndexAccumulatorState state, AggIndex.AggFunctionType functionType);

    PartialAggAccumulator getPartialAggAccumulator(Type type, AggIndex.AggFunctionType functionType);

    Object getResult(Type type, AggIndex.AggFunctionType functionType);

    void addMemoryUsage(long value);

    boolean resultIsNull();

    Type getType();

    AggIndex.AggFunctionType getAggFunctionType();
}
