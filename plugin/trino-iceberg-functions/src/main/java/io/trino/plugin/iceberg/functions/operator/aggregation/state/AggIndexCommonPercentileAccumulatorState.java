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
import io.airlift.stats.TDigest;
import io.trino.spi.function.AccumulatorState;
import org.apache.iceberg.aggindex.PercentileAccumulator;

public interface AggIndexCommonPercentileAccumulatorState
        extends AccumulatorState
{
    void merge(Slice slice, Double weight);

    void merge(AggIndexCommonPercentileAccumulatorState state);

    PercentileAccumulator getPercentileAccumulator();

    TDigest getResult();

    byte[] getIntermediateResult();

    void addMemoryUsage(long value);

    Double getWeight();

    void setWeight(Double weight);
}
