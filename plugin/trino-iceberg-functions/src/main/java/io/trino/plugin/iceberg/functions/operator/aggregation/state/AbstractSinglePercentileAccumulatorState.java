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
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.spi.type.DoubleType;
import org.apache.iceberg.aggindex.PercentileAccumulator;

public abstract class AbstractSinglePercentileAccumulatorState
        implements AggIndexCommonPercentileAccumulatorState
{
    protected PercentileAccumulator percentileAccumulator;
    protected Double weight;

    @Override
    public void merge(Slice slice, Double weight)
    {
        this.weight = weight;
        PercentileAccumulator initAggAccumulator = PercentileAccumulator.forType(TypeConverter.toIcebergType(DoubleType.DOUBLE), weight);
        initAggAccumulator.fromBinary(slice.getBytes());
        PercentileAccumulator mergedAggAccumulator = getPercentileAccumulator();
        mergedAggAccumulator.merge(initAggAccumulator);
        this.percentileAccumulator = mergedAggAccumulator;
    }

    @Override
    public void merge(AggIndexCommonPercentileAccumulatorState state)
    {
        PercentileAccumulator currentAccumulator = getPercentileAccumulator();
        PercentileAccumulator otherAccumulator = state.getPercentileAccumulator();
        currentAccumulator.merge(otherAccumulator);
        this.percentileAccumulator = currentAccumulator;
    }

    @Override
    public PercentileAccumulator getPercentileAccumulator()
    {
        if (percentileAccumulator == null) {
            if (weight == null) {
                throw new IllegalArgumentException("Param: weight for agg index percentile function should not be null");
            }
            percentileAccumulator = PercentileAccumulator.forType(TypeConverter.toIcebergType(DoubleType.DOUBLE), weight);
        }
        return percentileAccumulator;
    }

    @Override
    public TDigest getResult()
    {
        return percentileAccumulator == null ? null : percentileAccumulator.getResult();
    }

    @Override
    public void addMemoryUsage(long value)
    {
        // noop
    }
}
