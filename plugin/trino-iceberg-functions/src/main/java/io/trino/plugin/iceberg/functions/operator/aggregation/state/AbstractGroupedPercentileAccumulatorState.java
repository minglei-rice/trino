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
import io.trino.array.ObjectBigArray;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.spi.type.DoubleType;
import org.apache.iceberg.aggindex.PercentileAccumulator;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class AbstractGroupedPercentileAccumulatorState
        extends AbstractGroupedAccumulatorState
        implements AggIndexCommonPercentileAccumulatorState
{
    protected final ObjectBigArray<PercentileAccumulator> accumulators = new ObjectBigArray<>();
    protected long size;
    protected Double weight;

    @Override
    public void ensureCapacity(long size)
    {
        accumulators.ensureCapacity(size);
    }

    @Override
    public void merge(Slice slice, Double weight)
    {
        checkArgument(weight != null, "Agg index group digest weight should not be null!");
        this.weight = weight;
        PercentileAccumulator initAggAccumulator = PercentileAccumulator.forType(TypeConverter.toIcebergType(DoubleType.DOUBLE), weight);
        initAggAccumulator.fromBinary(slice.getBytes());
        PercentileAccumulator currentAccumulator = getPercentileAccumulator();
        addMemoryUsage(-currentAccumulator.estimatedMemorySize());
        currentAccumulator.merge(initAggAccumulator);
        addMemoryUsage(currentAccumulator.estimatedMemorySize());
        accumulators.set(getGroupId(), currentAccumulator);
    }

    @Override
    public void merge(AggIndexCommonPercentileAccumulatorState state)
    {
        PercentileAccumulator currentAccumulator = getPercentileAccumulator();
        PercentileAccumulator otherAccumulator = state.getPercentileAccumulator();
        addMemoryUsage(-currentAccumulator.estimatedMemorySize());
        currentAccumulator.merge(otherAccumulator);
        addMemoryUsage(currentAccumulator.estimatedMemorySize());
        accumulators.set(getGroupId(), currentAccumulator);
    }

    @Override
    public PercentileAccumulator getPercentileAccumulator()
    {
        if (accumulators.get(getGroupId()) == null) {
            if (weight == null) {
                throw new IllegalArgumentException("Param: weight for agg index percentile function should not be null");
            }
            accumulators.set(getGroupId(), PercentileAccumulator.forType(TypeConverter.toIcebergType(DoubleType.DOUBLE), weight));
        }
        return accumulators.get(getGroupId());
    }

    @Override
    public TDigest getResult()
    {
        return accumulators.get(getGroupId()) == null ? null : accumulators.get(getGroupId()).getResult();
    }

    @Override
    public byte[] getIntermediateResult()
    {
        return getPercentileAccumulator().toBinary();
    }

    @Override
    public void addMemoryUsage(long value)
    {
        size += value;
    }
}
