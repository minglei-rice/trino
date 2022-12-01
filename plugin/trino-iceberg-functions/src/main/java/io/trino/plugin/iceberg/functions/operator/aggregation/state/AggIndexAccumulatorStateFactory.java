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
import io.trino.array.ObjectBigArray;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.spi.aggindex.AggIndex;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.type.Type;
import org.apache.iceberg.aggindex.AvgAccumulator;
import org.apache.iceberg.aggindex.PartialAggAccumulator;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;

public class AggIndexAccumulatorStateFactory
        implements AccumulatorStateFactory<AggIndexAccumulatorState>
{
    @Override
    public AggIndexAccumulatorState createSingleState()
    {
        return new SingleAggIndexAccumulatorState();
    }

    @Override
    public AggIndexAccumulatorState createGroupedState()
    {
        return new GroupedAggIndexAccumulatorState();
    }

    public static class SingleAggIndexAccumulatorState
            implements AggIndexAccumulatorState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleAggIndexAccumulatorState.class).instanceSize();

        private Type type;
        private AggIndex.AggFunctionType functionType;
        private PartialAggAccumulator aggAccumulator;

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + (aggAccumulator == null ? 0 : aggAccumulator.estimatedMemorySize());
        }

        @Override
        public void merge(Type type, Slice slice, AggIndex.AggFunctionType functionType)
        {
            PartialAggAccumulator initAggAccumulator = initAggAccumulator(functionType, type);
            initAggAccumulator.fromBinary(slice.getBytes());
            PartialAggAccumulator mergedAggAccumulator = getPartialAggAccumulator(type, functionType);
            mergedAggAccumulator.merge(initAggAccumulator);
        }

        @Override
        public void merge(Type type, AggIndexAccumulatorState state, AggIndex.AggFunctionType functionType)
        {
            PartialAggAccumulator mergedAggAccumulator = getPartialAggAccumulator(type, functionType);
            mergedAggAccumulator.merge(state.getPartialAggAccumulator(type, functionType));
        }

        @Override
        public PartialAggAccumulator getPartialAggAccumulator(Type type, AggIndex.AggFunctionType functionType)
        {
            init(type, functionType);
            return getPartialAggAccumulator();
        }

        private PartialAggAccumulator getPartialAggAccumulator()
        {
            if (aggAccumulator == null) {
                aggAccumulator = initAggAccumulator(functionType, type);
            }

            return aggAccumulator;
        }

        @Override
        public Object getResult(Type type, AggIndex.AggFunctionType functionType)
        {
            return getPartialAggAccumulator(type, functionType).getResult();
        }

        @Override
        public void addMemoryUsage(long value)
        {
            // noop
        }

        @Override
        public boolean resultIsNull()
        {
            return aggAccumulator == null || aggAccumulator.resultIsNull();
        }

        @Override
        public Type getType()
        {
            return type;
        }

        @Override
        public AggIndex.AggFunctionType getAggFunctionType()
        {
            return functionType;
        }

        private void init(Type type, AggIndex.AggFunctionType functionType)
        {
            this.type = type;
            this.functionType = functionType;
        }
    }

    public static class GroupedAggIndexAccumulatorState
            extends AbstractGroupedAccumulatorState
            implements AggIndexAccumulatorState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedAggIndexAccumulatorState.class).instanceSize();

        private Type type;
        private AggIndex.AggFunctionType functionType;
        private final ObjectBigArray<PartialAggAccumulator> aggAccumulators = new ObjectBigArray();
        private long size;

        public GroupedAggIndexAccumulatorState() {}

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + aggAccumulators.sizeOf();
        }

        @Override
        public void merge(Type type, Slice slice, AggIndex.AggFunctionType functionType)
        {
            PartialAggAccumulator partialAggAccumulator = initAggAccumulator(functionType, type);
            partialAggAccumulator.fromBinary(slice.getBytes());
            PartialAggAccumulator mergedAggAccumulator = getPartialAggAccumulator(type, functionType);
            addMemoryUsage(-mergedAggAccumulator.estimatedMemorySize());
            mergedAggAccumulator.merge(partialAggAccumulator);
            addMemoryUsage(mergedAggAccumulator.estimatedMemorySize());
        }

        @Override
        public void merge(Type type, AggIndexAccumulatorState state, AggIndex.AggFunctionType functionType)
        {
            PartialAggAccumulator mergedAggAccumulator = getPartialAggAccumulator(type, functionType);
            addMemoryUsage(-mergedAggAccumulator.estimatedMemorySize());
            mergedAggAccumulator.merge(state.getPartialAggAccumulator(type, functionType));
            addMemoryUsage(mergedAggAccumulator.estimatedMemorySize());
        }

        private PartialAggAccumulator getPartialAggAccumulator()
        {
            PartialAggAccumulator aggAccumulator = aggAccumulators.get(getGroupId());
            if (aggAccumulator == null) {
                PartialAggAccumulator partialAggAccumulator = initAggAccumulator(functionType, type);
                aggAccumulators.set(getGroupId(), partialAggAccumulator);
            }
            return aggAccumulators.get(getGroupId());
        }

        @Override
        public PartialAggAccumulator getPartialAggAccumulator(Type type, AggIndex.AggFunctionType functionType)
        {
            init(type, functionType);
            return getPartialAggAccumulator();
        }

        @Override
        public Object getResult(Type type, AggIndex.AggFunctionType functionType)
        {
            return getPartialAggAccumulator(type, functionType).getResult();
        }

        @Override
        public void addMemoryUsage(long value)
        {
            this.size += value;
        }

        @Override
        public boolean resultIsNull()
        {
            return aggAccumulators.get(getGroupId()) == null || aggAccumulators.get(getGroupId()).resultIsNull();
        }

        @Override
        public Type getType()
        {
            return type;
        }

        @Override
        public AggIndex.AggFunctionType getAggFunctionType()
        {
            return functionType;
        }

        @Override
        public void ensureCapacity(long size)
        {
            aggAccumulators.ensureCapacity(size);
        }

        private void init(Type type, AggIndex.AggFunctionType functionType)
        {
            this.type = type;
            this.functionType = functionType;
        }
    }

    private static PartialAggAccumulator initAggAccumulator(AggIndex.AggFunctionType functionType, Type type)
    {
        org.apache.iceberg.types.Type icebergType = TypeConverter.toIcebergType(type);
        checkArgument(functionType != null, "Agg function type is null!");
        switch (functionType) {
            case AVG:
                return AvgAccumulator.forType(icebergType);
            default:
                throw new IllegalArgumentException("Unexpected agg function type: " + functionType);
        }
    }
}
