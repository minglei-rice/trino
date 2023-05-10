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

import io.airlift.slice.SizeOf;
import io.trino.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class AggIndexPercentileArrayStateFactory
        implements AccumulatorStateFactory<AggIndexPercentileArrayAccumulatorState>
{
    @Override
    public AggIndexPercentileArrayAccumulatorState createSingleState()
    {
        return new SingleAggIndexPercentileArrayState();
    }

    @Override
    public AggIndexPercentileArrayAccumulatorState createGroupedState()
    {
        return new GroupedAggIndexPercentileArrayState();
    }

    public static class GroupedAggIndexPercentileArrayState
            extends AbstractGroupedPercentileAccumulatorState
            implements AggIndexPercentileArrayAccumulatorState
    {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(GroupedAggIndexPercentileArrayState.class).instanceSize();
        private List<Double> percentiles;

        @Override
        public List<Double> getPercentiles()
        {
            return percentiles;
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            this.percentiles = requireNonNull(percentiles, "percentiles is null");
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (percentiles != null) {
                estimatedSize += SizeOf.sizeOfDoubleArray(percentiles.size());
            }
            return estimatedSize + size + accumulators.sizeOf();
        }

        @Override
        public Double getWeight()
        {
            return weight;
        }

        @Override
        public void setWeight(Double weight)
        {
            this.weight = weight;
        }
    }

    public static class SingleAggIndexPercentileArrayState
            extends AbstractSinglePercentileAccumulatorState
            implements AggIndexPercentileArrayAccumulatorState
    {
        public static final long INSTANCE_SIZE = ClassLayout.parseClass(SingleAggIndexPercentileArrayState.class).instanceSize();
        private List<Double> percentiles;

        @Override
        public List<Double> getPercentiles()
        {
            return percentiles;
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            this.percentiles = requireNonNull(percentiles, "percentiles is null");
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (percentileAccumulator != null) {
                estimatedSize += percentileAccumulator.estimatedMemorySize();
            }
            if (percentiles != null) {
                estimatedSize += SizeOf.sizeOfDoubleArray(percentiles.size());
            }
            return estimatedSize;
        }

        @Override
        public Double getWeight()
        {
            return weight;
        }

        @Override
        public void setWeight(Double weight)
        {
            this.weight = weight;
        }
    }
}
