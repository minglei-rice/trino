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

import io.trino.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

public class AggIndexPercentileStateFactory
        implements AccumulatorStateFactory<AggIndexPercentileAccumulatorState>
{
    @Override
    public AggIndexPercentileAccumulatorState createSingleState()
    {
        return new SingleAggIndexPercentileState();
    }

    @Override
    public AggIndexPercentileAccumulatorState createGroupedState()
    {
        return new GroupedAggIndexPercentileState();
    }

    public static class GroupedAggIndexPercentileState
            extends AbstractGroupedPercentileAccumulatorState
            implements AggIndexPercentileAccumulatorState
    {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(GroupedAggIndexPercentileState.class).instanceSize();
        private double percentile;

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + accumulators.sizeOf();
        }

        @Override
        public Double getPercentile()
        {
            return percentile;
        }

        @Override
        public void setPercentile(Double percentile)
        {
            this.percentile = percentile;
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

    public static class SingleAggIndexPercentileState
            extends AbstractSinglePercentileAccumulatorState
            implements AggIndexPercentileAccumulatorState
    {
        public static final long INSTANCE_SIZE = ClassLayout.parseClass(SingleAggIndexPercentileState.class).instanceSize();
        private double percentile;

        public SingleAggIndexPercentileState()
        {
        }

        @Override
        public Double getPercentile()
        {
            return percentile;
        }

        @Override
        public void setPercentile(Double percentile)
        {
            this.percentile = percentile;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + (percentileAccumulator == null ? 0 : percentileAccumulator.estimatedMemorySize());
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
