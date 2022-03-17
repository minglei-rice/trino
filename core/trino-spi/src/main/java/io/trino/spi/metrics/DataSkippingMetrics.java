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
package io.trino.spi.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DataSkippingMetrics
        implements Metric<DataSkippingMetrics>
{
    public static final DataSkippingMetrics EMPTY = new DataSkippingMetrics(Map.of());

    private final Map<MetricType, MetricEntry> metricMap;

    @JsonCreator
    public DataSkippingMetrics(Map<MetricType, MetricEntry> metricMap)
    {
        this.metricMap = Map.copyOf(requireNonNull(metricMap, "metricMap is null"));
    }

    @JsonValue
    public Map<MetricType, MetricEntry> getMetricMap()
    {
        return metricMap;
    }

    @Override
    public DataSkippingMetrics mergeWith(DataSkippingMetrics other)
    {
        Map<MetricType, MetricEntry> mergedMap = new HashMap<>(metricMap);
        other.metricMap.forEach((metricType, metricEntry) ->
                mergedMap.merge(metricType, metricEntry, MetricEntry::mergeWith));
        return new DataSkippingMetrics(mergedMap);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public enum MetricType
    {
        TOTAL,
        READ,
        SKIPPED_BY_MINMAX_STATS,
        SKIPPED_BY_INDEX_IN_COORDINATOR,
        SKIPPED_BY_INDEX_IN_WORKER,
        SKIPPED_BY_DF_IN_COORDINATOR,
        SKIPPED_BY_DF_IN_WORKER,
        SKIPPED_BY_PART_FILTER
    }

    public static class MetricEntry
    {
        private final int splitCount;
        private final long dataSize;

        @JsonCreator
        public MetricEntry(
                @JsonProperty("splitCount") int splitCount,
                @JsonProperty("dataSize") long dataSize)
        {
            this.splitCount = splitCount;
            this.dataSize = dataSize;
        }

        @JsonProperty
        public int getSplitCount()
        {
            return this.splitCount;
        }

        @JsonProperty
        public long getDataSize()
        {
            return this.dataSize;
        }

        public MetricEntry mergeWith(MetricEntry other)
        {
            return new MetricEntry(splitCount + other.splitCount, dataSize + other.dataSize);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MetricEntry other = (MetricEntry) o;
            return splitCount == other.splitCount && dataSize == other.dataSize;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(splitCount, dataSize);
        }
    }

    public static class Builder
    {
        private final Map<MetricType, MetricEntry> metricMap = new HashMap<>();

        public Builder withMetric(MetricType metricType, MetricEntry metricEntry)
        {
            metricMap.put(metricType, metricEntry);
            return this;
        }

        public Builder withMetric(MetricType metricType, int splitCount, long dataSize)
        {
            metricMap.put(metricType, new MetricEntry(splitCount, dataSize));
            return this;
        }

        public DataSkippingMetrics build()
        {
            return metricMap.isEmpty() ? EMPTY : new DataSkippingMetrics(metricMap);
        }
    }
}
