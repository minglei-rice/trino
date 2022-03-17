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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import org.apache.iceberg.FilterMetrics;

public class MetricsUtils
{
    public static final String TOTAL_SPLITS_IN_PARTITIONS = "iceberg_total_splits_in_partitions";
    public static final String TOTAL_SPLITS_READ = "iceberg_total_splits_read";
    public static final String SKIPPED_SPLITS_BY_INDEX = "iceberg_skipped_splits_by_index";
    public static final String SKIPPED_SPLITS_BY_MINMAX = "iceberg_skipped_splits_by_minmax";
    public static final String SKIPPED_SPLITS_BY_DF_IN_COORDINATOR = "iceberg_skipped_splits_by_df_in_coordinator";
    public static final String SKIPPED_SPLITS_BY_DF_IN_WORKER = "iceberg_skipped_splits_by_df_in_worker";

    private MetricsUtils() {}

    public static Metrics makeMetricsFromFilterMetrics(FilterMetrics filterMetrics)
    {
        if (filterMetrics == null) {
            return Metrics.EMPTY;
        }

        ImmutableMap.Builder<String, Metric<?>> metricsBuilder = ImmutableMap.builder();
        metricsBuilder.put(TOTAL_SPLITS_IN_PARTITIONS, new LongCount(
                filterMetrics.getMetricEntry(FilterMetrics.MetricType.TOTAL)
                        .map(FilterMetrics.MetricEntry::getRawSplitCount)
                        .orElse(0)));
        metricsBuilder.put(SKIPPED_SPLITS_BY_MINMAX, new LongCount(
                filterMetrics.getMetricEntry(FilterMetrics.MetricType.SKIPPED_BY_MINMAX)
                        .map(FilterMetrics.MetricEntry::getRawSplitCount)
                        .orElse(0)));
        return new Metrics(metricsBuilder.build());
    }

    public static Metrics makeLongCountMetrics(String name, long count)
    {
        return new Metrics(ImmutableMap.of(name, new LongCount(count)));
    }
}
