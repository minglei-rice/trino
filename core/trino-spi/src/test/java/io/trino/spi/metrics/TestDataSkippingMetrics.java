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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static io.trino.spi.metrics.DataSkippingMetrics.MetricType.READ;
import static io.trino.spi.metrics.DataSkippingMetrics.MetricType.SKIPPED_BY_MINMAX_STATS;
import static io.trino.spi.metrics.DataSkippingMetrics.MetricType.TOTAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDataSkippingMetrics
{
    @Test
    public void testMergeDataSkippingMetrics()
    {
        DataSkippingMetrics metrics1 = DataSkippingMetrics.builder()
                .withMetric(TOTAL, 1, 100)
                .withMetric(SKIPPED_BY_MINMAX_STATS, 2, 1000)
                .build();
        DataSkippingMetrics metrics2 = DataSkippingMetrics.builder()
                .withMetric(SKIPPED_BY_MINMAX_STATS, 3, 10000)
                .withMetric(READ, 4, 100000)
                .build();
        DataSkippingMetrics merged = metrics1.mergeWith(metrics2);

        DataSkippingMetrics expected = DataSkippingMetrics.builder()
                .withMetric(TOTAL, 1, 100)
                .withMetric(SKIPPED_BY_MINMAX_STATS, 5, 11000)
                .withMetric(READ, 4, 100000)
                .build();

        assertThat(merged.getMetricMap()).isEqualTo(expected.getMetricMap());
    }

    @Test
    public void testMergeTableLevelDataSkippingMetrics()
    {
        TableLevelDataSkippingMetrics metrics1 = new TableLevelDataSkippingMetrics(ImmutableMap.of(
                "table1", DataSkippingMetrics.builder().withMetric(TOTAL, 1, 100).build()));
        TableLevelDataSkippingMetrics metrics2 = new TableLevelDataSkippingMetrics(ImmutableMap.of(
                "table1", DataSkippingMetrics.builder().withMetric(TOTAL, 2, 1000).build()));
        TableLevelDataSkippingMetrics metrics3 = new TableLevelDataSkippingMetrics(ImmutableMap.of(
                "table2", DataSkippingMetrics.builder().withMetric(TOTAL, 4, 10000).build()));
        TableLevelDataSkippingMetrics merged = metrics1.mergeWith(metrics2).mergeWith(metrics3);

        TableLevelDataSkippingMetrics expected = new TableLevelDataSkippingMetrics(ImmutableMap.of(
                "table1", DataSkippingMetrics.builder().withMetric(TOTAL, 3, 1100).build(),
                "table2", DataSkippingMetrics.builder().withMetric(TOTAL, 4, 10000).build()));

        assertThat(getDataSkippingMetrics(merged, "table1").getMetricMap())
                .isEqualTo(getDataSkippingMetrics(expected, "table1").getMetricMap());
        assertThat(getDataSkippingMetrics(merged, "table2").getMetricMap())
                .isEqualTo(getDataSkippingMetrics(expected, "table2").getMetricMap());
    }

    private DataSkippingMetrics getDataSkippingMetrics(TableLevelDataSkippingMetrics metrics, String table)
    {
        return metrics.getDataSkippingMetricsMap().get(table);
    }
}
