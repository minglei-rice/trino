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
package io.trino.plugin.iceberg.functions;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproxDistinctAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproxDistinctPreAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateArrayPercentilePreAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateDoublePercentileAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateDoublePercentileArrayAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateLongPercentileAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateLongPercentileArrayAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximatePercentilePreAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateRealArrayPercentilePreAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateRealPercentileAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateRealPercentileArrayAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexApproximateRealPercentilePreAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexAveragePreAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexCountDistinctAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexDecimalAverageAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexDecimalAveragePreAggregations;
import io.trino.plugin.iceberg.functions.operator.aggregation.AggIndexDoubleAverageAggregations;
import io.trino.spi.Plugin;

import java.util.Set;

public class IcebergFunctionsPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(AggIndexDecimalAverageAggregations.class)
                .add(AggIndexDoubleAverageAggregations.class)
                .add(AggIndexCountDistinctAggregations.class)
                .add(AggIndexApproxDistinctAggregations.class)
                .add(AggIndexApproximateDoublePercentileAggregations.class)
                .add(AggIndexApproximateDoublePercentileArrayAggregations.class)
                .add(AggIndexApproximateLongPercentileAggregations.class)
                .add(AggIndexApproximateLongPercentileArrayAggregations.class)
                .add(AggIndexApproximateRealPercentileAggregations.class)
                .add(AggIndexApproximateRealPercentileArrayAggregations.class)
                .add(AggIndexApproxDistinctPreAggregations.class)
                .add(AggIndexApproximateArrayPercentilePreAggregations.class)
                .add(AggIndexApproximatePercentilePreAggregations.class)
                .add(AggIndexApproximateRealArrayPercentilePreAggregations.class)
                .add(AggIndexApproximateRealPercentilePreAggregations.class)
                .add(AggIndexAveragePreAggregations.class)
                .add(AggIndexDecimalAveragePreAggregations.class)
                .build();
    }
}
