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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.PartialSortApplicationResult;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isPushPartialSortIntoTableScan;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.sort;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * If the fields of the table are in ascending order, this rule cannot
 * be used if the query is in reverse order.
 */
public class PushPartialSortIntoTableScan
        implements Rule<ExchangeNode>
{
    private final Metadata metadata;

    public PushPartialSortIntoTableScan(PlannerContext context)
    {
        this.metadata = context.getMetadata();
    }

    private static final Pattern<ExchangeNode> PATTERN = exchange()
            .matching(exchange -> exchange.getScope() == ExchangeNode.Scope.LOCAL)
            .matching(exchange -> exchange.getType() == ExchangeNode.Type.GATHER)
            .matching(exchange -> exchange.getOrderingScheme().isPresent())
            .with(source().matching(sort().matching(SortNode::isPartial)));

    @Override
    public Pattern<ExchangeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushPartialSortIntoTableScan(session);
    }

    @Override
    public Result apply(ExchangeNode node, Captures captures, Context context)
    {
        Optional<TableScanNode> tableScanNode = findTableScanNode(node, context);
        if (isReverse(node) || tableScanNode.isEmpty()) {
            return Result.empty();
        }
        Session session = context.getSession();
        TableHandle table = tableScanNode.get().getTable();

        Optional<PartialSortApplicationResult> partialSortApplicationResult = metadata.applyPartialSort(session, table);
        if (partialSortApplicationResult.isEmpty()) {
            return Result.empty();
        }
        return tableScanNode.map(value -> Result.ofPlanNode(new ExchangeNode(
                node.getId(),
                node.getType(),
                node.getScope(),
                node.getPartitioningScheme(),
                ImmutableList.of(value),
                node.getInputs(),
                node.getOrderingScheme()))).orElseGet(Result::empty);
    }

    private Optional<TableScanNode> findTableScanNode(ExchangeNode node, Context context)
    {
        return PlanNodeSearcher.searchFrom(node, context.getLookup()).where(planNode -> planNode instanceof TableScanNode).findFirst();
    }

    private boolean isReverse(ExchangeNode node)
    {
        return !node.getOrderingScheme().get().getOrderingList().get(0).isAscending();
    }
}
