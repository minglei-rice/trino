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
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MismatchedOrderTopNNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isPushPartialTopNIntoTableScan;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.topN;

public class PushPartialTopNIntoTableScan
        implements Rule<ExchangeNode>
{
    private final Metadata metadata;
    private static final Pattern<ExchangeNode> PATTERN = exchange()
            .matching(exchange -> exchange.getScope() == ExchangeNode.Scope.REMOTE)
            .matching(exchange -> exchange.getType() == ExchangeNode.Type.GATHER)
            .with(source().matching(topN().matching(topNNode -> topNNode.getStep() == TopNNode.Step.PARTIAL)));

    public PushPartialTopNIntoTableScan(PlannerContext plannerContext)
    {
        this.metadata = plannerContext.getMetadata();
    }

    @Override
    public Pattern<ExchangeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushPartialTopNIntoTableScan(session);
    }

    @Override
    public Result apply(ExchangeNode node, Captures captures, Context context)
    {
        Optional<TopNNode> topNNode = findTopNNode(node, context);
        Optional<TableScanNode> tableScanNode = findTableScanNode(node, context);
        if (topNNode.isEmpty() || tableScanNode.isEmpty()) {
            return Result.empty();
        }
        Session session = context.getSession();
        TableHandle table = tableScanNode.get().getTable();
        Optional<PartialSortApplicationResult> partialSortApplicationResult = metadata.applyPartialSort(session, table);
        if (partialSortApplicationResult.isEmpty() || !partialSortApplicationResult.get().isCanRewrite()) {
            return Result.empty();
        }
        if (partialSortApplicationResult.get().isAsc() == asc(topNNode.get())) {
            // the query sort order is equal to file order.
            LimitNode limitNode = buildLimitNode(tableScanNode.get(), topNNode.get());
            ExchangeNode exchangeNode = new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    node.getPartitioningScheme(),
                    ImmutableList.of(limitNode),
                    node.getInputs(),
                    node.getOrderingScheme());
            return Result.ofPlanNode(exchangeNode);
        }
        else {
            MismatchedOrderTopNNode mismatchedOrderTopNNode = buildMismatchedOrderTopNNode(tableScanNode.get(), topNNode.get());
            ExchangeNode exchangeNode = new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    node.getPartitioningScheme(),
                    ImmutableList.of(mismatchedOrderTopNNode),
                    node.getInputs(),
                    node.getOrderingScheme());
            return Result.ofPlanNode(exchangeNode);
        }
    }

    private boolean asc(TopNNode node)
    {
        return node.getOrderingScheme().getOrderingList().get(0).isAscending();
    }

    private Optional<TopNNode> findTopNNode(ExchangeNode node, Context context)
    {
        return PlanNodeSearcher.searchFrom(node, context.getLookup()).where(planNode -> planNode instanceof TopNNode).findFirst();
    }

    private Optional<TableScanNode> findTableScanNode(ExchangeNode node, Context context)
    {
        return PlanNodeSearcher.searchFrom(node, context.getLookup()).where(planNode -> planNode instanceof TableScanNode).findFirst();
    }

    private LimitNode buildLimitNode(TableScanNode scanNode, TopNNode node)
    {
        return new LimitNode(node.getId(), scanNode, node.getCount(), Optional.empty(), true, ImmutableList.of());
    }

    private MismatchedOrderTopNNode buildMismatchedOrderTopNNode(TableScanNode scanNode, TopNNode node)
    {
        return new MismatchedOrderTopNNode(node.getId(), scanNode, node.getCount());
    }
}
