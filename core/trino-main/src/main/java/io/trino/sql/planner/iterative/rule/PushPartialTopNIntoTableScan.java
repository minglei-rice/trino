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
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.PartialSortApplicationResult;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.SimplePlanVisitor;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SortedRecordTailNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.isPushPartialTopNIntoTableScan;
import static io.trino.sql.planner.plan.Patterns.TopN.step;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * The premise of using this rule is that the data in the table is already sorted,
 * so we can directly read the sorted data in order. Rule that transform plan like
 * <pre> {@code
 *    Top[partial | asc]
 *      Project
 *        Filter
 *          TableScan
 *        }
 * </pre>
 * to
 * <pre> {@code
 *    Limit[partial]
 *      Project
 *        Filter
 *         TableScan
 *    }
 *  </pre>
 *  or the field sort order (for example: desc) of the query is inconsistent with the field sort order
 *  definition(for example: asc),Rule that transform plan like
 * <pre> {@code
 *    Top[partial | desc]
 *      Project
 *        Filter
 *          TableScan}
 * </pre>
 *  to
 *  <pre> {@code
 *    SortedRecordTail
 *      Project
 *        Filter
 *          TableScan}
 *  </pre>
 *
 *  TODO
 *      1. with iceberg row set to filter data
 *      2. support offset and limit
 */
public class PushPartialTopNIntoTableScan
        implements Rule<ExchangeNode>
{
    private static final Logger LOG = Logger.get(PushPartialTopNIntoTableScan.class);
    private static final Capture<TopNNode> TOP_N_NODE = Capture.newCapture();
    private final Metadata metadata;

    // We need to ensure that the source node of TopN(partial) is either a Project, a Filter, or a TableScan.
    // In addition, if there are nodes other than TableScan, Project, Filter when traversing from TopN(partial),
    // then the optimization need fails.
    private static final Pattern<ExchangeNode> PATTERN = exchange()
            .matching(exchange -> exchange.getScope() == ExchangeNode.Scope.REMOTE && exchange.getType() == ExchangeNode.Type.GATHER)
            .with(source().matching(
                    Pattern.typeOf(TopNNode.class).capturedAs(TOP_N_NODE).with(step().equalTo(TopNNode.Step.PARTIAL))
                        .with(source().matching(
                                planNode -> planNode instanceof TableScanNode || planNode instanceof FilterNode || planNode instanceof ProjectNode))));

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
        LOG.info("Plan have matched %s.", this.getClass().getCanonicalName());
        TopNNode topNNode = captures.get(TOP_N_NODE);
        TableScanNode tableScanNode = findTableScanNode(node, context).get();
        Session session = context.getSession();
        TableHandle table = tableScanNode.getTable();
        Optional<PartialSortApplicationResult<TableHandle>> partialSortApplicationResult = metadata.applyPartialSort(session, table);
        if (partialSortApplicationResult.isEmpty() || !partialSortApplicationResult.get().isCanRewrite()) {
            return Result.empty();
        }
        QueryRewriteUtil queryRewriteUtil = new QueryRewriteUtil(true);
        topNNode.accept(new Visitor(context.getLookup(), partialSortApplicationResult.get().getSortOrder()), queryRewriteUtil);
        if (!queryRewriteUtil.isCanRewrite()) {
            return Result.empty();
        }
        TableHandle newTable = partialSortApplicationResult.get().getHandle();
        PlanNode planNode = context.getLookup().resolve(topNNode.getSource());
        if (partialSortApplicationResult.get().isAsc() == asc(topNNode)) {
            LimitNode limitNode;
            if (planNode instanceof TableScanNode) {
                limitNode = buildLimitNodeWithTableScanSource(buildTableScanNode(tableScanNode, newTable), topNNode);
            }
            else if (planNode instanceof FilterNode) {
                limitNode = buildLimitNodeWithFilterSource((FilterNode) planNode, topNNode, tableScanNode, newTable);
            }
            else {
                ProjectNode projectNode = (ProjectNode) planNode;
                limitNode = buildLimitNodeWithProjectSource(context, projectNode, tableScanNode, newTable, topNNode);
            }
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
            SortedRecordTailNode sortedRecordTailNode;
            if (planNode instanceof TableScanNode) {
                sortedRecordTailNode = buildSortedRecordTailNodeWithTableScanSource(buildTableScanNode(tableScanNode, newTable), topNNode);
            }
            else if (planNode instanceof FilterNode) {
                sortedRecordTailNode = buildSortedRecordTailNodeWithFilterSource(tableScanNode, (FilterNode) planNode, topNNode, newTable);
            }
            else {
                sortedRecordTailNode = buildSortedRecordTailNodeWithProjectSource(context, (ProjectNode) planNode, tableScanNode, newTable, topNNode);
            }
            ExchangeNode exchangeNode = new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    node.getPartitioningScheme(),
                    ImmutableList.of(sortedRecordTailNode),
                    node.getInputs(),
                    node.getOrderingScheme());
            return Result.ofPlanNode(exchangeNode);
        }
    }

    private static class QueryRewriteUtil
    {
        private boolean canRewrite;

        public QueryRewriteUtil(boolean canRewrite)
        {
            this.canRewrite = canRewrite;
        }

        public boolean isCanRewrite()
        {
            return canRewrite;
        }

        public void setCanRewrite(boolean canRewrite)
        {
            this.canRewrite = canRewrite;
        }
    }

    private static class Visitor
            extends SimplePlanVisitor<QueryRewriteUtil>
    {
        private final Lookup lookup;
        private final String sortOrder;

        private Visitor(Lookup lookup, String sortOrder)
        {
            this.lookup = lookup;
            this.sortOrder = sortOrder;
        }

        public String extractUnique(Symbol symbol)
        {
            String name = symbol.getName();
            int index = name.indexOf('_');
            if (index == -1) {
                return name;
            }
            return name.substring(0, index);
        }

        @Override
        public Void visitTopN(TopNNode node, QueryRewriteUtil context)
        {
            node.getSource().accept(this, context);
            Symbol symbol = node.getOrderingScheme().getOrderBy().get(0);
            String querySortOrder = extractUnique(symbol);
            if (!sortOrder.equalsIgnoreCase(querySortOrder)) {
                context.setCanRewrite(false);
            }
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, QueryRewriteUtil context)
        {
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, QueryRewriteUtil context)
        {
            node.getSource().accept(this, context);
            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, QueryRewriteUtil context)
        {
            node.getSource().accept(this, context);
            return null;
        }

        @Override
        protected Void visitPlan(PlanNode node, QueryRewriteUtil context)
        {
            context.setCanRewrite(false);
            return null;
        }

        @Override
        public Void visitGroupReference(GroupReference node, QueryRewriteUtil context)
        {
            lookup.resolve(node).accept(this, context);
            return null;
        }
    }

    private boolean asc(TopNNode node)
    {
        return node.getOrderingScheme().getOrderingList().get(0).isAscending();
    }

    private LimitNode buildLimitNodeWithTableScanSource(TableScanNode scanNode, TopNNode node)
    {
        return new LimitNode(node.getId(), scanNode, node.getCount(), Optional.empty(), true, ImmutableList.of());
    }

    private LimitNode buildLimitNodeWithFilterSource(FilterNode filterNode, TopNNode node, TableScanNode scanNode, TableHandle newHandle)
    {
        return new LimitNode(node.getId(), buildFilterNode(filterNode, scanNode, newHandle), node.getCount(), Optional.empty(), true, ImmutableList.of());
    }

    private LimitNode buildLimitNodeWithProjectSource(Context context, ProjectNode projectNode, TableScanNode scanNode, TableHandle newTable, TopNNode node)
    {
        return new LimitNode(node.getId(), buildProjectNode(context, projectNode, scanNode, newTable), node.getCount(), Optional.empty(), true, ImmutableList.of());
    }

    private SortedRecordTailNode buildSortedRecordTailNodeWithTableScanSource(TableScanNode scanNode, TopNNode node)
    {
        return new SortedRecordTailNode(node.getId(), scanNode, node.getCount());
    }

    private SortedRecordTailNode buildSortedRecordTailNodeWithFilterSource(TableScanNode scanNode, FilterNode filterNode, TopNNode node, TableHandle newTable)
    {
        return new SortedRecordTailNode(node.getId(), buildFilterNode(filterNode, scanNode, newTable), node.getCount());
    }

    private SortedRecordTailNode buildSortedRecordTailNodeWithProjectSource(Context context, ProjectNode projectNode, TableScanNode scanNode, TableHandle newTable, TopNNode node)
    {
        return new SortedRecordTailNode(node.getId(), buildProjectNode(context, projectNode, scanNode, newTable), node.getCount());
    }

    private FilterNode buildFilterNode(FilterNode filterNode, TableScanNode scanNode, TableHandle newTable)
    {
        return new FilterNode(filterNode.getId(), buildTableScanNode(scanNode, newTable), filterNode.getPredicate());
    }

    private ProjectNode buildProjectNode(Context context, ProjectNode projectNode, TableScanNode scanNode, TableHandle newTable)
    {
        PlanNode source = context.getLookup().resolve(projectNode.getSource());
        if (source instanceof FilterNode) {
            return new ProjectNode(projectNode.getId(), buildFilterNode((FilterNode) source, scanNode, newTable), projectNode.getAssignments());
        }
        else {
            return new ProjectNode(projectNode.getId(), buildTableScanNode(scanNode, newTable), projectNode.getAssignments());
        }
    }

    private TableScanNode buildTableScanNode(TableScanNode scanNode, TableHandle newTable)
    {
        return new TableScanNode(
                scanNode.getId(),
                newTable,
                scanNode.getOutputSymbols(),
                scanNode.getAssignments(),
                scanNode.getEnforcedConstraint(),
                scanNode.getStatistics(),
                scanNode.isUpdateTarget(),
                scanNode.getUseConnectorNodePartitioning());
    }

    private Optional<TableScanNode> findTableScanNode(ExchangeNode node, Context context)
    {
        return PlanNodeSearcher.searchFrom(node, context.getLookup()).where(planNode -> planNode instanceof TableScanNode).findFirst();
    }
}
