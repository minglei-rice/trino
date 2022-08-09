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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.CorrColFilterApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.PUSHDOWN_CORR_COL_FILTERS;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.ExpressionUtils.filterDeterministicConjuncts;
import static io.trino.sql.planner.iterative.rule.PushJoinIntoTableScan.getJoinType;
import static io.trino.sql.planner.iterative.rule.PushJoinIntoTableScan.getPushableJoinCondition;
import static io.trino.sql.planner.iterative.rule.PushJoinIntoTableScan.toAssignments;
import static io.trino.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

/**
 * An optimization rule to push down correlated column filters into joining table.
 */
public class PushCorrColFilterIntoTableScan
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

    private final PlannerContext plannerContext;

    public PushCorrColFilterIntoTableScan(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return session.getSystemProperty(PUSHDOWN_CORR_COL_FILTERS, Boolean.class);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        JoinSource joinSource = collectJoinSource(context, node);
        Map<TableScanNode, TableHandle> oldToNewTableHandle = new HashMap<>();
        pushDown(context, joinSource, oldToNewTableHandle);
        if (oldToNewTableHandle.isEmpty()) {
            return Result.empty();
        }
        return Result.ofPlanNode(node.accept(new ReplaceTableScans(context.getLookup()), oldToNewTableHandle));
    }

    private void pushDown(Context context, JoinSource joinSource, Map<TableScanNode, TableHandle> oldToNewTableHandle)
    {
        // from left to right
        for (TableScanAndFilter leftTable : joinSource.leftSrc) {
            if (leftTable.hasFilter()) {
                for (TableScanAndFilter rightTable : joinSource.rightSrc) {
                    pushDown(joinSource.joinNode, context, leftTable, rightTable, true).ifPresent(r -> oldToNewTableHandle.put(rightTable.tableScan, r.getHandle()));
                }
            }
        }
        // from right to left
        for (TableScanAndFilter rightTable : joinSource.rightSrc) {
            if (rightTable.hasFilter()) {
                for (TableScanAndFilter leftTable : joinSource.leftSrc) {
                    pushDown(joinSource.joinNode, context, rightTable, leftTable, false).ifPresent(r -> oldToNewTableHandle.put(leftTable.tableScan, r.getHandle()));
                }
            }
        }
    }

    private Optional<CorrColFilterApplicationResult<TableHandle>> pushDown(JoinNode joinNode, Context context, TableScanAndFilter src, TableScanAndFilter target, boolean srcIsLeft)
    {
        Constraint constraint = toConstraint(context.getSession(), src.tableScan, src.filter, context.getSymbolAllocator());
        if (constraint.getSummary().isAll()) {
            return Optional.empty();
        }
        TableHandle table = target.tableScan.getTable();
        TableHandle corrTable = src.tableScan.getTable();
        // TODO: join key symbol may change after JoinNode and become different from the original ones in table scan
        Set<Symbol> leftSymbols = ImmutableSet.copyOf(srcIsLeft ? src.tableScan.getOutputSymbols() : target.tableScan.getOutputSymbols());
        Set<Symbol> rightSymbols = ImmutableSet.copyOf(srcIsLeft ? target.tableScan.getOutputSymbols() : src.tableScan.getOutputSymbols());

        Expression joinCondExpr = and(joinNode.getCriteria().stream().map(JoinNode.EquiJoinClause::toExpression).collect(toImmutableList()));
        List<JoinCondition> joinConditions = new ArrayList<>();
        for (Expression conjunct : extractConjuncts(joinCondExpr)) {
            Optional<JoinCondition> pushableCond = getPushableJoinCondition(conjunct, leftSymbols, rightSymbols, context.getSymbolAllocator());
            // unsupported condition, bail out
            if (pushableCond.isEmpty()) {
                return Optional.empty();
            }
            joinConditions.add(pushableCond.get());
        }

        Map<String, ColumnHandle> tableAssignments = toAssignments(target.tableScan);
        Map<String, ColumnHandle> corrTableAssignments = toAssignments(src.tableScan);
        return plannerContext.getMetadata().applyCorrColFilter(
                context.getSession(),
                table,
                corrTable,
                getJoinType(joinNode),
                joinConditions,
                tableAssignments,
                corrTableAssignments,
                !srcIsLeft,
                constraint);
    }

    private Constraint toConstraint(Session session, TableScanNode src, FilterNode filterNode, SymbolAllocator symbolAllocator)
    {
        Expression predicate = filterNode.getPredicate();

        // don't include non-deterministic predicates
        Expression deterministicPredicate = filterDeterministicConjuncts(plannerContext.getMetadata(), predicate);

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(
                plannerContext,
                session,
                deterministicPredicate,
                symbolAllocator.getTypes());

        TupleDomain<Symbol> tupleDomain = decomposedPredicate.getTupleDomain();
        TupleDomain<ColumnHandle> newDomain;
        // return ALL if any filter is not on src table columns
        if (!tupleDomain.isNone() && tupleDomain.getDomains().get().keySet().stream().anyMatch(s -> !src.getAssignments().containsKey(s))) {
            newDomain = TupleDomain.all();
        }
        else {
            newDomain = tupleDomain
                    .transformKeys(src.getAssignments()::get)
                    .intersect(src.getEnforcedConstraint());
        }
        return new Constraint(newDomain);
    }

    private static GetTableScanResult getDirectTableScan(Context context, PlanNode planNode)
    {
        FilterNode filter = null;
        while (!(planNode instanceof TableScanNode)) {
            if (planNode instanceof GroupReference) {
                planNode = context.getLookup().resolve(planNode);
            }
            else if (planNode instanceof FilterNode) {
                // TODO: how to handle multiple filters
                if (filter != null) {
                    break;
                }
                filter = (FilterNode) planNode;
                planNode = filter.getSource();
            }
            else if (planNode instanceof ProjectNode) {
                planNode = ((ProjectNode) planNode).getSource();
            }
            else {
                break;
            }
        }
        return new GetTableScanResult(planNode, filter);
    }

    private static JoinSource collectJoinSource(Context context, JoinNode join)
    {
        GetTableScanResult leftResult = getDirectTableScan(context, join.getLeft());
        List<TableScanAndFilter> leftSrc = new ArrayList<>();
        if (leftResult.hasTableScan()) {
            leftSrc.add(new TableScanAndFilter(leftResult.getTableScan(), leftResult.filter));
        }
        else if (leftResult.stopAt instanceof JoinNode) {
            // recursively process join in left sub-tree
            JoinSource leftJoinSource = collectJoinSource(context, (JoinNode) leftResult.stopAt);
            leftSrc.addAll(leftJoinSource.leftSrc);
            leftSrc.addAll(leftJoinSource.rightSrc);
        }

        GetTableScanResult rightResult = getDirectTableScan(context, join.getRight());
        List<TableScanAndFilter> rightSrc = new ArrayList<>();
        if (rightResult.hasTableScan()) {
            rightSrc.add(new TableScanAndFilter(rightResult.getTableScan(), rightResult.filter));
        }
        else if (rightResult.stopAt instanceof JoinNode) {
            // recursively process join in right sub-tree
            JoinSource rightJoinSource = collectJoinSource(context, (JoinNode) rightResult.stopAt);
            rightSrc.addAll(rightJoinSource.leftSrc);
            rightSrc.addAll(rightJoinSource.rightSrc);
        }

        return new JoinSource(join, leftSrc, rightSrc);
    }

    // represents all sources (both direct and in-direct) of a join
    private static class JoinSource
    {
        private final JoinNode joinNode;
        private final List<TableScanAndFilter> leftSrc;
        private final List<TableScanAndFilter> rightSrc;

        private JoinSource(JoinNode joinNode, List<TableScanAndFilter> leftSrc, List<TableScanAndFilter> rightSrc)
        {
            this.joinNode = joinNode;
            this.leftSrc = leftSrc;
            this.rightSrc = rightSrc;
        }
    }

    // represents a table scan source of a join
    private static class TableScanAndFilter
    {
        private final TableScanNode tableScan;
        private final FilterNode filter;

        private TableScanAndFilter(TableScanNode tableScan, FilterNode filter)
        {
            this.tableScan = requireNonNull(tableScan);
            this.filter = filter;
        }

        boolean hasFilter()
        {
            return filter != null;
        }
    }

    // represents the result of searching for table scan node
    private static class GetTableScanResult
    {
        private final PlanNode stopAt;
        private final FilterNode filter;

        private GetTableScanResult(PlanNode stopAt, FilterNode filter)
        {
            this.stopAt = requireNonNull(stopAt);
            this.filter = filter;
        }

        TableScanNode getTableScan()
        {
            return (TableScanNode) stopAt;
        }

        boolean hasTableScan()
        {
            return stopAt instanceof TableScanNode;
        }

        boolean hasFilter()
        {
            // TODO: what if filter has been pushed down into the table scan
            return filter != null;
        }

        boolean hasTableScanWithFilter()
        {
            return hasTableScan() && hasFilter();
        }
    }

    // a visitor to replace table scan nodes with new table handles
    private static class ReplaceTableScans
            extends PlanVisitor<PlanNode, Map<TableScanNode, TableHandle>>
    {
        private final Lookup lookup;

        private ReplaceTableScans(Lookup lookup)
        {
            this.lookup = lookup;
        }

        @Override
        public PlanNode visitGroupReference(GroupReference node, Map<TableScanNode, TableHandle> context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Map<TableScanNode, TableHandle> context)
        {
            return node.replaceChildren(Collections.singletonList(node.getSource().accept(this, context)));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Map<TableScanNode, TableHandle> context)
        {
            return node.replaceChildren(Collections.singletonList(node.getSource().accept(this, context)));
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Map<TableScanNode, TableHandle> context)
        {
            PlanNode left = node.getLeft().accept(this, context);
            PlanNode right = node.getRight().accept(this, context);
            return node.replaceChildren(Arrays.asList(left, right));
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Map<TableScanNode, TableHandle> context)
        {
            TableHandle newHandle = context.get(node);
            if (newHandle == null) {
                return node;
            }
            return new TableScanNode(
                    node.getId(),
                    newHandle,
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getEnforcedConstraint(),
                    node.getStatistics(),
                    node.isUpdateTarget(),
                    node.getUseConnectorNodePartitioning());
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Map<TableScanNode, TableHandle> context)
        {
            return node;
        }
    }
}
