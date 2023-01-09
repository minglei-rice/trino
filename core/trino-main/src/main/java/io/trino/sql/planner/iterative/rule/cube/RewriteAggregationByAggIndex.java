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
package io.trino.sql.planner.iterative.rule.cube;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionId;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.spi.aggindex.AggFunctionDesc;
import io.trino.spi.aggindex.AggIndex;
import io.trino.spi.aggindex.CorrColumns;
import io.trino.spi.aggindex.TableColumnIdentify;
import io.trino.spi.connector.AggIndexApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.planner.SimplePlanVisitor;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.DefaultExpressionTraversalVisitor;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.trino.SystemSessionProperties.isAllowReadAggIndexFiles;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.lang.String.format;

/**
 * Query can use cube data to query only when the query pattern matches the cube definition.
 * The column of "Group By" and the column in the "Where" condition must be the column defined in the dimension,
 * and the measure in SQL should be consistent with the measure defined in cube.
 *
 * The conditions for being able to query the cube are exactly the same as Apache/Kylin.
 */
public class RewriteAggregationByAggIndex
        implements Rule<AggregationNode>
{
    private static final Logger LOG = Logger.get(RewriteAggregationByAggIndex.class);

    private final Metadata metadata;

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(step().equalTo(AggregationNode.Step.SINGLE))
            .matching(RewriteAggregationByAggIndex::preCheck);

    private static final Set<String> SUPPORTED_AGG_FUNC = new HashSet<>(Arrays.asList("sum", "min", "max", "count", "avg", "approx_distinct", "approx_percentile"));

    public RewriteAggregationByAggIndex(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowReadAggIndexFiles(session);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Map<String, Symbol> nameToSymbol = context.getSymbolAllocator().getTypes().allTypes().keySet()
                .stream()
                .collect(Collectors.toMap(Symbol::getName, x -> x));
        Session session = context.getSession();
        List<TableScanNode> tableScanNodes = PlanNodeSearcher.searchFrom(node, context.getLookup())
                .where(TableScanNode.class::isInstance)
                .findAll();
        if (tableScanNodes.isEmpty()) {
            return Result.empty();
        }
        // Typically, a star schema will only have one table(fact table) with a cube definition. But the other tables (dimension tables) of
        // this star schema are fact tables inside another star schema and also define the cube.
        for (TableScanNode tableScan : tableScanNodes) {
            TableHandle tableHandle = tableScan.getTable();
            List<AggIndex> trinoAggIndex = metadata.getAggregationIndices(session, tableHandle);
            // no agg index defined on this table
            if (trinoAggIndex.isEmpty()) {
                continue;
            }
            AggIndex candidateAggIndex = null;
            RewriteUtil rewriteUtil = null;
            for (AggIndex aggIndex : trinoAggIndex) {
                rewriteUtil = new RewriteUtil(true).setAggIndex(aggIndex).setNameToSymbol(nameToSymbol);
                List<CorrColumns.Corr> corrList = aggIndex.getCorrColumns().stream().map(CorrColumns::getCorrelation).collect(Collectors.toList());
                boolean skipCurrAggIndex = false;
                for (CorrColumns.Corr corr : corrList) {
                    if (corr.getJoinConstraint() != CorrColumns.Corr.JoinConstraint.PK_FK) {
                        skipCurrAggIndex = true;
                        break;
                    }
                }
                if (skipCurrAggIndex) {
                    continue;
                }
                node.accept(new AggIndexVisitor(context.getLookup(), session, metadata), rewriteUtil);
                if (!rewriteUtil.canRewrite()) {
                    continue;
                }
                candidateAggIndex = aggIndex;
                break;
            }
            if (!rewriteUtil.canRewrite()) {
                continue;
            }

            RewriteUtil tmpRewriteUtil = rewriteUtil;
            boolean allowPartialAggIndex = node.getAggregations().values().stream()
                    .noneMatch(aggregation -> isCountDistinct(aggregation,
                            aggregation.getResolvedFunction().getSignature().getName(),
                            tmpRewriteUtil.getDistinctColumns()));
            candidateAggIndex.setAllowPartialAggIndex(allowPartialAggIndex);
            LOG.info("Find an AggIndex %s answer the query %s.", candidateAggIndex, session.getQueryId());
            Optional<AggIndexApplicationResult<TableHandle>> result = metadata.applyAggIndex(session, tableHandle, candidateAggIndex);
            if (result.isEmpty()) {
                LOG.warn("Push Agg Index down failed for the query %s.", session.getQueryId());
                continue;
            }

            TableHandle newTable = result.get().getHandle();

            // cube assignments
            Map<String, ColumnHandle> assignments = metadata.getColumnHandles(session, newTable);
            // cube schema column name to its original table column name
            Map<String, TableColumnIdentify> aggIndexFileColumnNameToIdentify = result.get().getAggIndexColumnNameToIdentify();
            // join indicator included
            Set<String> effectColumnNames = assignments.keySet().stream().filter(name -> !aggIndexFileColumnNameToIdentify.containsKey(name)).collect(Collectors.toSet());
            // for original query
            Map<TableColumnIdentify, Symbol> columnIdentToSymbol = rewriteUtil.getColumnIdentifyAndSymbol();
            Map<Symbol, TableColumnIdentify> aggArgsSymbolToColumn = rewriteUtil.getAggArgsSymbolToColumn();
            Collection<TableColumnIdentify> columnIdentifySet = columnIdentToSymbol.keySet();
            // column names in filter & group by expressions included
            effectColumnNames.addAll(aggIndexFileColumnNameToIdentify.entrySet().stream()
                    .filter(entry -> columnIdentifySet.contains(entry.getValue()))
                    .map(Map.Entry::getKey).collect(Collectors.toList()));
            Map<AggFunctionDesc, String> aggFunctionDescToName = candidateAggIndex.getAggFunctionDescToName();
            Set<Symbol> distinctColumns = rewriteUtil.getDistinctColumns();
            Map<Symbol, Expression> projectionMap = rewriteUtil.getProjectionMap();
            Set<String> aggregationNames = node.getAggregations().values().stream()
                    .map(aggregation -> aggFunctionDescToName.get(aggregationToAggFunctionDesc(aggregation, aggArgsSymbolToColumn, distinctColumns, projectionMap)))
                    .collect(Collectors.toSet());
            // agg column names included
            effectColumnNames.addAll(aggregationNames);
            // for later table scan node usage.
            Map<Symbol, ColumnHandle> newAssignments = new HashMap<>();
            // for later table scan node usage.
            List<Symbol> newOutputs = new ArrayList<>();
            for (Map.Entry<String, ColumnHandle> entry : assignments.entrySet()) {
                if (effectColumnNames.contains(entry.getKey())) {
                    TableColumnIdentify tableColumnIdentify = aggIndexFileColumnNameToIdentify.get(entry.getKey());
                    Symbol symbol = columnIdentToSymbol.get(tableColumnIdentify);
                    ColumnHandle columnHandle = entry.getValue();
                    if (symbol == null) {
                        symbol = context.getSymbolAllocator().newSymbol(entry.getKey(), metadata.getColumnMetadata(session, tableHandle, columnHandle).getType());
                    }
                    newAssignments.put(symbol, columnHandle);
                    newOutputs.add(symbol);
                }
            }

            PlanNode planNode = new TableScanNode(
                    context.getIdAllocator().getNextId(),
                    newTable,
                    newOutputs,
                    newAssignments,
                    TupleDomain.all(),
                    tableScan.getStatistics(),
                    tableScan.isUpdateTarget(),
                    tableScan.getUseConnectorNodePartitioning());
            List<Expression> exprCollector = rewriteUtil.getFilterCollector();
            List<Expression> expressions = ExpressionUtils.removeDuplicates(metadata, exprCollector);
            Expression expression = ExpressionUtils.combineConjuncts(metadata, expressions);
            if (exprCollector.size() > 0) {
                planNode = new FilterNode(context.getIdAllocator().getNextId(), planNode, expression);
                LOG.info("Conjunctive filter is %s for query %s", ((FilterNode) planNode).getPredicate().toString(), session.getQueryId());
            }

            if (projectionMap.size() > 0) {
                Assignments projectAssignments = Assignments.builder()
                        .putAll(projectionMap)
                        .putAll(newAssignments.keySet().stream().collect(Collectors.toMap(Function.identity(), Symbol::toSymbolReference)))
                        .build();
                planNode = new ProjectNode(context.getIdAllocator().getNextId(), planNode, projectAssignments);
            }

            Map<Symbol, AggregationNode.Aggregation> aggregationMap = rewriteAggregation(node.getAggregations(),
                    aggArgsSymbolToColumn, candidateAggIndex.getAggFunctionDescToName(), distinctColumns, projectionMap);

            if (result.get().isPartialResult()) {
                Map<Symbol, Symbol> aggToPreAggSymbol = new HashMap<>();
                PlanNode dataFilePreAggregationNode = new AggregationNode(context.getIdAllocator().getNextId(),
                        node.getSource(),
                        writePreAggregation(node.getAggregations(), context, aggToPreAggSymbol),
                        node.getGroupingSets(),
                        node.getPreGroupedSymbols(),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol());
                if (projectionMap.size() > 0) {
                    Assignments projectAssignments = Assignments.builder()
                            .putAll(projectionMap)
                            .putAll(dataFilePreAggregationNode.getOutputSymbols().stream().collect(Collectors.toMap(Function.identity(), Symbol::toSymbolReference)))
                            .build();
                    dataFilePreAggregationNode = new ProjectNode(context.getIdAllocator().getNextId(), dataFilePreAggregationNode, projectAssignments);
                }
                planNode = buildUnionAllNode(dataFilePreAggregationNode, planNode, aggregationMap, context, aggToPreAggSymbol);
            }
            return Result.ofPlanNode(new AggregationNode(
                    context.getIdAllocator().getNextId(),
                    planNode,
                    aggregationMap,
                    node.getGroupingSets(),
                    node.getPreGroupedSymbols(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol()));
        }
        return Result.empty();
    }

    private static class AggIndexVisitor
            extends SimplePlanVisitor<RewriteUtil>
    {
        private final Lookup lookup;
        private final Session session;
        private final Metadata metadata;
        private final AstVisitor<Void, RewriteUtil> filterExpressionVisitor;

        private AggIndexVisitor(Lookup lookup, Session session, Metadata metadata)
        {
            this.lookup = lookup;
            this.session = session;
            this.metadata = metadata;
            this.filterExpressionVisitor = new FilterExpressionVisitor();
        }

        @Override
        public Void visitAggregation(AggregationNode node, RewriteUtil rewriteUtil)
        {
            rewriteUtil.increaseCurrentAggregationDepth();
            if (!rewriteUtil.canRewrite()) {
                return null;
            }

            if (node.getAggregations().isEmpty()) {
                // for case count distinct is optimized by SingleDistinctAggregationToGroupBy
                rewriteUtil.addDistinctColumns(node.getGroupingSets().getGroupingKeys());
            }

            node.getSource().accept(this, rewriteUtil);
            if (!rewriteUtil.canRewrite()) {
                return null;
            }
            List<TableColumnIdentify> dimFieldsFromPlan = node.getGroupingSets().getGroupingKeys()
                    .stream()
                    .filter(symbol -> !rewriteUtil.getDistinctColumns().contains(symbol))
                    .map(groupKey -> rewriteUtil.getSymbolToTableColumnName().getOrDefault(groupKey, TableColumnIdentify.NONE))
                    .collect(Collectors.toList());
            List<TableColumnIdentify> dimFieldsFromAggIndex = rewriteUtil.getAggIndex().getDimFields();
            if (!dimFieldsFromAggIndex.containsAll(dimFieldsFromPlan)) {
                rewriteUtil.setCanRewrite(false);
                tuneAggregationNode(rewriteUtil.getAggIndex().getAggIndexId(), dimFieldsFromPlan, dimFieldsFromAggIndex, List.of(), Set.of());
                return null;
            }
            else {
                // matched
                for (int i = 0; i < dimFieldsFromPlan.size(); i++) {
                    rewriteUtil.putColumnIdentifyAndSymbol(dimFieldsFromPlan.get(i), node.getGroupingSets().getGroupingKeys().get(i));
                }
            }

            Function<Expression, TableColumnIdentify> applyExpr = exp -> {
                if (exp instanceof SymbolReference) {
                    return rewriteUtil.getSymbolToTableColumnName().getOrDefault(rewriteUtil.getNameToSymbol().get(((SymbolReference) exp).getName()), TableColumnIdentify.NONE);
                }
                // count(*).
                return null;
            };

            Function<Expression, Symbol> applyExprToSymbol = exp -> {
                if (exp instanceof SymbolReference) {
                    return rewriteUtil.getNameToSymbol().get(((SymbolReference) exp).getName());
                }
                return null;
            };

            // compare Aggregation Function Body
            List<AggFunctionDesc> aggFunctionFromPlan = new ArrayList<>();
            Map<Symbol, AggregationNode.Aggregation> aggregations = node.getAggregations();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
                AggregationNode.Aggregation aggregation = entry.getValue();
                String functionName = rewriteAggFunctionName(aggregation, aggregation.getResolvedFunction().getSignature().getName(), rewriteUtil.getDistinctColumns());
                AggFunctionDesc aggFunctionDesc;
                // count(*)
                if (aggregation.getArguments().size() == 0 && "count".equalsIgnoreCase(functionName)) {
                    aggFunctionDesc = new AggFunctionDesc(functionName, null, new ArrayList<>());
                }
                else {
                    List<Object> attributes = new ArrayList<>();
                    // only support single argument, do not support function like max(x1,x2) with two arguments.
                    // approx_distinct with maxStandardError is not supported
                    // approx_percentile has multiple arguments
                    if (aggregation.getArguments().size() > 1 && !checkApproxPercentileAndSetAttributes(
                            aggregation.getResolvedFunction().getSignature().getName(), aggregation, attributes, rewriteUtil.getProjectionMap())) {
                        rewriteUtil.setCanRewrite(false);
                        return null;
                    }
                    TableColumnIdentify tableColumnIdent;
                    if (aggregation.getArguments().size() > 0) {
                        tableColumnIdent = applyExpr.apply(aggregation.getArguments().get(0));
                    }
                    else {
                        // count(*) -> count_distinct
                        if (rewriteUtil.getDistinctColumns().size() != 1) {
                            rewriteUtil.setCanRewrite(false);
                            LOG.warn("This may be misjudged for query %s, however to be safety, skip rewriting agg index", session.getQueryId());
                            return null;
                        }
                        Symbol symbol = Iterables.getOnlyElement(rewriteUtil.getDistinctColumns());
                        tableColumnIdent = applyExpr.apply(symbol.toSymbolReference());
                        rewriteUtil.putAggArgsSymbolToColumn(symbol, tableColumnIdent);
                    }
                    aggFunctionDesc = new AggFunctionDesc(functionName, tableColumnIdent, attributes);
                }
                aggFunctionFromPlan.add(aggFunctionDesc);
            }
            Set<AggFunctionDesc> aggFunctionFromAggIndex = rewriteUtil.getAggIndex().getAggFunctionDescToName().keySet();
            if (!aggFunctionFromAggIndex.containsAll(aggFunctionFromPlan)) {
                rewriteUtil.setCanRewrite(false);
                tuneAggregationNode(rewriteUtil.getAggIndex().getAggIndexId(), List.of(), List.of(), aggFunctionFromPlan, aggFunctionFromAggIndex);
            }
            else {
                List<AggregationNode.Aggregation> aggArguments = new ArrayList<>(aggregations.values());
                for (AggregationNode.Aggregation aggregation : aggArguments) {
                    if (aggregation.getArguments().size() > 0) {
                        Expression expression = aggregation.getArguments().get(0);
                        Symbol measure = applyExprToSymbol.apply(expression);
                        rewriteUtil.putAggArgsSymbolToColumn(measure, rewriteUtil.getSymbolToTableColumnName()
                                .getOrDefault(measure, TableColumnIdentify.NONE));
                    }
                }
            }
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, RewriteUtil context)
        {
            node.getLeft().accept(this, context);
            if (!context.canRewrite()) {
                return null;
            }
            node.getRight().accept(this, context);
            if (!context.canRewrite()) {
                return null;
            }

            BiPredicate<JoinNode, Lookup> notStarSchema = (joinNode, lookUp) -> {
                PlanNode left = joinNode.getLeft();
                if (left instanceof GroupReference) {
                    left = lookup.resolve(left);
                }
                PlanNode right = joinNode.getRight();
                if (right instanceof GroupReference) {
                    right = lookup.resolve(right);
                }
                return left instanceof JoinNode && right instanceof JoinNode;
            };
            if (notStarSchema.test(node, lookup)) {
                context.setCanRewrite(false);
                tuneJoinNode(
                        context.getAggIndex().getAggIndexId(),
                        node.getType(),
                        TableColumnIdentify.NONE,
                        TableColumnIdentify.NONE,
                        "Only support star schema.");
                return null;
            }

            JoinNode.Type type = node.getType();
            // TODO support right join
            if (type == JoinNode.Type.FULL || type == JoinNode.Type.RIGHT) {
                context.setCanRewrite(false);
                tuneJoinNode(
                        context.getAggIndex().getAggIndexId(),
                        type,
                        TableColumnIdentify.NONE,
                        TableColumnIdentify.NONE,
                        "Full or Right join is not supported now.");
                return null;
            }

            Predicate<JoinNode> leftJoinRightInputContainsFilter = x -> {
                if (x.getType().equals(JoinNode.Type.LEFT)) {
                    return PlanNodeSearcher.searchFrom(x.getRight(), lookup).where(FilterNode.class::isInstance).matches();
                }
                return false;
            };

            // left join right output has a filter, we do not response.
            if (leftJoinRightInputContainsFilter.test(node)) {
                context.setCanRewrite(false);
                tuneJoinNode(
                        context.getAggIndex().getAggIndexId(),
                        node.getType(),
                        TableColumnIdentify.NONE,
                        TableColumnIdentify.NONE,
                        "Left join right input has a filter is not supported now.");
                return null;
            }

            List<CorrColumns.Corr> corrList = context.getAggIndex().getCorrColumns().stream().map(CorrColumns::getCorrelation).collect(Collectors.toList());
            int index = -1;
            // TODO t1 left join t2 on t1.id = t2.id left join t3 on t1.id = t3.id
            for (CorrColumns.Corr corr : corrList) {
                for (JoinNode.EquiJoinClause joinClause : node.getCriteria()) {
                    // When generated the `JoinNode` by `RelationPlanner#visitJoin` visitor, it has guaranteed
                    // that the left key of JoinNode must come from the left table.
                    TableColumnIdentify leftKeyFromPlan = context.getSymbolToTableColumnName()
                            .getOrDefault(joinClause.getLeft(), TableColumnIdentify.NONE);
                    TableColumnIdentify rightKeyFromPlan = context.getSymbolToTableColumnName()
                            .getOrDefault(joinClause.getRight(), TableColumnIdentify.NONE);
                    index = corr.getLeftKeys().indexOf(leftKeyFromPlan);
                    if (index == -1) {
                        break;
                    }
                    TableColumnIdentify rightKeyFromAggIndex = corr.getRightKeys().get(index);
                    if (!Objects.equals(rightKeyFromPlan, rightKeyFromAggIndex)) {
                        index = -1;
                        break;
                    }
                }
                // means current join node has matched.
                if (index != -1) {
                    break;
                }
            }

            if (index == -1) {
                context.setCanRewrite(false);
                tuneJoinNode(
                        context.getAggIndex().getAggIndexId(),
                        node.getType(),
                        TableColumnIdentify.NONE,
                        TableColumnIdentify.NONE,
                        "Join node can not match the correlation");
                return null;
            }
            return null;
        }

        @Override
        protected Void visitPlan(PlanNode node, RewriteUtil context)
        {
            context.setCanRewrite(false);
            return null;
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, RewriteUtil context)
        {
            node.getSource().accept(this, context);
            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, RewriteUtil context)
        {
            context.putAllProjectionMap(node.getAssignments().getMap());
            node.getSource().accept(this, context);
            return null;
        }

        /*
         * TODO
         * if we have a query [ T1 inner join T2 on T1.f1=T2.f2 where T1.f1 = 'a' ], T1.f1 is a dim field in cube meta dimensions,
         * T2.f2 is not in cube meta dimensions, after optimizer apply PredicatePushDown on this query, there will be a new filter
         * [ where T2.f2 = 'a'] on T2 table. Obviously, this can go wrong in subgraph matching because T2.f2 field is
         * not defined in cube meta dimensions, so we need work around this.
         *
         * Currently, Only the following four types of expressions are supported.
         */
        @Override
        public Void visitFilter(FilterNode node, RewriteUtil context)
        {
            node.getSource().accept(this, context);
            if (!context.canRewrite()) {
                return null;
            }
            filterExpressionVisitor.process(node.getPredicate(), context);
            if (context.canRewrite) {
                context.setFilterCollector(node.getPredicate());
            }
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, RewriteUtil context)
        {
            List<Symbol> outputSymbols = node.getOutputSymbols();
            // collect all symbols to ColumnIdent for a query.
            Map<Symbol, TableColumnIdentify> symbolToTableColumnIdent = new HashMap<>();
            String tableName = metadata.getTableSchema(session, node.getTable()).getTable().getTableName();
            for (Symbol symbol : outputSymbols) {
                symbolToTableColumnIdent.put(symbol,
                        new TableColumnIdentify(tableName, metadata.getColumnMetadata(session, node.getTable(), node.getAssignments().get(symbol)).getName()));
            }
            context.setSymbolToTableColumnIdent(symbolToTableColumnIdent);
            return null;
        }

        @Override
        public Void visitGroupReference(GroupReference node, RewriteUtil context)
        {
            lookup.resolve(node).accept(this, context);
            return null;
        }

        private static class FilterExpressionVisitor
                extends DefaultExpressionTraversalVisitor<RewriteUtil>
        {
            /**
             * Base condition(SymbolReference is the leaf node) when traverse the plan tree.
             */
            @Override
            protected Void visitSymbolReference(SymbolReference symbolRef, RewriteUtil context)
            {
                TableColumnIdentify filterExpColumnField = context.getSymbolToTableColumnName()
                        .getOrDefault(context.getNameToSymbol().get(symbolRef.getName()), TableColumnIdentify.NONE);
                boolean match = context.getAggIndex().getDimFields().contains(filterExpColumnField);
                if (!match) {
                    context.setCanRewrite(false);
                    tuneFilterNode(
                            context.getAggIndex().getAggIndexId(),
                            symbolRef,
                            context.getAggIndex().getDimFields());
                    return null;
                }
                context.putColumnIdentifyAndSymbol(filterExpColumnField, context.getNameToSymbol().get(symbolRef.getName()));
                return null;
            }
        }
    }

    private static class RewriteUtil
    {
        private static final int MAX_AGGREGATION_DEPTH = 2;
        private boolean canRewrite;

        private final List<Expression> filterCollector;

        /**
         * For FilterNode, the key is the column in the original table of the symbol used by the filter expression.
         * For AggregationNode, key is the column in the original table of the aggregation node grouping key
         *
         * Because the optimized logical plan is basically a single stage which may only contains `Aggregation -> Filter -> TableScan`.
         * Note that AggregationNode and FilterNode input symbols are original from TableScan output symbols, only collect
         * symbols from aggregation node grouping key and FilterNode.
         */
        private final Map<TableColumnIdentify, Symbol> columnIdentifyToSymbol;

        // symbols from AggregationNode arguments
        private final Map<Symbol, TableColumnIdentify> aggArgsSymbolToColumn;

        /**
         * The key is the output symbols of all TableScanNodes.
         * The value is the column field name of the original table.
         */
        private final Map<Symbol, TableColumnIdentify> symbolToTableColumnIdent;

        private final Set<Symbol> distinctColumns;

        private final Map<Symbol, Expression> projectionMap;

        private AggIndex aggIndex;

        private Map<String, Symbol> nameToSymbol;

        private int currentAggregationDepth;

        public RewriteUtil(boolean canRewrite)
        {
            this.canRewrite = canRewrite;
            this.symbolToTableColumnIdent = new HashMap<>();
            this.columnIdentifyToSymbol = new HashMap<>();
            this.nameToSymbol = new HashMap<>();
            this.filterCollector = new ArrayList<>();
            this.aggArgsSymbolToColumn = new HashMap<>();
            this.distinctColumns = new HashSet<>();
            this.projectionMap = new HashMap<>();
        }

        public Map<String, Symbol> getNameToSymbol()
        {
            return nameToSymbol;
        }

        public RewriteUtil setNameToSymbol(Map<String, Symbol> nameToSymbol)
        {
            this.nameToSymbol = nameToSymbol;
            return this;
        }

        public AggIndex getAggIndex()
        {
            return aggIndex;
        }

        public RewriteUtil setAggIndex(AggIndex aggIndex)
        {
            this.aggIndex = aggIndex;
            return this;
        }

        public List<Expression> getFilterCollector()
        {
            return filterCollector;
        }

        public void setFilterCollector(Expression expr)
        {
            filterCollector.add(expr);
        }

        public void putColumnIdentifyAndSymbol(TableColumnIdentify identify, Symbol symbol)
        {
            columnIdentifyToSymbol.put(identify, symbol);
        }

        public Map<TableColumnIdentify, Symbol> getColumnIdentifyAndSymbol()
        {
            return columnIdentifyToSymbol;
        }

        public void putAggArgsSymbolToColumn(Symbol symbol, TableColumnIdentify identify)
        {
            aggArgsSymbolToColumn.put(symbol, identify);
        }

        public Map<Symbol, TableColumnIdentify> getAggArgsSymbolToColumn()
        {
            return aggArgsSymbolToColumn;
        }

        public boolean canRewrite()
        {
            return canRewrite;
        }

        public void setCanRewrite(boolean canRewrite)
        {
            this.canRewrite = canRewrite;
        }

        public Map<Symbol, TableColumnIdentify> getSymbolToTableColumnName()
        {
            return symbolToTableColumnIdent;
        }

        public void setSymbolToTableColumnIdent(Map<Symbol, TableColumnIdentify> toTableSchemaColumn)
        {
            symbolToTableColumnIdent.putAll(toTableSchemaColumn);
        }

        public void addDistinctColumns(List<Symbol> symbols)
        {
            distinctColumns.addAll(symbols);
        }

        public Set<Symbol> getDistinctColumns()
        {
            return distinctColumns;
        }

        public void putAllProjectionMap(Map<Symbol, Expression> symbolToValue)
        {
            projectionMap.putAll(symbolToValue);
        }

        public Map<Symbol, Expression> getProjectionMap()
        {
            return projectionMap;
        }

        public void increaseCurrentAggregationDepth()
        {
            currentAggregationDepth++;
            if (currentAggregationDepth > MAX_AGGREGATION_DEPTH) {
                setCanRewrite(false);
            }
        }
    }

    private static boolean preCheck(AggregationNode node)
    {
        // grouping sets is not supported
        if (node.getGroupIdSymbol().isPresent() || node.getHashSymbol().isPresent()) {
            return false;
        }
        boolean allSymRef = true;
        Map<Symbol, AggregationNode.Aggregation> aggregations = node.getAggregations();
        if (aggregations.size() == 0) {
            return false;
        }

        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
            AggregationNode.Aggregation aggregation = entry.getValue();
            String name = aggregation.getResolvedFunction().getSignature().toSignature().getName();
            boolean allowFunc = SUPPORTED_AGG_FUNC.contains(name);
            if ((aggregation.isDistinct()
                    || aggregation.getFilter().isPresent()
                    || aggregation.getOrderingScheme().isPresent()
                    || aggregation.getMask().isPresent()
                    || !allowFunc) && !isCountDistinct(aggregation, name, null)) {
                return false;
            }
            allSymRef = allSymRef && entry.getValue().getArguments().stream().allMatch(SymbolReference.class::isInstance);
        }
        return allSymRef;
    }

    private static boolean isCountDistinct(AggregationNode.Aggregation aggregation, String name, Set<Symbol> distinctColumns)
    {
        if ("count".equalsIgnoreCase(name)) {
            if (aggregation.isDistinct()) {
                return true;
            }

            if (aggregation.getMask().isPresent() && aggregation.getMask().get().getName().endsWith("distinct")) {
                return true;
            }

            if (distinctColumns != null && distinctColumns.size() > 0) {
                // count(*) -> count distinct
                if (aggregation.getArguments().size() == 0 && distinctColumns.size() == 1) {
                    return true;
                }
                return distinctColumns.contains(Symbol.from(aggregation.getArguments().get(0)));
            }
        }
        return false;
    }

    private static boolean isApproxDistinct(String name)
    {
        return "approx_distinct".equalsIgnoreCase(name);
    }

    private static boolean isApproxPercentile(String name)
    {
        return "approx_percentile".equalsIgnoreCase(name);
    }

    private static boolean checkApproxPercentileAndSetAttributes(String name, AggregationNode.Aggregation aggregation, List<Object> attributes,
                                                                 Map<Symbol, Expression> projectionMap)
    {
        if (isApproxPercentile(name)) {
            attributes.addAll(getAggFuncAttributes(aggregation, projectionMap));
            return attributes.size() > 0;
        }
        return false;
    }

    private static List<Object> getAggFuncAttributes(AggregationNode.Aggregation aggregation, Map<Symbol, Expression> projectionMap)
    {
        List<Object> attributes = new ArrayList<>();
        String funcName = aggregation.getResolvedFunction().getSignature().getName();
        if (isApproxPercentile(funcName)) {
            if (aggregation.getArguments().size() > 3) {
                // LegacyApproximateLongPercentileAggregations function with accuracy is not supported
                LOG.warn("Query %s, approx_percentile with param: accuracy is not supported for agg index!");
                return attributes;
            }
            double weight = 1.0D;
            if (aggregation.getArguments().size() == 3) {
                Expression weightExp = projectionMap.get(Symbol.from(aggregation.getArguments().get(1)));
                if (weightExp == null || !(weightExp instanceof DoubleLiteral)) {
                    LOG.warn("approx_percentile with param: weight can not be recognized by agg index!");
                    return attributes;
                }
                weight = ((DoubleLiteral) weightExp).getValue();
            }
            attributes.add(weight);
        }
        return attributes;
    }

    private static String rewriteAggFunctionName(AggregationNode.Aggregation aggregation, String name, Set<Symbol> countDistinctColumns)
    {
        if (isCountDistinct(aggregation, name, countDistinctColumns)) {
            return AggIndex.AggFunctionType.COUNT_DISTINCT.getName();
        }

        if (isApproxDistinct(name)) {
            return AggIndex.AggFunctionType.APPROX_COUNT_DISTINCT.getName();
        }

        if (isApproxPercentile(name)) {
            return AggIndex.AggFunctionType.PERCENTILE.getName();
        }

        return name;
    }

    private static void tuneAggregationNode(
            int aggIndexId,
            List<TableColumnIdentify> dimColumnFromPlan,
            List<TableColumnIdentify> dimColumnFromConnector,
            List<AggFunctionDesc> aggFunctionDescFromPlan,
            Set<AggFunctionDesc> aggFunctionDescFromConnector)
    {
        LOG.info("agg index id is %s, dim column from plan %s", aggIndexId, Arrays.toString(dimColumnFromPlan.toArray()));
        LOG.info("agg index id is %s, dim column from cube %s", aggIndexId, Arrays.toString(dimColumnFromConnector.toArray()));
        LOG.info("agg index id is %s, function from plan %s", aggIndexId, Arrays.toString(aggFunctionDescFromPlan.toArray()));
        LOG.info("agg index id is %s, function from cube %s", aggIndexId, Arrays.toString(aggFunctionDescFromConnector.toArray()));
    }

    private static void tuneJoinNode(
            int aggIndexId,
            JoinNode.Type type,
            TableColumnIdentify fromAggIndex,
            TableColumnIdentify fromPlan,
            String extraMessage)
    {
        LOG.info("agg index id is %s, the logical plan join node type %s", aggIndexId, type.getJoinLabel());
        LOG.info("agg index id is %s, join clause from agg index %s", aggIndexId, fromAggIndex.toString());
        LOG.info("agg index id is %s, join clause from plan %s", aggIndexId, fromPlan.toString());
        LOG.info("agg index id is %s, extra message is %s", aggIndexId, extraMessage);
    }

    private static void tuneFilterNode(
            int aggIndexId,
            SymbolReference symbolReference,
            List<TableColumnIdentify> dimFields)
    {
        LOG.info("agg index id is %s, filter symbol reference is %s", aggIndexId, symbolReference.toString());
        LOG.info("agg index id is %s, dim fields is %s", aggIndexId, Arrays.toString(dimFields.toArray()));
    }

    private static Map<Symbol, AggregationNode.Aggregation> rewriteAggregation(Map<Symbol, AggregationNode.Aggregation> aggregationMap,
                                                                               Map<Symbol, TableColumnIdentify> aggArgsSymbolToColumn,
                                                                               Map<AggFunctionDesc, String> aggFunctionDescToName,
                                                                               Set<Symbol> distinctColumns,
                                                                               Map<Symbol, Expression> projectionMap)
    {
        Map<Symbol, AggregationNode.Aggregation> result = new HashMap<>();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregationMap.entrySet()) {
            ResolvedFunction prevResolvedFunction = entry.getValue().getResolvedFunction();
            BoundSignature boundSignature = prevResolvedFunction.getSignature();
            FunctionId functionId = prevResolvedFunction.getFunctionId();
            FunctionNullability functionNullability = prevResolvedFunction.getFunctionNullability();
            Map<TypeSignature, Type> typeDependencies = prevResolvedFunction.getTypeDependencies();
            String newAggArgName = aggFunctionDescToName.get(aggregationToAggFunctionDesc(entry.getValue(), aggArgsSymbolToColumn, distinctColumns, projectionMap));
            List<Expression> arguments = new ArrayList<>(Collections.singletonList(new SymbolReference(newAggArgName)));
            String functionFormat = "%s<t>(t):%s";
            switch (boundSignature.getName().toLowerCase(Locale.ROOT)) {
                case "avg":
                    String avgName = "cube_avg_double";
                    String funcIdName = String.format(functionFormat, avgName, StandardTypes.DOUBLE);
                    if (boundSignature.getReturnType() instanceof DecimalType) {
                        avgName = "cube_avg_decimal";
                        funcIdName = String.format(functionFormat, avgName, StandardTypes.DECIMAL);
                    }
                    boundSignature = new BoundSignature(avgName, boundSignature.getReturnType(), ImmutableList.of(VARBINARY));
                    functionId = new FunctionId(funcIdName);
                    typeDependencies = ImmutableMap.of(new TypeSignature("varbinary"), prevResolvedFunction.getSignature().getArgumentTypes().get(0));
                    break;
                case "count":
                    if (isCountDistinct(entry.getValue(), "count", distinctColumns)) {
                        boundSignature = new BoundSignature("cube_count_distinct", boundSignature.getReturnType(), ImmutableList.of(VARBINARY));
                    }
                    else {
                        boundSignature = new BoundSignature("sum", prevResolvedFunction.getSignature().getReturnType(), ImmutableList.of(BigintType.BIGINT));
                    }
                    functionNullability = new FunctionNullability(true, ImmutableList.of(false));
                    functionId = FunctionId.toFunctionId(boundSignature.toSignature());
                    break;
                case "approx_distinct":
                    String approxDistinctName = "cube_approx_distinct";
                    funcIdName = String.format(functionFormat, approxDistinctName, StandardTypes.BIGINT);
                    boundSignature = new BoundSignature(approxDistinctName, boundSignature.getReturnType(), ImmutableList.of(VARBINARY));
                    functionId = new FunctionId(funcIdName);
                    typeDependencies = ImmutableMap.of(new TypeSignature("varbinary"), prevResolvedFunction.getSignature().getArgumentTypes().get(0));
                    break;
                case "approx_percentile":
                    String approxPercentileName = getAggIndexApproxPercentileName(boundSignature.getArgumentTypes().get(0));
                    List<Type> paramTypes = new ImmutableList.Builder<Type>()
                            .add(VARBINARY)
                            .addAll(boundSignature.getArgumentTypes().subList(1, boundSignature.getArgumentTypes().size()))
                            .build();
                    boundSignature = new BoundSignature(approxPercentileName, boundSignature.getReturnType(), ImmutableList.copyOf(paramTypes));
                    functionId = FunctionId.toFunctionId(boundSignature.toSignature());
                    arguments.addAll(entry.getValue().getArguments().subList(1, entry.getValue().getArguments().size()));
                    break;
                case "sum":
                case "min":
                case "max":
                    break;
                default:
                    throw new IllegalArgumentException(format("Unsupported function name %s for cube aggregation rewrite!", entry.getKey().getName()));
            }

            ResolvedFunction resolvedFunction = new ResolvedFunction(
                    boundSignature,
                    functionId,
                    prevResolvedFunction.getFunctionKind(),
                    prevResolvedFunction.isDeterministic(),
                    functionNullability,
                    typeDependencies,
                    prevResolvedFunction.getFunctionDependencies());
            result.put(entry.getKey(), aggregationMapping(entry.getValue(), resolvedFunction, arguments));
        }
        return result;
    }

    private static Map<Symbol, AggregationNode.Aggregation> writePreAggregation(Map<Symbol, AggregationNode.Aggregation> aggregationMap, Context context, Map<Symbol, Symbol> aggSymbolMapping)
    {
        Map<Symbol, AggregationNode.Aggregation> result = new HashMap<>();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregationMap.entrySet()) {
            ResolvedFunction prevResolvedFunction = entry.getValue().getResolvedFunction();
            BoundSignature boundSignature = prevResolvedFunction.getSignature();
            FunctionId functionId = prevResolvedFunction.getFunctionId();
            FunctionNullability functionNullability = prevResolvedFunction.getFunctionNullability();
            Map<TypeSignature, Type> typeDependencies = prevResolvedFunction.getTypeDependencies();
            Symbol key = entry.getKey();
            String functionFormat = "%s<t>(t):varbinary";
            switch (boundSignature.getName().toLowerCase(Locale.ROOT)) {
                case "avg":
                    String avgName = "cube_avg_double_pre";
                    String funcIdName = String.format("%s(%s):varbinary", avgName, boundSignature.getArgumentType(0));
                    typeDependencies = ImmutableMap.of(boundSignature.getReturnType().getTypeSignature(), prevResolvedFunction.getSignature().getArgumentTypes().get(0));
                    if (boundSignature.getReturnType() instanceof DecimalType) {
                        avgName = "cube_avg_decimal_pre";
                        funcIdName = String.format(functionFormat, avgName);
                    }
                    boundSignature = new BoundSignature(avgName, VARBINARY, boundSignature.getArgumentTypes());
                    functionId = new FunctionId(funcIdName);
                    key = context.getSymbolAllocator().newSymbol(avgName + "_" + Symbol.from(entry.getValue().getArguments().get(0)).getName(), VARBINARY);
                    break;
                case "approx_distinct":
                    String approxDistinctName = "cube_approx_distinct_pre";
                    funcIdName = String.format(functionFormat, approxDistinctName);
                    Type type = prevResolvedFunction.getSignature().getArgumentTypes().get(0);
                    typeDependencies = ImmutableMap.of(type.getTypeSignature(), type);
                    boundSignature = new BoundSignature(approxDistinctName, VARBINARY, boundSignature.getArgumentTypes());
                    functionId = new FunctionId(funcIdName);
                    key = context.getSymbolAllocator().newSymbol(approxDistinctName + "_" + Symbol.from(entry.getValue().getArguments().get(0)).getName(), VARBINARY);
                    break;
                case "approx_percentile":
                    String approxPercentileName = getAggIndexPreApproxPercentileName(boundSignature.getArgumentTypes().get(0));
                    boundSignature = new BoundSignature(approxPercentileName, VARBINARY, boundSignature.getArgumentTypes());
                    functionId = FunctionId.toFunctionId(boundSignature.toSignature());
                    key = context.getSymbolAllocator().newSymbol(approxPercentileName + "_" + Symbol.from(entry.getValue().getArguments().get(0)).getName(), VARBINARY);
                    break;
                case "count":
                case "sum":
                case "min":
                case "max":
                    break;
                default:
                    throw new IllegalArgumentException(format("Unsupported function name %s to write cube pre aggregation!", entry.getKey().getName()));
            }

            aggSymbolMapping.put(entry.getKey(), key);
            ResolvedFunction resolvedFunction = new ResolvedFunction(
                    boundSignature,
                    functionId,
                    prevResolvedFunction.getFunctionKind(),
                    prevResolvedFunction.isDeterministic(),
                    functionNullability,
                    typeDependencies,
                    prevResolvedFunction.getFunctionDependencies());
            result.put(key, aggregationMapping(entry.getValue(), resolvedFunction, entry.getValue().getArguments()));
        }
        return result;
    }

    private static String getAggIndexApproxPercentileName(Type type)
    {
        if (type instanceof DoubleType) {
            return "cube_approx_double_percentile";
        }

        if (type instanceof BigintType) {
            return "cube_approx_long_percentile";
        }

        if (type instanceof RealType) {
            return "cube_approx_float_percentile";
        }

        throw new IllegalArgumentException(format("Invalid type %s for agg index: approx_percentile!", type));
    }

    private static String getAggIndexPreApproxPercentileName(Type type)
    {
        if (type instanceof DoubleType || type instanceof BigintType || type instanceof IntegerType) {
            return "cube_approx_percentile_pre";
        }

        if (type instanceof RealType) {
            return "cube_approx_float_percentile_pre";
        }

        throw new IllegalArgumentException(format("Invalid type %s for pre agg index: approx_percentile!", type));
    }

    private static AggregationNode.Aggregation aggregationMapping(AggregationNode.Aggregation aggregation, ResolvedFunction resolvedFunction, List<Expression> arguments)
    {
        return new AggregationNode.Aggregation(resolvedFunction,
                arguments,
                false,
                aggregation.getFilter(),
                aggregation.getOrderingScheme(),
                Optional.empty());
    }

    private static AggFunctionDesc aggregationToAggFunctionDesc(AggregationNode.Aggregation aggregation,
                                                                Map<Symbol, TableColumnIdentify> symbolToColumnIdentify,
                                                                Set<Symbol> distinctColumns,
                                                                Map<Symbol, Expression> projectionMap)
    {
        TableColumnIdentify tableColumnIdentify = null;
        String aggFunctionName = rewriteAggFunctionName(aggregation, aggregation.getResolvedFunction().getSignature().getName(), distinctColumns);
        // for count(*), the arguments will with size 0
        if (aggregation.getArguments().size() == 0 && AggIndex.AggFunctionType.COUNT_DISTINCT.getName().equals(aggFunctionName)) {
            tableColumnIdentify = symbolToColumnIdentify.get(distinctColumns.iterator().next());
        }
        else if (aggregation.getArguments().size() > 0) {
            tableColumnIdentify = symbolToColumnIdentify.get(Symbol.from(aggregation.getArguments().get(0)));
        }
        return new AggFunctionDesc(aggFunctionName, tableColumnIdentify, getAggFuncAttributes(aggregation, projectionMap));
    }

    private static PlanNode buildUnionAllNode(PlanNode dataFileSource, PlanNode aggIndexSource,
                                              Map<Symbol, AggregationNode.Aggregation> aggregationMap,
                                              Context context, Map<Symbol, Symbol> aggSymbolMapping)
    {
        ImmutableListMultimap.Builder<Symbol, Symbol> builder = new ImmutableListMultimap.Builder<>();
        dataFileSource.getOutputSymbols().stream().filter(aggIndexSource.getOutputSymbols()::contains)
                .forEach(symbol -> builder.putAll(symbol, Arrays.asList(symbol, symbol)));
        List<Symbol> outputs = ImmutableList.copyOf(aggIndexSource.getOutputSymbols());
        aggregationMap.entrySet().forEach(entry -> builder.putAll(getAggregationSymbol(entry.getKey(), entry.getValue()), Arrays.asList(getAggregationSymbol(entry.getKey(), entry.getValue()),
                aggSymbolMapping.get(entry.getKey()))));
        return new UnionNode(
                context.getIdAllocator().getNextId(),
                ImmutableList.of(aggIndexSource, dataFileSource),
                builder.build(),
                outputs);
    }

    private static Symbol getAggregationSymbol(Symbol defaultSymbol, AggregationNode.Aggregation aggregation)
    {
        if (aggregation.getArguments().size() > 0) {
            return Symbol.from(aggregation.getArguments().get(0));
        }
        return defaultSymbol;
    }
}
