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

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.aggindex.AggFunctionDesc;
import io.trino.spi.aggindex.AggIndex;
import io.trino.spi.aggindex.CorrColumns;
import io.trino.spi.aggindex.TableColumnIdentify;
import io.trino.spi.connector.AggIndexApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.planner.SimplePlanVisitor;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.DefaultExpressionTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.trino.SystemSessionProperties.isAllowReadAggIndexFiles;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.aggregation;

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

            LOG.info("Find an AggIndex %s answer the query.", candidateAggIndex);
            Optional<AggIndexApplicationResult<TableHandle>> result = metadata.applyAggIndex(session, tableHandle, candidateAggIndex);
            if (result.isEmpty()) {
                LOG.warn("Push Agg Index down failed, in most of time, this is not expected to happen.");
                continue;
            }
            TableHandle newTable = result.get().getHandle();

            // cube assignments
            Map<String, ColumnHandle> assignments = metadata.getColumnHandles(session, newTable);
            // cube schema column name to its original table column name
            Map<String, TableColumnIdentify> aggIndexFileColumnNameToIdentify = result.get().getAggIndexColumnNameToIdentify();
            // for original query
            Map<TableColumnIdentify, Symbol> columnIdentToSymbol = rewriteUtil.getColumnIdentifyAndSymbol();
            // for later table scan node usage.
            Map<Symbol, ColumnHandle> newAssignments = new HashMap<>();
            // for later table scan node usage.
            List<Symbol> newOutputs = new ArrayList<>();
            for (Map.Entry<String, ColumnHandle> entry : assignments.entrySet()) {
                String columnNameInAggIndexFile = entry.getKey();
                ColumnHandle columnHandle = entry.getValue();
                TableColumnIdentify identify = aggIndexFileColumnNameToIdentify.get(columnNameInAggIndexFile);
                if (identify != null) {
                    Symbol symbol = columnIdentToSymbol.get(identify);
                    if (symbol != null) {
                        newAssignments.put(symbol, columnHandle);
                        newOutputs.add(symbol);
                    }
                }
                else {
                    Symbol joinIndicator = context.getSymbolAllocator().newSymbol(columnNameInAggIndexFile, BOOLEAN);
                    newAssignments.put(joinIndicator, columnHandle);
                    newOutputs.add(joinIndicator);
                }
            }
            TableScanNode tableScanNode = new TableScanNode(
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
            FilterNode filterNode = new FilterNode(context.getIdAllocator().getNextId(), tableScanNode, expression);
            LOG.info("Conjunctive filter is %s", filterNode.getPredicate().toString());
            return Result.ofPlanNode(new AggregationNode(
                    context.getIdAllocator().getNextId(),
                    exprCollector.size() > 0 ? filterNode : tableScanNode,
                    node.getAggregations(),
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
            node.getSource().accept(this, rewriteUtil);
            if (!rewriteUtil.canRewrite()) {
                return null;
            }
            List<TableColumnIdentify> dimFieldsFromPlan = node.getGroupingSets().getGroupingKeys()
                    .stream()
                    .map(groupKey -> rewriteUtil.getSymbolToTableColumnName().getOrDefault(groupKey, TableColumnIdentify.NONE))
                    .collect(Collectors.toList());
            List<TableColumnIdentify> dimFieldsFromAggIndex = rewriteUtil.getAggIndex().getDimFields();
            if (!dimFieldsFromAggIndex.containsAll(dimFieldsFromPlan)) {
                rewriteUtil.setCanRewrite(false);
                tuneAggregationNode(rewriteUtil.getAggIndex().getAggIndexId(), dimFieldsFromPlan, dimFieldsFromAggIndex, List.of(), List.of());
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
                String functionName = aggregation.getResolvedFunction().getSignature().getName();
                AggFunctionDesc aggFunctionDesc;
                // count(*)
                if (aggregation.getArguments().size() == 0) {
                    aggFunctionDesc = new AggFunctionDesc(functionName, null);
                }
                else {
                    // only support single argument, do not support function like max(x1,x2) with two arguments.
                    if (aggregation.getArguments().size() > 1) {
                        rewriteUtil.setCanRewrite(false);
                        return null;
                    }
                    TableColumnIdentify tableColumnIdent = applyExpr.apply(aggregation.getArguments().get(0));
                    aggFunctionDesc = new AggFunctionDesc(functionName, tableColumnIdent);
                }
                aggFunctionFromPlan.add(aggFunctionDesc);
            }
            List<AggFunctionDesc> aggFunctionFromAggIndex = rewriteUtil.getAggIndex().getAggFunctionDescs();
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
                        rewriteUtil.putColumnIdentifyAndSymbol(rewriteUtil.getSymbolToTableColumnName()
                                .getOrDefault(measure, TableColumnIdentify.NONE), measure);
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
        public Void visitProject(ProjectNode node, RewriteUtil context)
        {
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
        private boolean canRewrite;

        private final List<Expression> filterCollector;

        /**
         * For FilterNode, the key is the column in the original table of the symbol used by the filter expression.
         * For AggregationNode, key is the column in the original table of the aggregation node grouping key and the
         * arguments involved in the function.
         *
         * Because the optimized logical plan is basically a single stage which may only contains `Aggregation -> Filter -> TableScan`.
         * Note that AggregationNode and FilterNode input symbols are original from TableScan output symbols, only collect
         * symbols from AggregationNode and FilterNode.
         */
        private final Map<TableColumnIdentify, Symbol> columnIdentifyToSymbol;

        /**
         * The key is the output symbols of all TableScanNodes.
         * The value is the column field name of the original table.
         */
        private final Map<Symbol, TableColumnIdentify> symbolToTableColumnIdent;

        private AggIndex aggIndex;

        private Map<String, Symbol> nameToSymbol;

        public RewriteUtil(boolean canRewrite)
        {
            this.canRewrite = canRewrite;
            this.symbolToTableColumnIdent = new HashMap<>();
            this.columnIdentifyToSymbol = new HashMap<>();
            this.nameToSymbol = new HashMap<>();
            this.filterCollector = new ArrayList<>();
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
    }

    // TODO support avg, count, etc.
    private static boolean preCheck(AggregationNode node)
    {
        // grouping sets is not supported
        if (node.getGroupIdSymbol().isPresent() || node.getHashSymbol().isPresent()) {
            return false;
        }
        boolean allSymRef = true;
        Map<Symbol, AggregationNode.Aggregation> aggregations = node.getAggregations();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
            AggregationNode.Aggregation aggregation = entry.getValue();
            String name = aggregation.getResolvedFunction().getSignature().toSignature().getName();
            boolean allowFunc = Objects.equals("sum", name) || Objects.equals("min", name) || Objects.equals("max", name);
            if (aggregation.isDistinct()
                    || aggregation.getFilter().isPresent()
                    || aggregation.getOrderingScheme().isPresent()
                    || aggregation.getMask().isPresent()
                    || !allowFunc) {
                return false;
            }
            allSymRef = allSymRef && entry.getValue().getArguments().stream().allMatch(SymbolReference.class::isInstance);
        }
        return allSymRef;
    }

    private static void tuneAggregationNode(
            int aggIndexId,
            List<TableColumnIdentify> dimColumnFromPlan,
            List<TableColumnIdentify> dimColumnFromConnector,
            List<AggFunctionDesc> aggFunctionDescFromPlan,
            List<AggFunctionDesc> aggFunctionDescFromConnector)
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
}
