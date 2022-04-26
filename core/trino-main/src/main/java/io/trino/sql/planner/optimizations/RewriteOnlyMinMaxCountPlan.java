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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsNormalizer;
import io.trino.cost.StatsProvider;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.cost.TableScanStatsRule;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SimplePlanVisitor;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static java.lang.Double.isFinite;
import static java.lang.Math.round;

public class RewriteOnlyMinMaxCountPlan
        implements PlanOptimizer
{
    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public RewriteOnlyMinMaxCountPlan(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = plannerContext;
        this.typeAnalyzer = typeAnalyzer;
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        if (SystemSessionProperties.isQueryOptimizeWithMetadataEnabled(session)) {
            return plan.accept(new AggregationQueryRewrite(plannerContext, types, session, typeAnalyzer), true);
        }
        return plan;
    }

    private static class TableScanStatsExtractor
    {
        public static Map<PlanNodeId, PlanNodeStatsEstimate> extract(PlanNode node, Metadata metadata, Session session, TypeProvider types)
        {
            ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> estimates = ImmutableMap.builder();
            node.accept(new TableScanStatsExtractor.Visitor(metadata, new StatsNormalizer(), null, Lookup.noLookup(), session, types, estimates::put), null);
            return estimates.build();
        }

        private static class Visitor
                extends SimplePlanVisitor<Void>
        {
            private final Metadata metadata;
            private final StatsNormalizer statsNormalizer;
            private final StatsProvider statsProvider;
            private final Lookup lookup;
            private final Session session;
            private final TypeProvider typeProvider;

            private final BiConsumer<PlanNodeId, PlanNodeStatsEstimate> consumer;

            public Visitor(
                    Metadata metadata,
                    StatsNormalizer statsNormalizer,
                    StatsProvider statsProvider,
                    Lookup lookup, Session session,
                    TypeProvider typeProvider,
                    BiConsumer<PlanNodeId, PlanNodeStatsEstimate> consumer)
            {
                this.metadata = metadata;
                this.statsNormalizer = statsNormalizer;
                this.statsProvider = statsProvider;
                this.lookup = lookup;
                this.session = session;
                this.typeProvider = typeProvider;
                this.consumer = consumer;
            }

            @Override
            public Void visitTableScan(TableScanNode node, Void context)
            {
                this.consumer.accept(node.getId(), new TableScanStatsRule(metadata, statsNormalizer).calculate(node, statsProvider, lookup, session, typeProvider).get());
                return visitPlan(node, context);
            }
        }
    }

    /**
     * Inspired by ConstantExpressionVerifier.
     */
    private static class ConstantExpressionVisitor
            extends DefaultTraversalVisitor<Void>
    {
        private final Expression expression;

        private ConstantExpressionVisitor(Expression expression)
        {
            this.expression = expression;
        }

        @Override
        protected Void visitSymbolReference(SymbolReference node, Void context)
        {
            throw semanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
        }
    }

    /**
     * Conditional rewrite query with aggregation functions should satisfy these conditions at least:
     * 1. No GroupSets
     * 2. No GroupBy
     * 3. Only min(T)/max(T)/count(T)/count(*)
     * 4. No filters on non-partitioned columns
     * 5. No DISTINCT
     * 6. No HAVING
     * 7. No OFFSET
     * 8. No JOIN
     * 9. No more than 1 select iterm
     * 10. Aggregation arguments are column reference symbols or constant expressions
     * 11. Min/Max on numeric columns
     *
     * <p> * Note that the WITH query will have the same logical plan with SELECT. </p>
     *
     */
    private static class AggregationQueryRewrite
            extends PlanVisitor<PlanNode, Boolean>
    {
        public static final Function<String, AggregationQueryRewrite.FunctionName> toFunctionName = key -> AggregationQueryRewrite.FunctionName.valueOf(key.toUpperCase(Locale.ROOT));
        private static final Expression NULL_DOUBLE = new Cast(new NullLiteral(), toSqlType(DOUBLE));
        private static final Expression NULL_VARCHAR = new Cast(new NullLiteral(), toSqlType(VARCHAR));
        public static ImmutableMap<AggregationQueryRewrite.FunctionName, Function<Type, BiFunction<PlanNodeStatsEstimate, SymbolStatsEstimate, Expression>>> supportedAggFuncs =
                ImmutableMap.<AggregationQueryRewrite.FunctionName, Function<Type, BiFunction<PlanNodeStatsEstimate, SymbolStatsEstimate, Expression>>>builder()
                        .put(AggregationQueryRewrite.FunctionName.COUNT, type -> (tableEstimate, columnEstimate) ->
                                new Cast(toDoubleLiteral(tableEstimate.getOutputRowCount() - columnEstimate.getAccurateNullsCount()), toSqlType(BIGINT)))
                        .put(AggregationQueryRewrite.FunctionName.MIN, type -> (tableEstimate, columnEstimate) ->
                                new Cast(toStringLiteral(type, columnEstimate.getLowValue()), toSqlType(type)))
                        .put(AggregationQueryRewrite.FunctionName.MAX, type -> (tableEstimate, columnEstimate) ->
                                new Cast(toStringLiteral(type, columnEstimate.getHighValue()), toSqlType(type)))
                        .build();
        private final Map<Symbol, AggregationNode.Aggregation> symbol2aggregation = new HashMap<>();
        private final Map<AggregationQueryRewrite.FunctionName, Set<Symbol>> aggregation2columns = new HashMap<>();
        private final Map<Symbol, SymbolReference> symbolMappings = new HashMap<>();
        private final Map<Symbol, Expression> symbol2constantExpressions = new HashMap<>();
        private final Map<Symbol, Expression> symbol2complexExpressions = new HashMap<>();
        private final Set<Symbol> tableOutputColumns = new HashSet<>();

        private final PlannerContext plannerContext;
        private final TypeProvider runtimeTypes;
        private final Session session;
        private final TypeAnalyzer typeAnalyzer;
        private boolean isCandidate = true;

        public AggregationQueryRewrite(PlannerContext plannerContext, TypeProvider types, Session session, TypeAnalyzer typeAnalyzer)
        {
            this.plannerContext = plannerContext;
            this.runtimeTypes = types;
            this.session = session;
            this.typeAnalyzer = typeAnalyzer;
        }

        @Override
        public PlanNode visitOutput(OutputNode node, Boolean context)
        {
            visitChildren(node, context);

            PlanNode rewrittenNode = node;
            if (isCandidatePlan()) {
                Map<PlanNodeId, PlanNodeStatsEstimate> tableScanStats =
                        TableScanStatsExtractor.extract(node, plannerContext.getMetadata(), session, runtimeTypes);
                // TODO: Support more table scans, specially for UNION.
                if (tableScanStats.size() == 1 && verifyTableScanStats(tableScanStats)) {
                    rewrittenNode = rewritePlan(node, tableScanStats.values().stream().findFirst().get());
                }
            }

            clean();
            return rewrittenNode;
        }

        public TypeProvider getRuntimeTypes()
        {
            return runtimeTypes;
        }

        private void doIfCandidate(Runnable runnable)
        {
            if (isCandidate) {
                runnable.run();
            }
        }

        private boolean isCandidatePlan()
        {
            return isCandidate && verifyVisitedExpressions();
        }

        private void clean()
        {
            symbol2aggregation.clear();
            aggregation2columns.clear();
            symbolMappings.clear();
            symbol2constantExpressions.clear();
            symbol2complexExpressions.clear();
            tableOutputColumns.clear();
        }

        private static Expression toDoubleLiteral(double value)
        {
            if (!isFinite(value)) {
                return NULL_DOUBLE;
            }
            return new DoubleLiteral(Double.toString(value));
        }

        private static Expression toStringLiteral(Type type, double value)
        {
            if (!isFinite(value)) {
                return NULL_VARCHAR;
            }
            if (type == BOOLEAN) {
                String representation;
                if (value == 0) {
                    representation = "false";
                }
                else if (value == 1) {
                    representation = "true";
                }
                else {
                    representation = Double.toString(value);
                }
                return new StringLiteral(representation);
            }
            if (type.equals(BigintType.BIGINT) || type.equals(IntegerType.INTEGER) || type.equals(SmallintType.SMALLINT) || type.equals(TinyintType.TINYINT)) {
                return new StringLiteral(Long.toString(round(value)));
            }
            if (type.equals(DOUBLE) || type instanceof DecimalType) {
                return new StringLiteral(Double.toString(value));
            }
            if (type.equals(RealType.REAL)) {
                return new StringLiteral(Float.toString((float) value));
            }
            if (type.equals(DATE)) {
                return new StringLiteral(LocalDate.ofEpochDay(round(value)).toString());
            }
            throw new IllegalArgumentException("Unexpected type: " + type);
        }

        /**
         * The aggregation arguments should be the column reference or the constant expression,
         * and furthermore Min/Max should be evaluating on numeric columns and Count on arbitrary
         * columns, because the statistics only carries the DOUBLE ranges on numeric columns.
         *
         * Notice that the aggregation may be pushed down to the connector side, like Jdbc connector,
         * therefore the plan won't contain aggregation nodes, in which case the plan should be skipped.
         */
        private boolean verifyVisitedExpressions()
        {
            return !symbol2aggregation.isEmpty() && symbol2aggregation.values()
                    .stream()
                    .allMatch(aggregation -> aggregation.getArguments().stream().allMatch(argument -> {
                        if (!(argument instanceof SymbolReference)) {
                            throw new TrinoException(StandardErrorCode.QUERY_REJECTED, String.format("The argument %s should be a symbol reference!", argument.toString()));
                        }
                        Symbol resolvedSymbol = resolveSymbolRecursively(Symbol.from(argument));
                        boolean isColumnReference = tableOutputColumns.contains(resolvedSymbol);
                        boolean isComputingOnValidColumn = isComputableSymbol(resolvedSymbol) ||
                                AggregationQueryRewrite.FunctionName.COUNT == toFunctionName.apply(aggregation.getResolvedFunction().getSignature().getName());
                        return symbol2constantExpressions.containsKey(resolvedSymbol) || isColumnReference && isComputingOnValidColumn;
                    }));
        }

        private boolean isConstantExpression(Expression expression)
        {
            try {
                new ConstantExpressionVisitor(expression).process(expression, null);
            }
            catch (TrinoException ex) {
                if (ex.getErrorCode().equals(EXPRESSION_NOT_CONSTANT.toErrorCode())) {
                    return false;
                }
                throw ex;
            }
            return true;
        }

        /**
         * Trino remembers the values range of the timestamp column with millis precision, which
         * means it's not accurate to construct the results from the column statistics for Min/Max.
         *
         * TODO: Using a session property to enable evaluating timestamp column?
         */
        private boolean isComputableSymbol(Symbol symbol)
        {
            Type type = getRuntimeTypes().get(symbol);
            Class<?> javaType = type.getJavaType();
            return !(type instanceof TimestampType || type instanceof TimestampWithTimeZoneType) &&
                    (javaType == long.class || javaType == double.class || javaType == boolean.class);
        }

        /**
         * If a table estimate has infinite output row count or have NaN values on any column estimated,
         * the table statistics should not be trustworthy.
         */
        private boolean verifyTableScanStats(Map<PlanNodeId, PlanNodeStatsEstimate> estimates)
        {
            for (PlanNodeStatsEstimate estimate : estimates.values()) {
                if (!isFinite(estimate.getOutputRowCount())) {
                    return false;
                }

                for (Map.Entry<Symbol, SymbolStatsEstimate> entry : estimate.getSymbolStatistics().entrySet()) {
                    Symbol columnSymbol = entry.getKey();
                    SymbolStatsEstimate columnEstimate = entry.getValue();
                    if (aggregation2columns.getOrDefault(AggregationQueryRewrite.FunctionName.COUNT, new HashSet<>()).contains(columnSymbol) && !isFinite(columnEstimate.getAccurateNullsCount()) ||
                            aggregation2columns.getOrDefault(AggregationQueryRewrite.FunctionName.MIN, new HashSet<>()).contains(columnSymbol) && !isFinite(columnEstimate.getLowValue()) ||
                            aggregation2columns.getOrDefault(AggregationQueryRewrite.FunctionName.MAX, new HashSet<>()).contains(columnSymbol) && !isFinite(columnEstimate.getHighValue())) {
                        return false;
                    }
                }
            }
            return true;
        }

        private PlanNode rewritePlan(OutputNode plan, PlanNodeStatsEstimate planNodeStatsEstimate)
        {
            List<Symbol> outputSymbols = plan.getOutputSymbols();
            List<String> outputNames = plan.getColumnNames();
            ImmutableList.Builder<Expression> rowValuesBuilder = ImmutableList.builder();

            for (Symbol outputSymbol : outputSymbols) {
                Symbol resolvedOutputSymbol = resolveSymbolRecursively(outputSymbol);
                Expression computedExpression = computeExpression(resolvedOutputSymbol.toSymbolReference(), planNodeStatsEstimate);
                rowValuesBuilder.add(computedExpression);
            }

            PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            return new OutputNode(
                    idAllocator.getNextId(),
                    new ValuesNode(
                            idAllocator.getNextId(),
                            outputSymbols,
                            1,
                            Optional.of(ImmutableList.of(new Row(rowValuesBuilder.build())))),
                    outputNames,
                    outputSymbols);
        }

        private Symbol resolveSymbolRecursively(Symbol symbol)
        {
            if (!symbolMappings.containsKey(symbol) || symbolMappings.get(symbol).equals(symbol.toSymbolReference())) {
                return symbol;
            }
            return resolveSymbolRecursively(Symbol.from(symbolMappings.get(symbol)));
        }

        private Expression computeExpression(Expression expr, PlanNodeStatsEstimate planNodeStatsEstimate)
        {
            if (expr instanceof SymbolReference) {
                Symbol resolvedSymbol = resolveSymbolRecursively(Symbol.from(expr));
                if (symbol2aggregation.containsKey(resolvedSymbol)) {
                    return computeAggregationExpression(symbol2aggregation.get(resolvedSymbol), planNodeStatsEstimate);
                }
                else if (symbol2constantExpressions.containsKey(resolvedSymbol)) {
                    return computeExpression(symbol2constantExpressions.get(resolvedSymbol), planNodeStatsEstimate);
                }
                else if (symbol2complexExpressions.containsKey(resolvedSymbol)) {
                    return computeExpression(symbol2complexExpressions.get(resolvedSymbol), planNodeStatsEstimate);
                }
                else {
                    throw new RuntimeException(String.format("Could not determine the symbol %s reference from the analyzed expressions!", expr.toString()));
                }
            }
            if (expr instanceof ArithmeticBinaryExpression) {
                ArithmeticBinaryExpression abe = (ArithmeticBinaryExpression) expr;
                return new ArithmeticBinaryExpression(
                        abe.getOperator(),
                        computeExpression(abe.getLeft(), planNodeStatsEstimate),
                        computeExpression(abe.getRight(), planNodeStatsEstimate));
            }
            else {
                return expr;
            }
        }

        /**
         * Transform the estimated result of the input aggregation to a expression according to agg func return type.
         *
         * Note that there are two basic implicit rules for MIN/MAX/COUNT on a column:
         *   1. If the computed value of the constant expression is NULL, count(expr) will return 0,
         *   while min/max returns NULL.
         *   2. If the computed value of the constant expression is not NULL, count(expr) is equal to
         *   count(*), while min/max returns the computed value.
         *
         * TODO: To support arithmetic expressions and UDFs as the argument, as possible?
         */
        private Expression computeAggregationExpression(AggregationNode.Aggregation aggregation, PlanNodeStatsEstimate planNodeStatsEstimate)
        {
            String resolvedFuncName = aggregation.getResolvedFunction().getSignature().getName();
            Type returnType = aggregation.getResolvedFunction().getSignature().getReturnType();
            // count(*) has 0 arguments and represents the cardinality of the input dataset
            if (AggregationQueryRewrite.FunctionName.COUNT == toFunctionName.apply(resolvedFuncName) && aggregation.getArguments().isEmpty()) {
                return new Cast(toDoubleLiteral(planNodeStatsEstimate.getOutputRowCount()), toSqlType(BIGINT));
            }

            Optional<Expression> argument = aggregation.getArguments().stream().findFirst();
            if (argument.isEmpty()) {
                throw new RuntimeException(String.format(
                        "Expecting the aggregation %s has at least 1 argument!",
                        aggregation.getResolvedFunction().toString()));
            }

            Symbol resolvedArgumentSymbol = resolveSymbolRecursively(Symbol.from(argument.get()));

            if (symbol2constantExpressions.containsKey(resolvedArgumentSymbol)) {
                Expression constantExpression = symbol2constantExpressions.get(resolvedArgumentSymbol);
                switch (toFunctionName.apply(resolvedFuncName)) {
                    case COUNT:
                        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, getRuntimeTypes(), constantExpression);
                        ExpressionInterpreter expressionInterpreter = new ExpressionInterpreter(constantExpression, plannerContext, session, expressionTypes);
                        Object value = expressionInterpreter.evaluate();
                        return new Cast(toDoubleLiteral(value == null ? 0 : planNodeStatsEstimate.getOutputRowCount()), toSqlType(BIGINT));
                    case MIN:
                    case MAX:
                        return constantExpression;
                }
            }

            SymbolStatsEstimate symbolStatsEstimate = planNodeStatsEstimate.getSymbolStatistics(Symbol.from(argument.get()));
            return supportedAggFuncs.get(toFunctionName.apply(resolvedFuncName))
                    .apply(returnType)
                    .apply(planNodeStatsEstimate, symbolStatsEstimate);
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Boolean context)
        {
            if (!context) {
                isCandidate = false;
                return node;
            }

            for (PlanNode subNode : node.getSources()) {
                if (!isCandidate) {
                    break;
                }
                subNode.accept(this, true);
            }
            return node;
        }

        @Override
        public PlanNode visitOffset(OffsetNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitTopN(TopNNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitSample(SampleNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitSpatialJoin(SpatialJoinNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitWindow(WindowNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitExcept(ExceptNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitRowNumber(RowNumberNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitTopNRanking(TopNRankingNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitEnforceSingleRow(EnforceSingleRowNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitApply(ApplyNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitAssignUniqueId(AssignUniqueId node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitGroupReference(GroupReference node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitCorrelatedJoin(CorrelatedJoinNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        /**
         * If there were filters on unpartitioned keys, one or more Filter nodes should be existed,
         * in which case the plan should not be rewritten. Otherwise, the filters on partition keys
         * will be pushed down to TableScanNode, in which case the plan should be rewritten.
         */
        @Override
        public PlanNode visitFilter(FilterNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        @Override
        public PlanNode visitValues(ValuesNode node, Boolean context)
        {
            return visitPlan(node, false);
        }

        /**
         * If there were aggregations with constant expressions as the arguments, these arguments
         * will be optimized/evaluated and then be mapped as serials of symbols mappings, for an
         * instance of min(split_part('test', ',', 6)), it will be analyzed as the following two
         * mappings at least:
         *    min[Symbol] -> min(split_part)[Aggregation]
         *    split_part[Symbol] -> Cast(null as varchar(4))[Expression]
         *
         * For an instance of min_by(cast(1.0 as DOUBLE), 2):
         *    min_by -> min_by(expr_1, expr_2)
         *    expr_1 -> 1.0
         *    expr_2 -> 2
         *
         * Note that all the arguments mappings will be placed on the ProjectNode  prior to the
         * TableScanNode.
         */
        @Override
        public PlanNode visitProject(ProjectNode node, Boolean context)
        {
            visitChildren(node, context);

            doIfCandidate(() -> {
                for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                    Symbol symbol = entry.getKey();
                    Expression expr = entry.getValue();
                    if (expr instanceof SymbolReference) {
                        // the symbol mapping in the top level node may have the same key and value
                        symbolMappings.putIfAbsent(symbol, (SymbolReference) expr);
                    }
                    else if (isConstantExpression(expr)) {
                        symbol2constantExpressions.put(symbol, expr);
                    }
                    else if (expr instanceof ArithmeticBinaryExpression) {
                        symbol2complexExpressions.put(symbol, expr);
                    }
                    else {
                        visitPlan(node, false);
                    }
                }
            });

            return node;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Boolean context)
        {
            if (!node.hasEmptyGroupingSet() || node.hasNonEmptyGroupingSet() || containsUnsupportedAggFunc(node)) {
                return visitPlan(node, false);
            }

            if (AggregationNode.Step.PARTIAL == node.getStep()) {
                // PARTIAL node has the pairs of column symbol to aggregation
                symbol2aggregation.putAll(node.getAggregations());

                for (AggregationNode.Aggregation aggregation : node.getAggregations().values()) {
                    aggregation.getArguments().forEach(expr -> {
                        String resolvedFuncName = aggregation.getResolvedFunction().getSignature().getName();
                        AggregationQueryRewrite.FunctionName functionName = toFunctionName.apply(resolvedFuncName);
                        if (!aggregation2columns.containsKey(functionName)) {
                            aggregation2columns.put(functionName, new HashSet<>());
                        }
                        aggregation2columns.get(functionName).add(Symbol.from(expr));
                    });
                }
            }
            else if (AggregationNode.Step.FINAL == node.getStep()) {
                // FINAL node has the pairs of column reference to aggregation
                node.getAggregations().forEach((from, agg) -> symbolMappings.put(from, (SymbolReference) agg.getArguments().get(0)));
            }

            visitChildren(node, context);

            doIfCandidate(() -> {
                // the aggregation arguments should be symbol references to columns, constant expressions,
                // min/max/count or empty for count
                BiFunction<AggregationNode.Aggregation, AggregationNode.Step, Boolean> verifyAggregation = (aggregation, step) -> {
                    if ((!aggregation.isDistinct()) && aggregation.getFilter().isEmpty() && aggregation.getArguments().size() <= 1) {
                        return aggregation.getArguments().stream().allMatch(expr -> {
                            if (!(expr instanceof SymbolReference)) {
                                return false;
                            }
                            Symbol symbol = Symbol.from(expr);
                            if (symbol2constantExpressions.containsKey(symbol)) {
                                return true;
                            }
                            if (AggregationNode.Step.PARTIAL == step) {
                                return tableOutputColumns.contains(symbol);
                            }
                            else {
                                return symbol2aggregation.containsKey(symbol);
                            }
                        });
                    }
                    return false;
                };

                for (AggregationNode.Aggregation aggregation : node.getAggregations().values()) {
                    if (!verifyAggregation.apply(aggregation, node.getStep())) {
                        visitPlan(node, false);
                        break;
                    }
                }
            });

            return node;
        }

        private boolean containsUnsupportedAggFunc(AggregationNode node)
        {
            return node.getAggregations()
                    .values()
                    .stream()
                    .anyMatch(agg -> !AggregationQueryRewrite.FunctionName.hasName(agg.getResolvedFunction().getSignature().getName()));
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Boolean context)
        {
            boolean isCandidateConnector = plannerContext.getMetadata()
                    .getConnectorCapabilities(session, node.getTable().getCatalogName())
                    .contains(ConnectorCapabilities.METRICS_ACCURATE);
            if (isCandidateConnector) {
                visitChildren(node, context);
                doIfCandidate(() -> tableOutputColumns.addAll(node.getAssignments().keySet()));
                return node;
            }
            else {
                return visitPlan(node, false);
            }
        }

        private PlanNode visitChildren(PlanNode node, Boolean context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }
            return node;
        }

        enum FunctionName
        {
            COUNT,
            MIN,
            MAX;

            public static boolean hasName(String funcName)
            {
                return Arrays.stream(values()).map(AggregationQueryRewrite.FunctionName::name).anyMatch(name -> name.equals(funcName == null ? "" : funcName.toUpperCase(Locale.ROOT)));
            }
        }
    }
}
