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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableLayoutResult;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableProperties.TablePartitioning;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.StringPredicate;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.LayoutConstraintEvaluator;
import io.trino.sql.planner.StringPredicateTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.IndexedExpression;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.TableLayoutResult.computeEnforced;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.sql.ExpressionUtils.SplitExpression;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.ExpressionUtils.extractIndexedConjuncts;
import static io.trino.sql.ExpressionUtils.filterConjuncts;
import static io.trino.sql.ExpressionUtils.transferToIndexedConjuncts;
import static io.trino.sql.planner.iterative.rule.Rules.deriveTableStatisticsForPushdown;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * These rules should not be run after AddExchanges so as not to overwrite the TableLayout
 * chosen by AddExchanges
 */
public class PushPredicateIntoTableScan
        implements Rule<FilterNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
            tableScan().capturedAs(TABLE_SCAN)));

    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public PushPredicateIntoTableScan(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        Optional<PlanNode> rewritten = pushFilterIntoTableScan(
                filterNode,
                tableScan,
                false,
                context.getSession(),
                context.getSymbolAllocator(),
                plannerContext,
                typeAnalyzer,
                context.getStatsProvider(),
                new DomainTranslator(plannerContext));

        if (rewritten.isEmpty() || arePlansSame(filterNode, tableScan, rewritten.get())) {
            return Result.empty();
        }

        return Result.ofPlanNode(rewritten.get());
    }

    private boolean arePlansSame(FilterNode filter, TableScanNode tableScan, PlanNode rewritten)
    {
        if (!(rewritten instanceof FilterNode)) {
            return false;
        }

        FilterNode rewrittenFilter = (FilterNode) rewritten;
        if (!Objects.equals(filter.getPredicate(), rewrittenFilter.getPredicate())) {
            return false;
        }

        if (!(rewrittenFilter.getSource() instanceof TableScanNode)) {
            return false;
        }

        TableScanNode rewrittenTableScan = (TableScanNode) rewrittenFilter.getSource();

        return Objects.equals(tableScan.getEnforcedConstraint(), rewrittenTableScan.getEnforcedConstraint()) &&
                Objects.equals(tableScan.getTable(), rewrittenTableScan.getTable());
    }

    public static Optional<PlanNode> pushFilterIntoTableScan(
            FilterNode filterNode,
            TableScanNode node,
            boolean pruneWithPredicateExpression,
            Session session,
            SymbolAllocator symbolAllocator,
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            StatsProvider statsProvider,
            DomainTranslator domainTranslator)
    {
        if (!isAllowPushdownIntoConnectors(session)) {
            return Optional.empty();
        }

        Expression predicate = filterNode.getPredicate();
        SplitExpression splitExpression = new SplitExpression(plannerContext.getMetadata(), transferToIndexedConjuncts(predicate));

        // don't include non-deterministic predicates
        Expression deterministicPredicate = combineConjuncts(plannerContext.getMetadata(), splitExpression.getDeterministicPredicate());

        // only the symbol reference or cast expressions could be extracted, others (except startsWith) will be the remaining.
        DomainTranslator.ExtractionResult decomposedOrderedPredicate = DomainTranslator.getExtractionResult(
                plannerContext,
                session,
                splitExpression.getOrderedDeterministicPredicate(),
                symbolAllocator.getTypes());

        Expression remainingExpression = combineExpressionsInOrder(plannerContext.getMetadata(), decomposedOrderedPredicate.getRemainingExpression());

        Set<StringPredicate> stringPredicates = StringPredicateTranslator
                .extractConjunctStringPredicates(deterministicPredicate);

        TupleDomain<ColumnHandle> newDomain = decomposedOrderedPredicate.getTupleDomain()
                .transformKeys(node.getAssignments()::get)
                .intersect(node.getEnforcedConstraint());

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

        Constraint constraint;
        // use evaluator only when there is some predicate which could not be translated into tuple domain
        if (pruneWithPredicateExpression && !TRUE_LITERAL.equals(remainingExpression)) {
            LayoutConstraintEvaluator evaluator = new LayoutConstraintEvaluator(
                    plannerContext,
                    typeAnalyzer,
                    session,
                    symbolAllocator.getTypes(),
                    node.getAssignments(),
                    combineConjuncts(
                            plannerContext.getMetadata(),
                            deterministicPredicate,
                            // Simplify the tuple domain to avoid creating an expression with too many nodes,
                            // which would be expensive to evaluate in the call to isCandidate below.
                            domainTranslator.toPredicate(session, newDomain.simplify().transformKeys(assignments::get))));
            constraint = new Constraint(newDomain, evaluator::isCandidate, evaluator.getArguments(), stringPredicates);
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint(newDomain, stringPredicates);
        }

        // we are interested only in functional predicate here, so we set the summary to ALL.
        Supplier<Constraint> constraintEvaluatorSupplier = () -> Optional.ofNullable(remainingExpression)
                .map(p -> filterConjuncts(plannerContext.getMetadata(), p, expression -> !DynamicFilters.isDynamicFilter(expression)))
                .map(p -> new LayoutConstraintEvaluator(plannerContext, typeAnalyzer, session, symbolAllocator.getTypes(), node.getAssignments(), p))
                .map(evaluator -> new Constraint(TupleDomain.all(), evaluator::isCandidate, evaluator.getArguments()))
                .orElse(alwaysTrue());
        // TODO: Verify the expression arguments more strictly instead using intersection.
        Predicate<Expression> hasAnyPartitionSymbolInRemainingExpression = expression -> {
            ImmutableSet.Builder<ColumnHandle> columnsInRemainingExpression = ImmutableSet.builder();
            SymbolsExtractor.extractUnique(expression).forEach(symbol -> {
                ColumnHandle col = node.getAssignments().get(symbol);
                if (col != null) {
                    columnsInRemainingExpression.add(col);
                }
            });
            Map<String, ColumnHandle> partitionColumns = plannerContext.getMetadata().getPartitionColumnHandles(session, node.getTable());
            Set<ColumnHandle> partitionColumnSet = partitionColumns.values().stream().collect(toImmutableSet());
            return columnsInRemainingExpression.build().stream().anyMatch(partitionColumnSet::contains);
        };
        Predicate<Expression> pushdownConstraintEvaluatorPredicate = expression ->
                plannerContext.getMetadata().supportsPruningPartitionsWithPredicateExpression(session, node.getTable()) && hasAnyPartitionSymbolInRemainingExpression.test(expression);
        boolean supportPruningStringPredicate = plannerContext.getMetadata().supportsPruningStringPredicate(session, node.getTable());

        TableHandle newTable;
        Optional<TablePartitioning> newTablePartitioning;
        TupleDomain<ColumnHandle> remainingFilter;
        boolean precalculateStatistics;
        if (!plannerContext.getMetadata().usesLegacyTableLayouts(session, node.getTable())) {
            // check if new domain is wider than domain already provided by table scan
            if (constraint.predicate().isEmpty() && newDomain.contains(node.getEnforcedConstraint()) &&
                    (!supportPruningStringPredicate || stringPredicates.isEmpty())) {
                Expression resultingPredicate = createResultingPredicateInOrder(
                        plannerContext,
                        session,
                        symbolAllocator,
                        typeAnalyzer,
                        TRUE_LITERAL,
                        splitExpression.getIndexedNonDeterministicPredicate(),
                        decomposedOrderedPredicate.getRemainingExpression());

                TableScanNode newNode = node;
                if (pushdownConstraintEvaluatorPredicate.test(remainingExpression) && !TRUE_LITERAL.equals(resultingPredicate)) {
                    // TODO: To avoid repeatedly construct new table scan node if the remaining expression is equal to the previous applied.
                    Optional<ConstraintApplicationResult<TableHandle>> evaluableTableHandle = plannerContext.getMetadata().applyFilter(session, node.getTable(), constraintEvaluatorSupplier.get());
                    if (evaluableTableHandle.isPresent()) {
                        newNode = new TableScanNode(
                                node.getId(),
                                evaluableTableHandle.get().getHandle(),
                                node.getOutputSymbols(),
                                node.getAssignments(),
                                node.getEnforcedConstraint(),
                                node.getStatistics(),
                                node.isUpdateTarget(),
                                node.getUseConnectorNodePartitioning());
                    }
                }

                if (!TRUE_LITERAL.equals(resultingPredicate)) {
                    return Optional.of(new FilterNode(filterNode.getId(), newNode, resultingPredicate));
                }

                return Optional.of(newNode);
            }

            if (newDomain.isNone()) {
                // TODO: DomainTranslator.fromPredicate can infer that the expression is "false" in some cases (TupleDomain.none()).
                // This should move to another rule that simplifies the filter using that logic and then rely on RemoveTrivialFilters
                // to turn the subtree into a Values node
                return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            if (pushdownConstraintEvaluatorPredicate.test(remainingExpression)) {
                constraint = new Constraint(constraint, constraintEvaluatorSupplier);
            }

            Optional<ConstraintApplicationResult<TableHandle>> result =
                    plannerContext.getMetadata().applyFilter(
                            session,
                            node.getTable(),
                            constraint);

            if (result.isEmpty()) {
                return Optional.empty();
            }

            newTable = result.get().getHandle();

            TableProperties newTableProperties = plannerContext.getMetadata().getTableProperties(session, newTable);
            newTablePartitioning = newTableProperties.getTablePartitioning();
            if (newTableProperties.getPredicate().isNone()) {
                return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            remainingFilter = result.get().getRemainingFilter();
            precalculateStatistics = result.get().isPrecalculateStatistics();
        }
        else {
            Optional<TableLayoutResult> layout = plannerContext.getMetadata().getLayout(
                    session,
                    node.getTable(),
                    constraint,
                    Optional.of(node.getOutputSymbols().stream()
                            .map(node.getAssignments()::get)
                            .collect(toImmutableSet())));

            if (layout.isEmpty() || layout.get().getTableProperties().getPredicate().isNone()) {
                return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            newTable = layout.get().getNewTableHandle();
            newTablePartitioning = layout.get().getTableProperties().getTablePartitioning();
            remainingFilter = layout.get().getUnenforcedConstraint();
            precalculateStatistics = false;
        }

        verifyTablePartitioning(session, plannerContext.getMetadata(), node, newTablePartitioning);

        TableScanNode tableScan = new TableScanNode(
                node.getId(),
                newTable,
                node.getOutputSymbols(),
                node.getAssignments(),
                computeEnforced(newDomain, remainingFilter),
                // TODO (https://github.com/trinodb/trino/issues/8144) distinguish between predicate pushed down and remaining
                deriveTableStatisticsForPushdown(statsProvider, session, precalculateStatistics, filterNode),
                node.isUpdateTarget(),
                node.getUseConnectorNodePartitioning());

        Expression resultingPredicate = createResultingPredicateInOrder(
                plannerContext,
                session,
                symbolAllocator,
                typeAnalyzer,
                domainTranslator.toPredicate(session, remainingFilter.transformKeys(assignments::get)),
                splitExpression.getIndexedNonDeterministicPredicate(),
                decomposedOrderedPredicate.getRemainingExpression());

        if (!TRUE_LITERAL.equals(resultingPredicate)) {
            return Optional.of(new FilterNode(filterNode.getId(), tableScan, resultingPredicate));
        }

        return Optional.of(tableScan);
    }

    // PushPredicateIntoTableScan might be executed after AddExchanges and DetermineTableScanNodePartitioning.
    // In that case, table scan node partitioning (if present) was used to fragment plan with ExchangeNodes.
    // Therefore table scan node partitioning should not change after AddExchanges is executed since it would
    // make plan with ExchangeNodes invalid.
    private static void verifyTablePartitioning(
            Session session,
            Metadata metadata,
            TableScanNode oldTableScan,
            Optional<TablePartitioning> newTablePartitioning)
    {
        if (oldTableScan.getUseConnectorNodePartitioning().isEmpty()) {
            return;
        }

        Optional<TablePartitioning> oldTablePartitioning = metadata.getTableProperties(session, oldTableScan.getTable()).getTablePartitioning();
        verify(newTablePartitioning.equals(oldTablePartitioning), "Partitioning must not change after predicate is pushed down");
    }

    static Expression combineExpressionsInOrder(Metadata metadata, Expression expression)
    {
        List<Expression> conjuncts = extractIndexedConjuncts(expression)
                .stream()
                .sorted(Comparator.comparingInt(IndexedExpression::getId))
                .map(IndexedExpression::getOriginExpression)
                .collect(toList());

        return combineConjuncts(metadata, conjuncts);
    }

    static Expression combineExpressionsInOrder(
            PlannerContext plannerContext,
            Expression unenforcedConstraints,
            List<IndexedExpression> nonDeterministicPredicate,
            Expression remainingDecomposedPredicate)
    {
        List<IndexedExpression> orderedRemainingExpressions = extractIndexedConjuncts(remainingDecomposedPredicate);
        List<Expression> extractedUnenforcedExpression = extractConjuncts(unenforcedConstraints);
        List<Expression> conjuncts = Stream.concat(
                extractedUnenforcedExpression.stream(),
                Stream.concat(nonDeterministicPredicate.stream(), orderedRemainingExpressions.stream())
                        .sorted(Comparator.comparingInt(IndexedExpression::getId))
                        .map(IndexedExpression::getOriginExpression))
                .collect(toImmutableList());

        return combineConjuncts(plannerContext.getMetadata(), conjuncts);
    }

    static Expression createResultingPredicateInOrder(
            PlannerContext plannerContext,
            Session session,
            SymbolAllocator symbolAllocator,
            TypeAnalyzer typeAnalyzer,
            Expression unenforcedConstraints,
            List<IndexedExpression> nonDeterministicPredicate,
            Expression remainingDecomposedPredicate)
    {
        Expression expression = combineExpressionsInOrder(plannerContext, unenforcedConstraints, nonDeterministicPredicate, remainingDecomposedPredicate);

        // Make sure we produce an expression whose terms are consistent with the canonical form used in other optimizations
        // Otherwise, we'll end up ping-ponging among rules
        expression = SimplifyExpressions.rewrite(expression, session, symbolAllocator, plannerContext, typeAnalyzer);

        return expression;
    }
}
