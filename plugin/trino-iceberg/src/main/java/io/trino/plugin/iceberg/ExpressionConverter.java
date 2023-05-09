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
package io.trino.plugin.iceberg;

import com.google.common.base.VerifyException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergTypes.convertTrinoValueToIceberg;
import static java.lang.String.format;
import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.arrayContains;
import static org.apache.iceberg.expressions.Expressions.elementAt;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.hasTerm;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.jsonExtractScalar;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.like;
import static org.apache.iceberg.expressions.Expressions.mapKeys;
import static org.apache.iceberg.expressions.Expressions.mapValues;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.startsWith;

public final class ExpressionConverter
{
    private ExpressionConverter() {}

    public static Expression toIcebergExpression(TupleDomain<IcebergColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return alwaysTrue();
        }
        if (tupleDomain.getDomains().isEmpty()) {
            return alwaysFalse();
        }
        Map<IcebergColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        List<Expression> conjuncts = new ArrayList<>();
        for (Map.Entry<IcebergColumnHandle, Domain> entry : domainMap.entrySet()) {
            IcebergColumnHandle columnHandle = entry.getKey();
            checkArgument(!isMetadataColumnId(columnHandle.getId()), "Constraint on an unexpected column %s", columnHandle);
            Domain domain = entry.getValue();
            conjuncts.add(toIcebergExpression(columnHandle.getQualifiedName(), columnHandle.getType(), domain));
        }
        return and(conjuncts);
    }

    public static Expression toIcebergExpression(ConnectorExpression connectorExpression, Map<String, IcebergColumnHandle> assignments)
    {
        if (connectorExpression == null) {
            return alwaysTrue();
        }
        if (connectorExpression instanceof Call) {
            Call call = (Call) connectorExpression;
            switch (call.getFunctionName().getName()) {
                case "$and":
                    List<Expression> conjuncts = call.getArguments().stream().map(expr -> toIcebergExpression(expr, assignments)).collect(Collectors.toList());
                    return and(conjuncts);
                case "$or":
                    List<Expression> disConjuncts = call.getArguments().stream().map(expr -> toIcebergExpression(expr, assignments)).collect(Collectors.toList());
                    return or(disConjuncts);
                case "starts_with":
                    if (call.getArguments().size() != 2 || !(call.getArguments().get(1) instanceof Constant)) {
                        return alwaysTrue();
                    }
                    UnboundTerm startsWithTerm = toIcebergTerm(call.getArguments().get(0), assignments);
                    if (startsWithTerm == null) {
                        return alwaysTrue();
                    }
                    Constant startConstant = (Constant) call.getArguments().get(1);
                    return startsWith(startsWithTerm, (String) convertTrinoValueToIceberg(startConstant.getType(), startConstant.getValue()));
                case "has_token":
                    if (call.getArguments().size() != 2 || !(call.getArguments().get(1) instanceof Constant)) {
                        return alwaysTrue();
                    }
                    UnboundTerm hasTokenTerm = toIcebergTerm(call.getArguments().get(0), assignments);
                    if (hasTokenTerm == null) {
                        return alwaysTrue();
                    }
                    Constant tokenConstant = (Constant) call.getArguments().get(1);
                    return hasTerm(hasTokenTerm, (String) convertTrinoValueToIceberg(tokenConstant.getType(), tokenConstant.getValue()));
                case "$like":
                    if (call.getArguments().size() != 2 || !(call.getArguments().get(1) instanceof Constant)) {
                        return alwaysTrue();
                    }
                    UnboundTerm likeTerm = toIcebergTerm(call.getArguments().get(0), assignments);
                    if (likeTerm == null) {
                        return alwaysTrue();
                    }
                    Constant likeConstant = (Constant) call.getArguments().get(1);
                    return like(likeTerm, (String) convertTrinoValueToIceberg(likeConstant.getType(), likeConstant.getValue()));
                case "contains":
                    if (call.getArguments().size() != 2 || !(call.getArguments().get(1) instanceof Constant)) {
                        return alwaysTrue();
                    }
                    ConnectorExpression firstArg = call.getArguments().get(0);
                    Constant containsConstant = (Constant) call.getArguments().get(1);
                    UnboundTerm containsTerm = toIcebergTerm(firstArg, assignments);
                    if (containsTerm == null) {
                        return alwaysTrue();
                    }
                    return arrayContains(containsTerm, convertTrinoValueToIceberg(containsConstant.getType(), containsConstant.getValue()));
                case "$equal":
                    if (call.getArguments().size() != 2 || !(call.getArguments().get(0) instanceof Call) || !(call.getArguments().get(1) instanceof Constant)) {
                        return alwaysTrue();
                    }
                    Call function = (Call) call.getArguments().get(0);
                    UnboundTerm icebergTerm = toIcebergTerm(function, assignments);
                    if (icebergTerm == null) {
                        return alwaysTrue();
                    }
                    Constant elementsConstant = (Constant) call.getArguments().get(1);
                    return equal(icebergTerm, convertTrinoValueToIceberg(elementsConstant.getType(), elementsConstant.getValue()));
                default:
                    return alwaysTrue();
            }
        }
        return alwaysTrue();
    }

    private static UnboundTerm toIcebergTerm(ConnectorExpression expr, Map<String, IcebergColumnHandle> assignments)
    {
        if (expr instanceof Variable) {
            Variable containsColumn = (Variable) expr;
            return Expressions.ref(resolve(containsColumn, assignments));
        }
        else if (expr instanceof Call) {
            Call call = (Call) expr;
            switch (call.getFunctionName().getName()) {
                case "element_at":
                    if (call.getArguments().size() != 2 || !(call.getArguments().get(0) instanceof Variable) || !(call.getArguments().get(1) instanceof Constant)) {
                        return null;
                    }
                    Variable elementsColumn = (Variable) call.getArguments().get(0);
                    Constant elementsConstant = (Constant) call.getArguments().get(1);
                    return elementAt(resolve(elementsColumn, assignments), convertTrinoValueToIceberg(elementsConstant.getType(), elementsConstant.getValue()).toString());
                case "map_keys":
                    if (call.getArguments().size() != 1 || !(call.getArguments().get(0) instanceof Variable)) {
                        return null;
                    }
                    Variable mapColumn = (Variable) call.getArguments().get(0);
                    return mapKeys(resolve(mapColumn, assignments));
                case "map_values":
                    if (call.getArguments().size() != 1 || !(call.getArguments().get(0) instanceof Variable)) {
                        return null;
                    }
                    Variable mapValuesColumn = (Variable) call.getArguments().get(0);
                    return mapValues(resolve(mapValuesColumn, assignments));
                case "json_extract_scalar":
                    if (call.getArguments().size() != 2 || !(call.getArguments().get(0) instanceof Variable) || !(call.getArguments().get(1) instanceof Constant)) {
                        return null;
                    }
                    Variable jsonColumn = (Variable) call.getArguments().get(0);
                    Constant jsonConstant = (Constant) call.getArguments().get(1);
                    return jsonExtractScalar(resolve(jsonColumn, assignments), jsonConstant.getValue().toString());
                default:
                    return null;
            }
        }
        else {
            return null;
        }
    }

    private static String resolve(Variable variable, Map<String, IcebergColumnHandle> assignments)
    {
        ColumnHandle columnHandle = assignments.get(variable.getName());
        checkArgument(columnHandle != null, "No assignment for %s", variable);
        return ((IcebergColumnHandle) columnHandle).getName();
    }

    private static Expression toIcebergExpression(String columnName, Type type, Domain domain)
    {
        if (domain.isAll()) {
            return alwaysTrue();
        }
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? alwaysTrue() : not(isNull(columnName));
        }

        // Skip structural types. TODO (https://github.com/trinodb/trino/issues/8759) Evaluate Apache Iceberg's support for predicate on structural types
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            // Fail fast. Ignoring expression could lead to data loss in case of deletions.
            throw new UnsupportedOperationException("Unsupported type for expression: " + type);
        }

        if (type.isOrderable()) {
            List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
            List<Object> icebergValues = new ArrayList<>();
            List<Expression> rangeExpressions = new ArrayList<>();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    icebergValues.add(convertTrinoValueToIceberg(type, range.getLowBoundedValue()));
                }
                else {
                    rangeExpressions.add(toIcebergExpression(columnName, range));
                }
            }
            Expression ranges = or(rangeExpressions);
            Expression values = icebergValues.isEmpty() ? alwaysFalse() : in(columnName, icebergValues);
            Expression nullExpression = domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();
            return or(nullExpression, or(values, ranges));
        }

        throw new VerifyException(format("Unsupported type %s with domain values %s", type, domain));
    }

    private static Expression toIcebergExpression(String columnName, Range range)
    {
        Type type = range.getType();

        if (range.isSingleValue()) {
            Object icebergValue = convertTrinoValueToIceberg(type, range.getSingleValue());
            return equal(columnName, icebergValue);
        }

        List<Expression> conjuncts = new ArrayList<>(2);
        if (!range.isLowUnbounded()) {
            Object icebergLow = convertTrinoValueToIceberg(type, range.getLowBoundedValue());
            Expression lowBound;
            if (range.isLowInclusive()) {
                lowBound = greaterThanOrEqual(columnName, icebergLow);
            }
            else {
                lowBound = greaterThan(columnName, icebergLow);
            }
            conjuncts.add(lowBound);
        }

        if (!range.isHighUnbounded()) {
            Object icebergHigh = convertTrinoValueToIceberg(type, range.getHighBoundedValue());
            Expression highBound;
            if (range.isHighInclusive()) {
                highBound = lessThanOrEqual(columnName, icebergHigh);
            }
            else {
                highBound = lessThan(columnName, icebergHigh);
            }
            conjuncts.add(highBound);
        }

        return and(conjuncts);
    }

    private static Expression and(List<Expression> expressions)
    {
        if (expressions.isEmpty()) {
            return alwaysTrue();
        }
        return combine(expressions, Expressions::and);
    }

    private static Expression or(Expression left, Expression right)
    {
        return Expressions.or(left, right);
    }

    private static Expression or(List<Expression> expressions)
    {
        if (expressions.isEmpty()) {
            return alwaysFalse();
        }
        return combine(expressions, Expressions::or);
    }

    private static Expression combine(List<Expression> expressions, BiFunction<Expression, Expression, Expression> combiner)
    {
        // Build balanced tree that preserves the evaluation order of the input expressions.
        //
        // The tree is built bottom up by combining pairs of elements into binary expressions.
        //
        // Example:
        //
        // Initial state:
        //  a b c d e
        //
        // First iteration:
        //
        //  /\    /\   e
        // a  b  c  d
        //
        // Second iteration:
        //
        //    / \    e
        //  /\   /\
        // a  b c  d
        //
        //
        // Last iteration:
        //
        //      / \
        //    / \  e
        //  /\   /\
        // a  b c  d

        Queue<Expression> queue = new ArrayDeque<>(expressions);
        while (queue.size() > 1) {
            Queue<Expression> buffer = new ArrayDeque<>();

            // combine pairs of elements
            while (queue.size() >= 2) {
                buffer.add(combiner.apply(queue.remove(), queue.remove()));
            }

            // if there's and odd number of elements, just append the last one
            if (!queue.isEmpty()) {
                buffer.add(queue.remove());
            }

            // continue processing the pairs that were just built
            queue = buffer;
        }

        return queue.remove();
    }
}
