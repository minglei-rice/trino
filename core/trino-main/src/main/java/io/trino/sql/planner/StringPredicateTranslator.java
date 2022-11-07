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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.predicate.StringPredicate;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Set;

import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;

public class StringPredicateTranslator
{
    private StringPredicateTranslator() {}

    public static Set<StringPredicate> extractConjunctStringPredicates(Expression expression)
    {
        ImmutableSet.Builder<StringPredicate> result = ImmutableSet.builder();
        if (expression != null) {
            extractPredicates(AND, expression, result);
        }
        return result.build();
    }

    private static void extractPredicates(LogicalExpression.Operator operator,
                                          Expression expression,
                                          ImmutableSet.Builder<StringPredicate> result)
    {
        if (expression instanceof LogicalExpression && ((LogicalExpression) expression).getOperator() == operator) {
            LogicalExpression logicalExpression = (LogicalExpression) expression;
            for (Expression term : logicalExpression.getTerms()) {
                extractPredicates(operator, term, result);
            }
        }
        else {
            if (expression instanceof FunctionCall) {
                FunctionCall node = (FunctionCall) expression;
                String name = ResolvedFunction.extractFunctionName(node.getName());
                if (name.equals("starts_with")) {
                    extractFunctionCall(node, result, StringPredicate.Operator.STARTS_WITH);
                }
                else if (name.equals("has_token")) {
                    extractFunctionCall(node, result, StringPredicate.Operator.HAS_TOKEN);
                }
            }
            else if (expression instanceof ComparisonExpression) {
                ComparisonExpression comExpr = (ComparisonExpression) expression;
                if (comExpr.getOperator().equals(EQUAL)) {
                    Expression left = comExpr.getLeft();
                    Expression right = comExpr.getRight();

                    if (left instanceof SymbolReference && right instanceof StringLiteral) {
                        extractEqualOperator(result, (StringLiteral) right, left);
                    }
                    else if (right instanceof SymbolReference && left instanceof StringLiteral) {
                        extractEqualOperator(result, (StringLiteral) left, right);
                    }
                }
            }
            else if (expression instanceof LikePredicate) {
                LikePredicate likePredicate = (LikePredicate) expression;
                if (likePredicate.getEscape().isEmpty()) {
                    Expression value = likePredicate.getValue();
                    Expression pattern = likePredicate.getPattern();

                    if (value instanceof SymbolReference && pattern instanceof StringLiteral) {
                        Symbol symbol = Symbol.from(value);
                        Slice constantStr = ((StringLiteral) pattern).getSlice();
                        result.add(StringPredicate.of(symbol.getName(), constantStr.toStringUtf8(),
                                StringPredicate.Operator.LIKE));
                    }
                }
            }
        }
    }

    private static void extractEqualOperator(
            ImmutableSet.Builder<StringPredicate> result,
            StringLiteral valueLiteral,
            Expression colExpr)
    {
        Symbol symbol = Symbol.from(colExpr);
        Slice constantStr = valueLiteral.getSlice();
        result.add(StringPredicate.of(symbol.getName(), constantStr.toStringUtf8(),
                StringPredicate.Operator.EQUALS));
    }

    private static void extractFunctionCall(
            FunctionCall node,
            ImmutableSet.Builder<StringPredicate> result,
            StringPredicate.Operator operator)
    {
        List<Expression> args = node.getArguments();
        if (args.size() != 2) {
            return;
        }

        Expression target = args.get(0);
        if (!(target instanceof SymbolReference)) {
            // Target is not a symbol
            return;
        }

        Expression prefix = args.get(1);
        if (!(prefix instanceof StringLiteral)) {
            // dynamic pattern
            return;
        }

        Symbol symbol = Symbol.from(target);
        Slice constantPrefix = ((StringLiteral) prefix).getSlice();

        result.add(StringPredicate.of(symbol.getName(), constantPrefix.toStringUtf8(), operator));
    }
}
