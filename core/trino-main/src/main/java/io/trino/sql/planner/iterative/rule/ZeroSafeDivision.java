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

import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullIfExpression;

import java.math.BigDecimal;

import static io.trino.SystemSessionProperties.ZERO_SAFE_DIVISION;

/**
 * Rewrite division expressions by wrapping the divisor in a nullif expression, in order to avoid division by zero error.
 */
public class ZeroSafeDivision
        extends ExpressionRewriteRuleSet
{
    public ZeroSafeDivision()
    {
        super((expression, context) -> ExpressionTreeRewriter.rewriteWith(new Visitor(), expression, context));
    }

    private static class Visitor
            extends io.trino.sql.tree.ExpressionRewriter<Rule.Context>
    {
        @Override
        public Expression rewriteArithmeticBinary(ArithmeticBinaryExpression node, Rule.Context context, ExpressionTreeRewriter<Rule.Context> treeRewriter)
        {
            if (!context.getSession().getSystemProperty(ZERO_SAFE_DIVISION, Boolean.class) || node.getOperator() != ArithmeticBinaryExpression.Operator.DIVIDE) {
                return treeRewriter.defaultRewrite(node, context);
            }

            final Expression divisor = node.getRight();
            Expression stripped = stripCast(divisor);
            // divisor is a non-zero literal, no need to rewrite
            if (isLiteral(stripped) && !isZero(stripped)) {
                return treeRewriter.defaultRewrite(node, context);
            }

            // divisor is already a zero-safe nullif, no need to rewrite
            if (stripped instanceof NullIfExpression && isZero(((NullIfExpression) stripped).getSecond())) {
                return treeRewriter.defaultRewrite(node, context);
            }

            Expression newDivisor = new NullIfExpression(divisor, LongLiteral.ZERO);
            node = new ArithmeticBinaryExpression(node.getOperator(), node.getLeft(), newDivisor);
            return treeRewriter.defaultRewrite(node, context);
        }

        private Expression stripCast(Expression expression)
        {
            while (expression instanceof Cast) {
                expression = ((Cast) expression).getExpression();
            }
            return expression;
        }

        private boolean isLiteral(Expression expression)
        {
            return expression instanceof LongLiteral || expression instanceof DoubleLiteral || expression instanceof DecimalLiteral;
        }

        private boolean isZero(Expression expression)
        {
            if (expression instanceof LongLiteral) {
                return ((LongLiteral) expression).getValue() == 0;
            }
            if (expression instanceof DecimalLiteral) {
                return new BigDecimal(((DecimalLiteral) expression).getValue()).compareTo(BigDecimal.ZERO) == 0;
            }
            // real and double literals are considered never equal to 0
            return false;
        }
    }
}
