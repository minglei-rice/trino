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

import io.trino.Session;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.SymbolReference;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.ZERO_SAFE_DIVISION;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE;

public class TestZeroSafeDivision
        extends BaseRuleTest
{
    private Session session;

    @BeforeClass
    public void enableSessionProperty()
    {
        session = Session.builder(tester().getSession()).setSystemProperty(ZERO_SAFE_DIVISION, "true").build();
    }

    @Test
    public void testAlreadyZeroSafe()
            throws Exception
    {
        ZeroSafeDivision zeroSafeDivision = new ZeroSafeDivision();
        tester().assertThat(zeroSafeDivision.projectExpressionRewrite())
                .withSession(session)
                .on(p -> p.project(
                        Assignments.of(
                                p.symbol("expr"),
                                new ArithmeticBinaryExpression(DIVIDE, new SymbolReference("x"), new NullIfExpression(new SymbolReference("y"), new LongLiteral("0")))),
                        p.values(p.symbol("x"), p.symbol("y"))))
                .doesNotFire();
    }

    @Test
    public void testNonZeroLiteralDivisor()
            throws Exception
    {
        ZeroSafeDivision zeroSafeDivision = new ZeroSafeDivision();
        tester().assertThat(zeroSafeDivision.projectExpressionRewrite())
                .withSession(session)
                .on(p -> p.project(
                        Assignments.of(p.symbol("expr"), new ArithmeticBinaryExpression(DIVIDE, new SymbolReference("x"), new LongLiteral("2"))),
                        p.values(p.symbol("x"))))
                .doesNotFire();
    }

    @Test
    public void testDoubleDivisor()
            throws Exception
    {
        ZeroSafeDivision zeroSafeDivision = new ZeroSafeDivision();
        tester().assertThat(zeroSafeDivision.projectExpressionRewrite())
                .withSession(session)
                .on(p -> p.project(
                        Assignments.of(p.symbol("expr"), new ArithmeticBinaryExpression(DIVIDE, new SymbolReference("x"), new DoubleLiteral("0"))),
                        p.values(p.symbol("x"))))
                .doesNotFire();
    }
}
