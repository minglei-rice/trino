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
package io.trino.tests;

import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCompleteExecuteQuery
        extends AbstractTestQueryFramework
{
    private static final String PREPARE_STATEMENT_NAME = "my_select";
    private static final String PREPARE_STATEMENT_PREFIX = "PREPARE " + PREPARE_STATEMENT_NAME + " FROM ";
    private static final String EXECUTE_STATEMENT_PREFIX = "EXECUTE " + PREPARE_STATEMENT_NAME + " USING ";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void testCompleteExecuteQuery()
    {
        assertCompleteExecuteQuery(
                "SELECT COUNT(*) FROM customer WHERE mktsegment = ?",
                EXECUTE_STATEMENT_PREFIX + "'HOUSEHOLD'",
                "SELECT COUNT(*) FROM customer WHERE mktsegment = 'HOUSEHOLD'");
        assertCompleteExecuteQuery(
                "SELECT COUNT(*) FROM orders WHERE custkey > ?",
                EXECUTE_STATEMENT_PREFIX + "2*1000/300",
                "SELECT COUNT(*) FROM orders WHERE custkey > 2*1000/300");
        assertCompleteExecuteQuery(
                "SELECT c.name, sum(o.totalprice) " +
                        "FROM (orders o INNER JOIN customer c ON o.custkey = c.custkey) " +
                        "WHERE nationkey = ? AND mktsegment = ? " +
                        "GROUP BY c.name " +
                        "LIMIT ?",
                EXECUTE_STATEMENT_PREFIX + "2, 'BUILDING', 5",
                "SELECT c.name, sum(o.totalprice) " +
                        "FROM (orders o INNER JOIN customer c ON o.custkey = c.custkey) " +
                        "WHERE nationkey = 2 AND mktsegment = 'BUILDING' " +
                        "GROUP BY c.name " +
                        "LIMIT 5");
    }

    private void assertCompleteExecuteQuery(
            @Language("SQL") String prepareQuery,
            @Language("SQL") String executeQuery,
            @Language("SQL") String expectedQuery)
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement(PREPARE_STATEMENT_NAME, prepareQuery(prepareQuery))
                .build();
        QueryInfo queryInfo = runAndGetFullQueryInfo(session, executeQuery);
        assertTrue(queryInfo.getCompleteExecuteQuery().isPresent(), "missing complete execute query");
        assertEquals(queryInfo.getCompleteExecuteQuery().get(), prepareQuery(expectedQuery));
    }

    private String prepareQuery(@Language("SQL") String query)
    {
        QueryInfo queryInfo = runAndGetFullQueryInfo(getSession(), PREPARE_STATEMENT_PREFIX + query);
        assertTrue(queryInfo.getAddedPreparedStatements().containsKey(PREPARE_STATEMENT_NAME), "fail to prepare query");
        return queryInfo.getAddedPreparedStatements().get(PREPARE_STATEMENT_NAME);
    }

    private QueryInfo runAndGetFullQueryInfo(Session session, @Language("SQL") String query)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        ResultWithQueryId<MaterializedResult> resultWithQueryId = queryRunner.executeWithQueryId(session, query);
        return queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(resultWithQueryId.getQueryId());
    }
}
