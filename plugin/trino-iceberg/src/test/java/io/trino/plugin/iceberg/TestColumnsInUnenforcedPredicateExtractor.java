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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.Column;
import io.trino.execution.ColumnsInPredicate;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.ResultWithQueryId;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestColumnsInUnenforcedPredicateExtractor
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner();
    }

    @BeforeClass
    public void setUp()
    {
        assertQuerySucceeds("CREATE TABLE t1(id BIGINT, name VARCHAR, val DECIMAL, date VARCHAR) WITH (partitioning = ARRAY['date'])");
        assertQuerySucceeds("CREATE TABLE t2(id BIGINT, name VARCHAR, val DECIMAL)");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS t1");
        assertQuerySucceeds("DROP TABLE IF EXISTS t2");
    }

    @Test
    public void testSingleTableScan()
    {
        // equality
        assertResultEquals("SELECT * FROM t1 WHERE name = 'Tom'",
                createColumnsInPredicate("t1", ImmutableSet.of("name"), ImmutableSet.of()));
        assertResultEquals("SELECT * FROM t1 WHERE name IN ('Tom', 'Jack', 'Peter')",
                createColumnsInPredicate("t1", ImmutableSet.of("name"), ImmutableSet.of()));
        assertResultEquals("SELECT * FROM t1 WHERE id >= 1 AND id <= 1",
                createColumnsInPredicate("t1", ImmutableSet.of("id"), ImmutableSet.of()));
        assertResultEquals("SELECT * FROM t1 WHERE id >= 1 AND id <= 1 OR id = 3",
                createColumnsInPredicate("t1", ImmutableSet.of("id"), ImmutableSet.of()));

        // inequality
        assertResultEquals("SELECT * FROM t1 WHERE id > 0",
                createColumnsInPredicate("t1", ImmutableSet.of(), ImmutableSet.of("id")));
        assertResultEquals("SELECT * FROM t1 WHERE val <> 0",
                createColumnsInPredicate("t1", ImmutableSet.of(), ImmutableSet.of("val")));
        assertResultEquals("SELECT * FROM t1 WHERE val BETWEEN 0.5 and 3",
                createColumnsInPredicate("t1", ImmutableSet.of(), ImmutableSet.of("val")));

        // out of range
        assertEmptyResult("SELECT * FROM t1 WHERE val > 0 AND val < 0");

        // partition key
        assertEmptyResult("SELECT * FROM t1 WHERE date = '20220101'");

        // multiple columns
        assertResultEquals("SELECT * FROM t1 WHERE id > 100 AND name = 'Tom' AND val BETWEEN 80 and 90",
                createColumnsInPredicate("t1", ImmutableSet.of("name"), ImmutableSet.of("id", "val")));
        assertResultEquals("SELECT * FROM t1 WHERE id > 100 AND (name = 'Tom' OR val BETWEEN 80 and 90)",
                createColumnsInPredicate("t1", ImmutableSet.of(), ImmutableSet.of("id")));
        assertEmptyResult("SELECT * FROM t1 WHERE id + val < 100");
    }

    @Test
    public void testMultipleTableScan()
    {
        // single table
        Set<ColumnsInPredicate> expected = ImmutableSet.of(
                createColumnsInPredicate("t1", ImmutableSet.of(), ImmutableSet.of("id")),
                createColumnsInPredicate("t1", ImmutableSet.of("name"), ImmutableSet.of()));
        assertResultEquals("SELECT * FROM t1 as x, t1 as y WHERE x.id > 100 AND x.val > y.val AND y.name = 'Tom'", expected);
        assertResultEquals("SELECT * FROM t1 as x JOIN t1 as y ON x.id > 100 AND x.val > y.val AND y.name = 'Tom'", expected);

        // multiple tables
        assertResultEquals("SELECT * FROM t1 JOIN t2 ON t1.name = t2.name WHERE t1.val > 0 AND t2.id <= 200",
                ImmutableSet.of(
                        createColumnsInPredicate("t1", ImmutableSet.of(), ImmutableSet.of("val")),
                        createColumnsInPredicate("t2", ImmutableSet.of(), ImmutableSet.of("id"))));
    }

    private ColumnsInPredicate createColumnsInPredicate(
            String table,
            Set<String> colsInDiscretePredicate,
            Set<String> colsInRangePredicate)
    {
        TransactionManager transactionManager = getQueryRunner().getTransactionManager();
        TransactionId transactionId = transactionManager.beginTransaction(false);
        Session session = getSession().beginTransactionId(transactionId, transactionManager, getQueryRunner().getAccessControl());
        String catalog = session.getCatalog().orElseThrow();
        String schema = session.getSchema().orElseThrow();

        Metadata metadata = getQueryRunner().getMetadata();
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, new QualifiedObjectName(catalog, schema, table));
        assertTrue(tableHandle.isPresent(), String.format("table %s is not found", table));

        Map<String, String> columnType = metadata.getColumnHandles(session, tableHandle.get())
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> metadata.getColumnMetadata(session, tableHandle.get(), entry.getValue()).getType().toString()));

        return new ColumnsInPredicate(
                catalog,
                schema,
                table,
                colsInDiscretePredicate.stream()
                        .map(name -> new Column(name, columnType.get(name)))
                        .collect(Collectors.toSet()),
                colsInRangePredicate.stream()
                        .map(name -> new Column(name, columnType.get(name)))
                        .collect(Collectors.toSet()));
    }

    private void assertEmptyResult(@Language("SQL") String query)
    {
        assertResultEquals(query, ImmutableSet.of());
    }

    private void assertResultEquals(@Language("SQL") String query, ColumnsInPredicate columnsInPredicate)
    {
        assertResultEquals(query, ImmutableSet.of(columnsInPredicate));
    }

    private void assertResultEquals(@Language("SQL") String query, Set<ColumnsInPredicate> expected)
    {
        QueryInfo queryInfo = runAndGetQueryInfo(query);
        assertEquals(queryInfo.getState(), QueryState.FINISHED);
        assertEquals(queryInfo.getColumnsInUnenforcedPredicate(), expected);
    }

    private QueryInfo runAndGetQueryInfo(@Language("SQL") String query)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        ResultWithQueryId<MaterializedResult> resultWithQueryId = queryRunner.executeWithQueryId(getSession(), query);
        return queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(resultWithQueryId.getQueryId());
    }
}
