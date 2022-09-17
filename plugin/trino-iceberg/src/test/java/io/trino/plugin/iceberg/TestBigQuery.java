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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestBigQuery
        extends AbstractTestQueryFramework
{
    private File metastoreDir;

    private static final Map<String, String> PROPERTIES = ImmutableMap.of(
            "forbid_cross_join", "true",
            "order_by_full_table", "true");

    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("iceberg")
            .setSchema("tpch")
            .setSystemProperties(PROPERTIES).build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tempDir = Files.createTempDirectory("test_iceberg_table").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");
        DistributedQueryRunner icebergQueryRunner = createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("iceberg.query-partition-filter-required", "true"),
                Collections.emptyList(),
                Optional.of(metastoreDir));
        initTables(icebergQueryRunner);
        return icebergQueryRunner;
    }

    private void initTables(QueryRunner queryRunner)
    {
        queryRunner.execute("create table t1(f1 bigint, f2 varchar) with (partitioning = ARRAY['f2'])");
        queryRunner.execute("create table t2(f1 bigint, f2 bigint)");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.getParentFile().toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testFilterOnTable()
    {
        assertQuerySucceeds(TEST_SESSION, "select * from t2");
        assertQueryFails(TEST_SESSION, "select * from t1", "Filter required on tpch.t1 for at least one partition column, if you actually want query run without partition filter, please set session query_partition_filter_required to false.");
        assertQueryFails(TEST_SESSION, "select * from t1 where t1.f1 = 12", "Filter required on tpch.t1 for at least one partition column, if you actually want query run without partition filter, please set session query_partition_filter_required to false.");
        assertQuerySucceeds(TEST_SESSION, "select * from t1 where t1.f2 = '202201'");
        assertQuerySucceeds(TEST_SESSION, "select * from t1 left join t2 on t1.f1 = t2.f1 where t1.f2 = '20220101'");
        assertQueryFails(TEST_SESSION, "select * from t1 left join t2 on t1.f1 = t2.f1", "Filter required on tpch.t1 for at least one partition column, if you actually want query run without partition filter, please set session query_partition_filter_required to false.");
        assertQueryFails(TEST_SESSION, "with temp as (select * from t1 where f1 = 12 order by f1) select * from temp left join t2 on temp.f1 = t2.f2",
                "OrderBy without limit is not allowed, if you actually want query run order by without limit, please set session order_by_full_table to false.");
    }

    @Test
    public void testOrderByFullTable()
    {
        assertQuerySucceeds(TEST_SESSION, "show tables");
        assertQuerySucceeds(TEST_SESSION, "show schemas");
        assertQuerySucceeds(TEST_SESSION, "select * from t2 order by f1 limit 3");
        assertQuerySucceeds(TEST_SESSION, "select * from t2 order by f1 FETCH FIRST ROW WITH TIES");
        assertQueryFails(TEST_SESSION, "select * from t1 where f2='202201' order by f1", "OrderBy without limit is not allowed, if you actually want query run order by without limit, please set session order_by_full_table to false.");
        assertQueryFails(TEST_SESSION, "select * from t2 order by f1", "OrderBy without limit is not allowed, if you actually want query run order by without limit, please set session order_by_full_table to false.");
        assertQueryFails(TEST_SESSION,
                "with temp as (select * from t1 where f2='202201' order by f1) select * from temp left join t2 on temp.f1 = t2.f2",
                "OrderBy without limit is not allowed, if you actually want query run order by without limit, please set session order_by_full_table to false.");
        assertQueryFails(TEST_SESSION, "select * from t2 where f2 = 9 order by f1", "OrderBy without limit is not allowed, if you actually want query run order by without limit, please set session order_by_full_table to false.");
    }

    @Test
    public void testCrossJoin()
    {
        assertQueryFails(TEST_SESSION, "select * from t1 cross join t2", "Cross join is not allowed, if you actually want query run cross join, please set session forbid_cross_join to false.");
    }

    @Test
    public void testLeftJoinOptimizedToCrossJoin()
    {
        assertQuerySucceeds(TEST_SESSION, "select * from t1 left join t2 on t1.f1 = t2.f1 where t2.f1 = 10 and t1.f2 = '20220101'");
        assertQuerySucceeds(TEST_SESSION, "select * from t1 left join t2 on t1.f1 = t2.f1 where t1.f1 = 10 and t1.f2 = '20220101'");
        assertExplain(TEST_SESSION, "EXPLAIN select * from t1 left join t2 on t1.f1 = t2.f1 where t2.f1 = 10 and t1.f2 = '20220101'",
                "\\QCrossJoin");
        assertExplain(TEST_SESSION, "EXPLAIN select * from t1 left join t2 on t1.f1 = t2.f1 where t1.f1 = 10 and t1.f2 = '20220101'",
                "\\QLeftJoin");
    }
}
