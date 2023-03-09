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
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.SystemSessionProperties.ZERO_SAFE_DIVISION;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestIcebergZeroSafeDivision
        extends AbstractTestQueryFramework
{
    private File metastoreDir;
    private HiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;

    @BeforeClass
    public void enableSystemProperty()
    {
        getQueryRunner().getSessionPropertyManager().addRuntimeSystemSessionProperty(ZERO_SAFE_DIVISION, "true");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.getParentFile().toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        this.hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        File tempDir = Files.createTempDirectory("test_iceberg_split_source").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");
        this.metastore = createTestingFileHiveMetastore(metastoreDir);

        return IcebergQueryRunner.builder()
                .setMetastoreDirectory(metastoreDir)
                .build();
    }

    @Test
    public void testRealAndDouble()
    {
        // trino returns infinity for division by real or double 0
        assertEquals(getQueryRunner().execute("select 1 / double '0'").getMaterializedRows().toString(), "[[Infinity]]");
        assertEquals(getQueryRunner().execute("select 1 / real '0'").getMaterializedRows().toString(), "[[Infinity]]");
        try {
            getQueryRunner().getSessionPropertyManager().removeRuntimeSystemSessionProperty(ZERO_SAFE_DIVISION);
            getQueryRunner().execute("select 1 / double '0'");
            getQueryRunner().execute("select 1/ real '0.00'");
        }
        finally {
            getQueryRunner().getSessionPropertyManager().addRuntimeSystemSessionProperty(ZERO_SAFE_DIVISION, "true");
        }
    }

    @Test
    public void testRewrite()
            throws Exception
    {
        assertNullResult("select 1/0");
        assertNullResult("select 1/0.0");
        assertNullResult("select 1/cast(double '0' as decimal (10,5))");
        assertNullResult("select 1/cast('0' as decimal (10,5))");
        String foo = "foo" + randomTableSuffix();
        getQueryRunner().execute(format("create table %s (i, l, dec, d, s) as select 0, bigint '1', decimal '1.000', double '0', '0'", foo));
        assertNullResult(format("select 1 / cast(d as decimal(10,5)) from %s", foo));
        assertNullResult(format("select 1 / cast(s as decimal(10,5)) from %s", foo));
        assertNullResult(format("select 1 / (2*i) from %s", foo));
        assertNullResult(format("select dec / cast(0.0 as decimal(10,5)) from %s", foo));
        assertNullResult(format("select 1 / nullif(l-1, 1) from %s", foo));
        assertNullResult(format("select (1 / i) / (2 / dec - 2) from %s", foo));
        assertResult(format("select i from %s where 1 / abs(i) > 0.3", foo), "[]");
        assertResult(format("select max(l) from %s group by dec/i", foo), "[[1]]");
        assertResult(format("select max(l) from %s group by i having 2/(max(l)-1)>0.6", foo), "[]");
        assertResult(format("select x, y, x * 1.0 / y as res from (select sum(l) as x, sum(i) as y from %s) t", foo), "[[1, 0, null]]");

        String bar = "bar" + randomTableSuffix();
        getQueryRunner().execute(format("create table %s (i, l, dec) as select 1, bigint '2', decimal '2.0'", bar));
        assertResult(format("select * from %s join %s on %s.dec / %s.i = %s.dec", foo, bar, foo, foo, bar), "[]");
    }

    private void assertNullResult(@Language("SQL") String sql)
    {
        assertResult(sql, "[[null]]");
    }

    private void assertResult(@Language("SQL") String sql, String expected)
    {
        assertEquals(getQueryRunner().execute(sql).getMaterializedRows().toString(), expected);
        try {
            getQueryRunner().getSessionPropertyManager().removeRuntimeSystemSessionProperty(ZERO_SAFE_DIVISION);
            assertThatThrownBy(() -> getQueryRunner().execute(sql)).hasMessageMatching("Division by zero");
        }
        finally {
            getQueryRunner().getSessionPropertyManager().addRuntimeSystemSessionProperty(ZERO_SAFE_DIVISION, "true");
        }
    }
}
