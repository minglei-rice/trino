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
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static org.testng.Assert.assertEquals;

public class TestAccessHiveFunction
        extends AbstractTestQueryFramework
{
    private File metastoreDir;
    private HiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;

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

        File tempDir = Files.createTempDirectory("test_hive_function").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");
        this.metastore = createTestingFileHiveMetastore(metastoreDir);

        return IcebergQueryRunner.builder()
                .setMetastoreDirectory(metastoreDir)
                .setInitialTables(TpchTable.getTables())
                .build();
    }

    @Test
    public void testHiveBuiltInFunction()
    {
        assertEquals(getQueryRunner().execute("select reflect('java.lang.Math', 'abs', -3)").getOnlyValue(), "3");
        assertEquals(getQueryRunner().execute("select bin(12)").getOnlyValue(), "1100");
        assertEquals(getQueryRunner().execute("SELECT murmur_hash('hive')").getOnlyValue().toString(), "-1992589179");
        getQueryRunner().execute("create table foo(x int,y int)");
        getQueryRunner().execute("insert into foo values (-1,2),(-3,4),(5,6)");
        List<MaterializedRow> rows = getQueryRunner().execute("select reflect('java.lang.Math', 'abs', x) from foo").getMaterializedRows();
        assertEquals(rows.stream().map(row -> row.getField(0).toString()).collect(Collectors.toList()), List.of("1", "3", "5"));
    }
}
