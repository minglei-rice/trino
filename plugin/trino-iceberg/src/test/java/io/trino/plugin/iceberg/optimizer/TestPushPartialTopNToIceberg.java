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
package io.trino.plugin.iceberg.optimizer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;

@Test(singleThreaded = true)
public class TestPushPartialTopNToIceberg
        extends AbstractTestQueryFramework
{
    private File metastoreDir;
    private HiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;
    private QueryRunner queryRunner;

    private static final Map<String, String> PROPERTIES = ImmutableMap.of(
            "allow_push_partial_top_n_to_table_scan", "true");

    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("iceberg")
            .setSchema("tpch")
            .setSystemProperties(PROPERTIES).build();

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.getParentFile().toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        this.hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        File tempDir = Files.createTempDirectory("test_iceberg_table").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");
        this.metastore = createTestingFileHiveMetastore(metastoreDir);
        queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of("iceberg.read-indices-switch-on", "true"))
                .setMetastoreDirectory(metastoreDir).build();
        return queryRunner;
    }

    @Test
    public void testReadSortedTable()
    {
        queryRunner.execute(TEST_SESSION, "create table t1(f1 bigint)");
        Table table = loadIcebergTable("t1");
        table.replaceSortOrder().asc("f1", NullOrder.NULLS_FIRST).commit();
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(table.location())
                .withFormat(FileFormat.ORC)
                .withRecordCount(100)
                .withFileSizeInBytes(table.newScan().targetSplitSize() * 10)
                .withSortOrder(SortOrder.builderFor(table.schema()).asc("f1", NullOrder.NULLS_FIRST).build()).build();
        table.newAppend().appendFile(dataFile).commit();
        assertExplain(TEST_SESSION, "EXPLAIN select * from t1 order by f1 desc nulls last limit 3", "\\QReversedTopN");
        assertExplain(TEST_SESSION, "EXPLAIN select * from t1 order by f1 nulls last limit 3", "\\QTopNPartial");
        assertExplain(TEST_SESSION, "EXPLAIN select * from t1 order by f1 nulls first limit 3", "\\QLimitPartial");
        assertExplain(TEST_SESSION, "EXPLAIN select * from t1 order by f1 + 1 desc nulls last limit 3", "\\QTopNPartial");
        assertExplain(TEST_SESSION, "EXPLAIN select * from t1 order by length(cast(f1 as varchar))=2 desc nulls last limit 3", "\\QTopNPartial");
        assertExplain(TEST_SESSION, "EXPLAIN select * from (select f1, count(*) as y from t1 group by f1) where y > 5 order by f1 limit 3", "\\QTopNPartial");
    }

    private Table loadIcebergTable(String tableName)
    {
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT);
        IcebergTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory);
        TrinoCatalog catalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                CachingHiveMetastore.memoizeMetastore(metastore, 1000),
                fileSystemFactory,
                new TestingTypeManager(),
                tableOperationsProvider,
                "test",
                false,
                false,
                false);
        return IcebergUtil.loadIcebergTable(catalog, new FileMetastoreTableOperationsProvider(new HdfsFileSystemFactory(hdfsEnvironment)),
                SESSION, new SchemaTableName("tpch", tableName));
    }
}
