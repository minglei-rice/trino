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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IndexField;
import org.apache.iceberg.IndexFile;
import org.apache.iceberg.IndexSpec;
import org.apache.iceberg.IndexType;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexClient;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexWriter;
import org.apache.iceberg.index.util.IndexUtils;
import org.apache.iceberg.io.CloseableIterable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

/**
 * Test reading iceberg tables with indices.
 */
public class TestIcebergIndex
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
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        this.hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        File tempDir = Files.createTempDirectory("test_iceberg_split_source").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");
        this.metastore = createTestingFileHiveMetastore(metastoreDir);

        return createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("iceberg.read-indices-switch-on", "true"),
                Collections.emptyList(),
                Optional.of(metastoreDir));
    }

    @Test
    public void testFieldID()
            throws Exception
    {
        getQueryRunner().execute("create table foo(x int,y int)");
        getQueryRunner().execute("insert into foo values (1,2),(3,4),(5,6)");
        SchemaTableName tableName = new SchemaTableName("tpch", "foo");
        Table table = IcebergUtil.loadIcebergTable(metastore, new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment)), SESSION, tableName);
        // define indices on both x and y
        table.updateIndexSpec().addIndex("x_bf", IndexType.BLOOMFILTER, "x", Collections.emptyMap()).commit();
        table.updateIndexSpec().addIndex("y_bf", IndexType.BLOOMFILTER, "y", Collections.emptyMap()).commit();
        // generate index files
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> taskIterable = table.newScan().caseSensitive(false).ignoreResiduals().includeIndexStats().planFiles()) {
            taskIterable.iterator().forEachRemaining(tasks::add);
        }
        assertEquals(tasks.size(), 1, "Can only handle a single task");
        List<DataFile> currentDataFiles = tasks.stream()
                .map(FileScanTask::file)
                .collect(Collectors.toList());
        DataFile newDataFile = generateIndexFiles(
                table,
                tasks.get(0),
                new Object[][] {
                        new Object[] {1, 2},
                        new Object[] {3, 4},
                        new Object[] {5, 6}
                });
        RewriteFiles rewriteFiles = table.newRewrite();
        rewriteFiles.rewriteFiles(Sets.newHashSet(currentDataFiles), Sets.newHashSet(newDataFile));
        rewriteFiles.commit();

        // only select y from the table, to make sure we're using the correct index to filter the data
        assertEquals(getQueryRunner().execute("select y from foo where y=2").getMaterializedRows().size(), 1);
    }

    private DataFile generateIndexFiles(Table table, FileScanTask task, Object[][] rows)
            throws Exception
    {
        Path indexRootPath = new Path(table.location(), "index");
        return generateIndexFiles(table, task, indexRootPath, rows);
    }

    // generate all required index files for a source file, and return the updated DataFile
    private DataFile generateIndexFiles(Table table, FileScanTask task, Path indexRootPath, Object[][] rows)
            throws Exception
    {
        DataFile sourceFile = task.file();
        IndexSpec indexSpec = table.indexSpec();
        List<IndexFile> indexFiles = new ArrayList<>();
        IndexClient client = new IndexClient(table.io());
        for (IndexField indexField : indexSpec.fields()) {
            Path indexPath = IndexUtils.getIndexPath(new Path(sourceFile.path().toString()), new Path(table.location()),
                    indexField, indexRootPath, 0);
            IndexFile indexFile = new IndexFile(indexPath.toString(), indexField.indexType(),
                    indexField.sourceId(), indexField.indexName());
            IndexMetadata indexMetadata = new IndexMetadata(indexFile, new Properties(), sourceFile.recordCount());
            IndexWriter indexWriter = client.createIndexWriter(indexMetadata);
            // just assume source id starts from 1
            final int ordinal = indexField.sourceId() - 1;
            for (Object[] row : rows) {
                indexWriter.addData(row[ordinal]);
            }
            indexWriter.finish();
            indexFiles.add(indexFile);
        }
        return DataFiles.builder(table.spec()).copy(sourceFile).withIndexFile(indexFiles).build();
    }
}
