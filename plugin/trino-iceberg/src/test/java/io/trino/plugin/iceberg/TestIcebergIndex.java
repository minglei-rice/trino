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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.QueryId;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.metrics.DataSkippingMetrics;
import io.trino.spi.type.TestingTypeManager;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CorrelatedColumns;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IndexField;
import org.apache.iceberg.IndexFile;
import org.apache.iceberg.IndexSpec;
import org.apache.iceberg.IndexType;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.index.IndexFactory;
import org.apache.iceberg.index.IndexWriter;
import org.apache.iceberg.index.IndexWriterContext;
import org.apache.iceberg.index.IndexWriterResult;
import org.apache.iceberg.index.util.IndexUtils;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CorrelationUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.util.MetricsUtils.DATA_SKIPPING_METRICS_NAME;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

/**
 * Test reading iceberg tables with indices.
 */
@Test(singleThreaded = true)
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
        HdfsConfiguration configuration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        this.hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        File tempDir = Files.createTempDirectory("test_iceberg_split_source").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");
        this.metastore = createTestingFileHiveMetastore(metastoreDir);

        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of("iceberg.read-indices-switch-on", "true"))
                .setExtraProperties(ImmutableMap.of("enable-dynamic-filtering", "false", "join-distribution-type", "PARTITIONED"))
                .setMetastoreDirectory(metastoreDir).build();
    }

    @Test
    public void testFieldID()
            throws Exception
    {
        getQueryRunner().execute("create table foo(x int,y int)");
        getQueryRunner().execute("insert into foo values (1,2),(3,4),(5,6)");
        Table table = loadIcebergTable("foo");
        // define indices on both x and y
        table.updateIndexSpec().addIndex("x_bf", IndexType.BLOOMFILTER, "x", Collections.emptyMap()).commit();
        table.updateIndexSpec().addIndex("y_bf", IndexType.BLOOMFILTER, "y", Collections.emptyMap()).commit();
        // generate index files
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> taskIterable = table.newScan().caseSensitive(false).ignoreResiduals().includeIndexStats().planFiles()) {
            taskIterable.iterator().forEachRemaining(tasks::add);
        }
        assertEquals(tasks.size(), 1, "Can only handle a single task");
        generateIndexFiles(
                table,
                tasks.get(0),
                new Object[][] {
                        new Object[] {1, 2},
                        new Object[] {3, 4},
                        new Object[] {5, 6}
                });

        // only select y from the table, to make sure we're using the correct index to filter the data
        assertEquals(getQueryRunner().execute("select y from foo where y=2").getMaterializedRows().size(), 1);

        MaterializedResultWithQueryId resultWithId = getDistributedQueryRunner().executeWithQueryId(getSession(), "select y from foo where y=3");
        QueryId queryId = resultWithId.getQueryId();
        List<OperatorStats> operatorStats = getTableScanStats(queryId, "foo");
        assertEquals(operatorStats.size(), 1);
        DataSkippingMetrics dataSkippingMetrics = (DataSkippingMetrics) operatorStats.get(0).getConnectorMetrics().getMetrics().get(DATA_SKIPPING_METRICS_NAME);
        // make sure split is skipped on worker
        assertEquals(dataSkippingMetrics.getMetricMap().get(DataSkippingMetrics.MetricType.SKIPPED_BY_INDEX_IN_WORKER).getSplitCount(), 1L);
    }

    @Test
    public void testCorrColFilter()
            throws Exception
    {
        getQueryRunner().execute("create table fact(f_k int,v int)");
        getQueryRunner().execute("create table dim(d_k int,v double)");
        getQueryRunner().execute("insert into fact values (1,1),(1,2),(3,3)");
        getQueryRunner().execute("insert into dim values (1,1.0),(2,2.0),(3,3.0),(4,4.0)");
        Table fact = loadIcebergTable("fact");
        Table dim = loadIcebergTable("dim");
        // alter table fact add correlated column (v as dim_v) from inner join dim on f_k=d_k with pk_fk
        CorrelatedColumns.Builder corrColBuilder = new CorrelatedColumns.Builder(fact.schema(), false)
                .corrTableId(TableIdentifier.of("tpch", "dim"))
                .joinType(CorrelatedColumns.JoinType.INNER)
                .leftKeyNames(Collections.singletonList("f_k"))
                .rightKeys(Collections.singletonList("d_k"))
                .joinConstraint(CorrelatedColumns.JoinConstraint.PK_FK)
                .addColumn(1, "v", Types.DoubleType.get(), "dim_v");
        fact.updateCorrelatedColumnsSpec().addCorrelatedColumns(corrColBuilder.build()).commit();
        fact.refresh();
        fact.updateIndexSpec().addIndex("dim_v_mm", IndexType.MINMAX, "dim_v", Collections.emptyMap()).commit();
        fact.refresh();
        List<FileScanTask> fileScanTasks = new ArrayList<>();
        fact.newScan().planFiles().forEach(fileScanTasks::add);
        assertEquals(fileScanTasks.size(), 1);
        generateIndexFiles(
                fact,
                fileScanTasks.get(0),
                new Object[][] {
                        new Object[] {1, 1, 1.0},
                        new Object[] {1, 2, 1.0},
                        new Object[] {3, 3, 3.0}
                },
                dim);
        fact.refresh();

        MaterializedResultWithQueryId resultWithId = getDistributedQueryRunner().executeWithQueryId(
                getSession(), "select sum(fact.v) from fact join dim on f_k=d_k where dim.v=5.0 group by f_k");
        assertEquals(resultWithId.getResult().getMaterializedRows().size(), 0);
        QueryId queryId = resultWithId.getQueryId();
        List<OperatorStats> operatorStats = getTableScanStats(queryId, "fact");
        assertEquals(operatorStats.size(), 1);
        assertEquals(operatorStats.get(0).getOutputPositions(), 0L);

        getQueryRunner().execute("insert into dim values (5,5.0)");
        resultWithId = getDistributedQueryRunner().executeWithQueryId(
                getSession(), "select sum(fact.v) from fact join dim on f_k=d_k where dim.v=5.0 group by f_k");
        assertEquals(resultWithId.getResult().getMaterializedRows().size(), 0);
        queryId = resultWithId.getQueryId();
        operatorStats = getTableScanStats(queryId, "fact");
        assertEquals(operatorStats.size(), 1);
        DataSkippingMetrics dataSkippingMetrics = (DataSkippingMetrics) operatorStats.get(0).getConnectorMetrics().getMetrics().get(DATA_SKIPPING_METRICS_NAME);
        assertEquals(dataSkippingMetrics.getMetricMap().get(DataSkippingMetrics.MetricType.READ).getSplitCount(), 1L);
    }

    private List<OperatorStats> getTableScanStats(QueryId queryId, String tableName)
    {
        List<OperatorStats> operatorStats =
                getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats().getOperatorSummaries();
        List<OperatorStats> allTSStats = operatorStats.stream()
                .filter(x -> x.getOperatorType().equals("ScanFilterAndProjectOperator")).collect(Collectors.toList());
        PlanNode root = getDistributedQueryRunner().getQueryPlan(queryId).getRoot();
        List<OperatorStats> res = new ArrayList<>();
        for (OperatorStats stats : allTSStats) {
            PlanNode start = PlanNodeSearcher.searchFrom(root).where(n -> n.getId().equals(stats.getPlanNodeId())).findOnlyElement(null);
            assertNotNull(start, String.format("PlanNodeId (%s) not found in query plan", stats.getPlanNodeId()));
            Optional<PlanNode> tableScan = PlanNodeSearcher.searchFrom(start).where(node -> {
                if (node instanceof TableScanNode) {
                    TableScanNode ts = (TableScanNode) node;
                    return ((IcebergTableHandle) ts.getTable().getConnectorHandle()).getTableName().equals(tableName);
                }
                return false;
            }).findSingle();
            tableScan.ifPresent(ignore -> res.add(stats));
        }
        return res;
    }

    @Test
    public void testTokenBF()
            throws Exception
    {
        getQueryRunner().execute("create table test(x int,y varchar, z varchar)");
        getQueryRunner().execute("insert into test values (1,'Spring is warm','春天来了'),(3,'Summer is hot','夏天来了'),(5,'Winter is cold','冬天来了')");
        Table table = loadIcebergTable("test");
        // define token bf on y
        table.updateIndexSpec().addIndex("y_tokenbf", IndexType.TOKENBF, "y", Collections.emptyMap()).commit();
        table.updateIndexSpec().addIndex("z_ngrambf", IndexType.NGRAMBF, "z", Collections.emptyMap()).commit();
        // generate index files
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> taskIterable = table.newScan().caseSensitive(false).ignoreResiduals().includeIndexStats().planFiles()) {
            taskIterable.iterator().forEachRemaining(tasks::add);
        }
        assertEquals(tasks.size(), 1, "Can only handle a single task");
        generateIndexFiles(
                table,
                tasks.get(0),
                new Object[][] {
                        new Object[] {1, "Spring is warm", "春天来了"},
                        new Object[] {3, "Summer is hot", "夏天来了"},
                        new Object[] {5, "Winter is cold", "冬天来了"}
                });

        // test ngrambf data skipping
        MaterializedResultWithQueryId resultWithId = getDistributedQueryRunner().executeWithQueryId(
                getSession(), "select y from test where z LIKE '%秋天来了%'");
        assertEquals(resultWithId.getResult().getMaterializedRows().size(), 0);
        QueryId queryId = resultWithId.getQueryId();
        List<OperatorStats> operatorStats = getTableScanStats(queryId, "test");
        assertEquals(operatorStats.size(), 1);
        DataSkippingMetrics dataSkippingMetrics = (DataSkippingMetrics) operatorStats.get(0).getConnectorMetrics().getMetrics().get("iceberg_data_skipping_metrics");
        assertEquals(dataSkippingMetrics.getMetricMap().get(DataSkippingMetrics.MetricType.SKIPPED_BY_INDEX_IN_WORKER).getSplitCount(), 1L);

        // test tokenbf data skipping
        resultWithId = getDistributedQueryRunner().executeWithQueryId(
                getSession(), "select y from test where starts_with(y, 'Autumn is cool')");
        assertEquals(resultWithId.getResult().getMaterializedRows().size(), 0);
        queryId = resultWithId.getQueryId();
        operatorStats = getTableScanStats(queryId, "test");
        assertEquals(operatorStats.size(), 1);
        dataSkippingMetrics = (DataSkippingMetrics) operatorStats.get(0).getConnectorMetrics().getMetrics().get("iceberg_data_skipping_metrics");
        assertEquals(dataSkippingMetrics.getMetricMap().get(DataSkippingMetrics.MetricType.SKIPPED_BY_INDEX_IN_WORKER).getSplitCount(), 1L);

        // test tokenbf data skipping by has_token
        resultWithId = getDistributedQueryRunner().executeWithQueryId(
                getSession(), "select y from test where has_token(y, 'Autumn')");
        assertEquals(resultWithId.getResult().getMaterializedRows().size(), 0);
        queryId = resultWithId.getQueryId();
        operatorStats = getTableScanStats(queryId, "test");
        assertEquals(operatorStats.size(), 1);
        dataSkippingMetrics = (DataSkippingMetrics) operatorStats.get(0).getConnectorMetrics().getMetrics().get("iceberg_data_skipping_metrics");
        assertEquals(dataSkippingMetrics.getMetricMap().get(DataSkippingMetrics.MetricType.SKIPPED_BY_INDEX_IN_WORKER).getSplitCount(), 1L);

        // test no data skipping
        resultWithId = getDistributedQueryRunner().executeWithQueryId(
                getSession(), "select y from test where starts_with(y, 'Spring is warm') and z LIKE '%春天来了%'");
        assertEquals(resultWithId.getResult().getMaterializedRows().size(), 1);
        queryId = resultWithId.getQueryId();
        operatorStats = getTableScanStats(queryId, "test");
        assertEquals(operatorStats.size(), 1);
        dataSkippingMetrics = (DataSkippingMetrics) operatorStats.get(0).getConnectorMetrics().getMetrics().get("iceberg_data_skipping_metrics");
        assertFalse(dataSkippingMetrics.getMetricMap().containsKey(DataSkippingMetrics.MetricType.SKIPPED_BY_INDEX_IN_WORKER));

        getQueryRunner().execute("drop table test");
    }

    @Test
    public void testIndexOnTransform() throws Exception
    {
        String json1 = "{\"employee\":[{\"name\":\"amy\", \"age\": 20}," +
                " {\"name\":\"bob\", \"age\":18}]}";
        String json2 = "{\"employee\":[{\"name\":\"dan\", \"age\": 40}," +
                " {\"name\":\"hen\", \"age\":38}]}";
        getQueryRunner().execute("create table test(a int,b varchar, c array(int), d map(varchar, int), e varchar)");
        getQueryRunner().execute(String.format("insert into test values (1,'a',array[1,2],map(array['a','b'],array[1,2]), '%s')", json1));
        Table table = loadIcebergTable("test");
        // define token bf on y
        table.updateIndexSpec().addIndex("c_bf", IndexType.BLOOMFILTER, "c", Collections.emptyMap()).commit();
        table.updateIndexSpec().addIndex("d_values_bf", IndexType.BLOOMFILTER, Expressions.mapValues("d"), Collections.emptyMap()).commit();
        table.updateIndexSpec().addIndex("e_json_bf", IndexType.BLOOMFILTER, Expressions.jsonExtractScalar("e", "$.employee[0].age"), Collections.emptyMap()).commit();
        // generate index files
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> taskIterable = table.newScan().caseSensitive(false).ignoreResiduals().includeIndexStats().planFiles()) {
            taskIterable.iterator().forEachRemaining(tasks::add);
        }
        assertEquals(tasks.size(), 1);
        Map<String, Integer> map1 = new HashMap<>();
        map1.put("a", 1);
        map1.put("b", 2);
        generateIndexFiles(
                table,
                tasks.get(0),
                new Object[][]{
                        new Object[]{1, "a", Lists.newArrayList(1, 2), map1, json1}
                });
        getQueryRunner().execute(String.format("insert into test values (2,'b',array[3,4],map(array['c','d'],array[3,4]), '%s')", json2));

        // test array contains
        MaterializedResultWithQueryId resultWithId = getDistributedQueryRunner().executeWithQueryId(
                getSession(), "select a from test where contains(c, 3)");
        assertEquals(resultWithId.getResult().getMaterializedRows().size(), 1);
        QueryId queryId = resultWithId.getQueryId();
        List<OperatorStats> operatorStats = getTableScanStats(queryId, "test");
        assertEquals(operatorStats.size(), 1);
        DataSkippingMetrics dataSkippingMetrics = (DataSkippingMetrics) operatorStats.get(0).getConnectorMetrics().getMetrics().get("iceberg_data_skipping_metrics");
        assertEquals(dataSkippingMetrics.getMetricMap().get(DataSkippingMetrics.MetricType.SKIPPED_BY_INDEX_IN_WORKER).getSplitCount(), 1L);

        //test map element_at
        resultWithId = getDistributedQueryRunner().executeWithQueryId(
                getSession(), "select a from test where element_at(d, 'c') = 3");
        assertEquals(resultWithId.getResult().getMaterializedRows().size(), 1);
        queryId = resultWithId.getQueryId();
        operatorStats = getTableScanStats(queryId, "test");
        assertEquals(operatorStats.size(), 1);
        dataSkippingMetrics = (DataSkippingMetrics) operatorStats.get(0).getConnectorMetrics().getMetrics().get("iceberg_data_skipping_metrics");
        assertEquals(dataSkippingMetrics.getMetricMap().get(DataSkippingMetrics.MetricType.SKIPPED_BY_INDEX_IN_WORKER).getSplitCount(), 1L);

        // test json_extract_scalar
        resultWithId = getDistributedQueryRunner().executeWithQueryId(
                getSession(), "select a from test where json_extract_scalar(e, '$.employee[0].age') = '40'");
        assertEquals(resultWithId.getResult().getMaterializedRows().size(), 1);
        queryId = resultWithId.getQueryId();
        operatorStats = getTableScanStats(queryId, "test");
        assertEquals(operatorStats.size(), 1);
        dataSkippingMetrics = (DataSkippingMetrics) operatorStats.get(0).getConnectorMetrics().getMetrics().get("iceberg_data_skipping_metrics");
        assertEquals(dataSkippingMetrics.getMetricMap().get(DataSkippingMetrics.MetricType.SKIPPED_BY_INDEX_IN_WORKER).getSplitCount(), 1L);

        getQueryRunner().execute("drop table test");
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

    private void generateIndexFiles(Table table, FileScanTask task, Object[][] rows)
            throws Exception
    {
        generateIndexFiles(table, task, rows, null);
    }

    private void generateIndexFiles(Table table, FileScanTask task, Object[][] rows, Table corrTable)
            throws Exception
    {
        Path indexRootPath = new Path(table.location(), "index");
        DataFile newDataFile = generateIndexFiles(table, task, indexRootPath, rows, corrTable);
        RewriteFiles rewriteFiles = table.newRewrite();
        rewriteFiles.rewriteFiles(Sets.newHashSet(task.file()), Sets.newHashSet(newDataFile));
        rewriteFiles.commit();
    }

    // generate all required index files for a source file, and return the updated DataFile
    private DataFile generateIndexFiles(Table table, FileScanTask task, Path indexRootPath, Object[][] rows, Table corrTable)
            throws Exception
    {
        DataFile sourceFile = task.file();
        IndexSpec indexSpec = table.indexSpec();
        List<IndexFile> indexFiles = new ArrayList<>();
        for (IndexField indexField : indexSpec.fields()) {
            Path indexPath = IndexUtils.getIndexPath(new Path(sourceFile.path().toString()), new Path(table.location()), indexField, indexRootPath);
            Type dataType = CorrelationUtils.schemaWithCorrCols(table.schema(), table.correlatedColumnsSpec()).findType(indexField.sourceId());
            IndexWriter indexWriter = IndexFactory.createIndexWriter(table.io(), indexPath.toString(), -1,
                    new IndexWriterContext<>(indexField, dataType, null, null));
            // just assume source id starts from 1
            final int ordinal = indexField.sourceId() - 1;
            for (Object[] row : rows) {
                indexWriter.addData(row[ordinal]);
            }
            IndexWriterResult writerResult = indexWriter.finish();
            IndexFile indexFile = corrTable == null ? new IndexFile(indexField.indexId(), writerResult.isInPlace(), writerResult.getIndexData()) : new IndexFile(indexField.indexId(), writerResult.isInPlace(), writerResult.getIndexData(), corrTable.currentSnapshot().snapshotId());
            indexFiles.add(indexFile);
        }
        return DataFiles.builder(table.spec()).copy(sourceFile).withIndexFile(indexFiles).build();
    }
}
