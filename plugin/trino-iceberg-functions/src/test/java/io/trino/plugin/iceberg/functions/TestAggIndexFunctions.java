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
package io.trino.plugin.iceberg.functions;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.trino.Session;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.HdfsFileIoProvider;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.assertions.Assert;
import org.apache.iceberg.AggIndexFile;
import org.apache.iceberg.CorrelatedColumns;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.cube.AggregationHolder;
import org.apache.iceberg.cube.Functions;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.AssertProvider;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.functions.IcebergQueryRunner.createIcebergQueryRunner;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.apache.iceberg.types.Types.NestedField.optional;

@Test(singleThreaded = true)
public class TestAggIndexFunctions
        extends AbstractTestQueryFramework
{
    private File metastoreDir;
    private HiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;
    private QueryRunner queryRunner;

    private static final Map<String, String> PROPERTIES = ImmutableMap.of(
            "allow_read_agg_index_files", "true");

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

    @BeforeMethod
    public void startMethod()
    {
        createTables();
    }

    @AfterMethod
    public void endMethod()
    {
        dropTables();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        this.hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        File tempDir = Files.createTempDirectory("test_iceberg_table").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");
        this.metastore = createTestingFileHiveMetastore(metastoreDir);
        queryRunner = createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(),
                Collections.emptyList(),
                Optional.of(metastoreDir));
        return queryRunner;
    }

    private void createTables()
    {
        // p_lineorder is a fact table
        queryRunner.execute(TEST_SESSION, "create table p_lineorder(v_revenue bigint, lo_orderdate bigint, lo_discount bigint, lo_quantity bigint, lo_custkey bigint, lo_suppkey bigint, lo_revenue bigint, lo_money decimal(10, 2))");
        queryRunner.execute(TEST_SESSION, "create table dates(d_datekey bigint, d_year bigint, d_yearmonth varchar)");
        //
        queryRunner.execute(TEST_SESSION, "create table test(id bigint, m1 bigint, m2 double, m3 real, m4 varchar, dm1 bigint, dm2 bigint)");
    }

    private void dropTables()
    {
        queryRunner.execute(TEST_SESSION, "drop table p_lineorder");
        queryRunner.execute(TEST_SESSION, "drop table dates");
        queryRunner.execute(TEST_SESSION, "drop table test");
    }

    private void insertDataForTestTable()
    {
        Map<String, String> properties = ImmutableMap.of(
                "allow_read_agg_index_files", "true",
                "task_writer_count", "1");
        Session testSession = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("tpch")
                .setSystemProperties(properties).build();
        queryRunner.execute(testSession, "insert into test values (1, 1, 2.0, 3.0, 's1', 2, 3), " +
                "(2, 2, 3.0, 4.0, 's2', 2, 3), (3, 4, 4.0, 5.0, 's3', 2, 3), (4, 4, 4.0, 5.0, 's3', 3, 4), " +
                "(5, 4, 4.0, 5.0, 's3', 3, 4), (6, 5, 5.0, 6.0, 's4', 4, 5), (7, 6, 6.0, 7.0, 's5', 4, 5), " +
                "(8, 7, 7.0, 8.0, 's6', 4, 5)");
    }

    private Schema buildFactTable()
    {
        return new Schema(optional(1, "v_revenue", Types.LongType.get()),
                optional(2, "lo_orderdate", Types.LongType.get()),
                optional(3, "lo_discount", Types.LongType.get()),
                optional(4, "lo_quantity", Types.LongType.get()),
                optional(5, "lo_custkey", Types.LongType.get()),
                optional(6, "lo_suppkey", Types.LongType.get()),
                optional(7, "lo_revenue", Types.LongType.get()),
                optional(8, "lo_money", Types.DecimalType.of(10, 2)));
    }

    /**
     * alter table P_LINEORDER add correlated column (d_year) from left join dates on lo_orderdate = d_datekey with pk_fk;
     */
    private CorrelatedColumns buildCorrColsForQ1_1()
    {
        return new CorrelatedColumns.Builder(buildFactTable(), false)
                .correlationId(1)
                .corrTableId(TableIdentifier.of("dates"))
                .joinConstraint(CorrelatedColumns.JoinConstraint.PK_FK)
                .joinType(CorrelatedColumns.JoinType.LEFT)
                .leftKeyIds(Collections.singletonList(2))
                .rightKeys(Collections.singletonList("d_datekey"))
                .addColumn(2, "d_year", Types.LongType.get()).build();
    }

    /**
     * Test data:
     * sql("insert into p_lineorder values
     * # v_revenue|lo_orderdate|lo_discount|lo_quantity|lo_custkey|lo_suppkey|lo_revenue|lo_money
     * (10, 20221002, 5, 10, 11390002, 1519995, 10, 100.50),
     * (19, 20221103, 8, 12, 11390002, 1519995, 19, 200.52),
     * (22, 20221103, 8, 12, 11390002, 1519995, 19, 300.54),
     * (30, 20221002, 12, 30, 11390002, 1519995, 30, 400.54));
     * # d_datekey|d_year|d_yearmonth
     * sql("insert into dates values (20221103, 2022, '11'),(20221002, 2022, '10')");
     */
    @Test
    public void testQueryRewriteForAvgWithRealData()
    {
        Table table = loadIcebergTable("p_lineorder");
        AggregationHolder countAgg = new AggregationHolder(Functions.Count.get(), "*");
        AggregationHolder sumAgg = new AggregationHolder(Functions.Sum.get(), "v_revenue");
        AggregationHolder avgDoubleAgg = new AggregationHolder(Functions.Avg.get(), "v_revenue");
        AggregationHolder minAgg = new AggregationHolder(Functions.Min.get(), "lo_money");
        AggregationHolder avgDecimalAgg = new AggregationHolder(Functions.Avg.get(), "lo_money");
        List<AggregationHolder> measuresAgg = new ArrayList<>(Arrays.asList(countAgg, sumAgg, avgDoubleAgg, minAgg, avgDecimalAgg));
        table.updateCorrelatedColumnsSpec().addCorrelatedColumns(buildCorrColsForQ1_1()).commit();
        table.updateAggregationIndexSpec()
                .addAggregationIndex("index1", Arrays.asList("d_year", "lo_discount", "lo_quantity", "lo_orderdate"), measuresAgg)
                .commit();
        addAggIndexFiles(table, "./aggindex/lineorder_dates", "lineorder_dates_avg_agg_index.parquet");

        AssertProvider<QueryAssertions.QueryAssert> queryAssert = query(TEST_SESSION, "select count(*), avg(lo_money), avg(lo_money) from p_lineorder left join dates on lo_orderdate = d_datekey where d_year = 2022 group by lo_discount, lo_quantity");
        queryAssert.assertThat().matches(new MaterializedResult(Arrays.asList(
                new MaterializedRow(Arrays.asList(2L, new BigDecimal("250.53"), new BigDecimal("250.53"))),
                new MaterializedRow(Arrays.asList(1L, new BigDecimal("100.50"), new BigDecimal("100.50"))),
                new MaterializedRow(Arrays.asList(1L, new BigDecimal("400.54"), new BigDecimal("400.54")))),
                Arrays.asList(BigintType.BIGINT, DecimalType.createDecimalType(10, 2), DecimalType.createDecimalType(10, 2))));

        queryAssert = query(TEST_SESSION,
                "select sum(v_revenue) as v_revenue_sum, count(*) as v_revenue_cnt, avg(v_revenue) as avg_revenue, avg(lo_money) as avg_money, min(lo_money) from p_lineorder left join dates on lo_orderdate = d_datekey where d_year = 2022");

        queryAssert.assertThat().matches(new MaterializedResult(Collections.singletonList(new MaterializedRow(
                Arrays.asList(81L, 4L, 20.25, new BigDecimal("250.53"), new BigDecimal("100.50")))),
                Arrays.asList(BigintType.BIGINT, BigintType.BIGINT, DoubleType.DOUBLE, DecimalType.createDecimalType(10, 2), DecimalType.createDecimalType(10, 2))));

        queryAssert = query(TEST_SESSION, "select sum(v_revenue), count(*), avg(v_revenue) from p_lineorder left join dates on lo_orderdate = d_datekey where d_year = 2022 group by d_year");
        queryAssert.assertThat().matches(new MaterializedResult(Arrays.asList(new MaterializedRow(Arrays.asList(81L, 4L, 20.25))),
                Arrays.asList(BigintType.BIGINT, BigintType.BIGINT, DoubleType.DOUBLE)));

        queryAssert = query(TEST_SESSION, "select count(*), avg(lo_money) from p_lineorder left join dates on lo_orderdate = d_datekey where d_year = 2022 group by lo_discount");
        queryAssert.assertThat().matches(new MaterializedResult(Arrays.asList(
                new MaterializedRow(Arrays.asList(2L, new BigDecimal("250.53"))),
                new MaterializedRow(Arrays.asList(1L, new BigDecimal("100.50"))),
                new MaterializedRow(Arrays.asList(1L, new BigDecimal("400.54")))),
                Arrays.asList(BigintType.BIGINT, DecimalType.createDecimalType(10, 2))));
    }

    @Test
    public void testQueryRewriteForAggFunctions()
    {
        Table table = loadIcebergTable("p_lineorder");
        AggregationHolder countAgg = new AggregationHolder(Functions.Count.get(), "v_revenue");
        AggregationHolder sumAgg = new AggregationHolder(Functions.Sum.get(), "lo_money");
        AggregationHolder countDistinctAgg = new AggregationHolder(Functions.COUNT_DISTINCT.get(), "v_revenue");
        AggregationHolder approxDistinctAgg = new AggregationHolder(Functions.APPROX_COUNT_DISTINCT.get(), "lo_suppkey");
        AggregationHolder percentileAgg = new AggregationHolder(Functions.Percentile.of(), "lo_suppkey");
        List<AggregationHolder> measuresAgg = Lists.newArrayList(countAgg, countDistinctAgg, sumAgg, approxDistinctAgg, percentileAgg);
        table.updateAggregationIndexSpec()
                .addAggregationIndex("index1", Arrays.asList("lo_discount", "lo_quantity", "lo_orderdate"), measuresAgg)
                .commit();
        assertExplain(TEST_SESSION, "EXPLAIN select count(distinct v_revenue) from p_lineorder",
                "\\Qiceberg:tpch.p_lineorder$dataAggIndex{aggIndexId=1");
        assertExplain(TEST_SESSION, "EXPLAIN select approx_distinct(lo_suppkey), approx_percentile(lo_suppkey, 1.0, 0.5), count(distinct v_revenue) from p_lineorder",
                "\\Qiceberg:tpch.p_lineorder$dataAggIndex{aggIndexId=1");
        assertExplain(TEST_SESSION, "EXPLAIN select count(distinct v_revenue), count(v_revenue) as revenue from p_lineorder where lo_orderdate = 20221010",
                "\\Qiceberg:tpch.p_lineorder$dataAggIndex{aggIndexId=1");
        // approx_percentile with weight different from cube defined will not answered by cube
        assertExplain(TEST_SESSION, "EXPLAIN select approx_percentile(lo_suppkey, 2.0, 0.02) from p_lineorder",
                "\\Qiceberg:tpch.p_lineorder$dataALLALL");
        // approx_percentile with accuracy is not supported by cube
        assertExplain(TEST_SESSION, "EXPLAIN select approx_percentile(lo_suppkey, 0.5, 1, 0.01) from p_lineorder",
                "\\Qiceberg:tpch.p_lineorder$dataALLALL");
        // approx_distinct with maxStandardError is not supported by cube
        assertExplain(TEST_SESSION, "EXPLAIN select approx_distinct(lo_suppkey, 0.05) from p_lineorder",
                "\\Qiceberg:tpch.p_lineorder$dataALLALL");
    }

    @Test
    public void testSpecialCases()
    {
        getQueryRunner().execute("create table foo(x int,d1 int,d2 int)");
        Table table = loadIcebergTable("foo");
        table.updateAggregationIndexSpec().addAggregationIndex("index1", Arrays.asList("d1", "d2"),
                Collections.singletonList(new AggregationHolder(Functions.Count.get(), "*"))).commit();
        // should not be rewrite
        assertExplain(TEST_SESSION, "explain select count(*) from (select count(*) from foo group by d1) a",
                "\\Qiceberg:tpch.foo$dataALLALL");

        table.updateAggregationIndexSpec().removeAggregationIndex("index1").commit();
        table.updateAggregationIndexSpec().addAggregationIndex("index2", Arrays.asList("d2"),
                Collections.singletonList(new AggregationHolder(Functions.COUNT_DISTINCT.get(), "d1"))).commit();
        assertExplain(TEST_SESSION, "explain select count(*) from (select count(*) from foo group by d1) a",
                "\\Qiceberg:tpch.foo$dataAggIndex{aggIndexId=2");

        table.updateAggregationIndexSpec().removeAggregationIndex("index2").commit();
        table.updateAggregationIndexSpec().addAggregationIndex("index3", Arrays.asList("d2"),
                Collections.singletonList(new AggregationHolder(Functions.COUNT_DISTINCT.get(), "d1"))).commit();
        assertExplain(TEST_SESSION, "explain select count(*) from (select count(d2) from foo group by d1) a",
                "\\Qiceberg:tpch.foo$dataAggIndex{aggIndexId=3");
    }

    @Test
    public void testQueryRewriteForCountFunctions()
    {
        Table table = loadIcebergTable("p_lineorder");
        AggregationHolder countAgg = new AggregationHolder(Functions.Count.get(), "v_revenue");
        List<AggregationHolder> measuresAgg = Lists.newArrayList(countAgg);
        table.updateAggregationIndexSpec()
                .addAggregationIndex("index1", Arrays.asList("lo_discount"), measuresAgg)
                .commit();
        // will not rewrite
        assertExplain(TEST_SESSION, "EXPLAIN select lo_discount, lo_quantity from p_lineorder group by lo_discount, lo_quantity",
                "\\Qiceberg:tpch.p_lineorder$dataALLALL");
    }

    @Test
    public void testQueryRewriteForAggIndexWithRealData()
    {
        Table table = loadIcebergTable("test");
        AggregationHolder countDistinctAgg1 = new AggregationHolder(Functions.COUNT_DISTINCT.get(), "m1");
        AggregationHolder countDistinctAgg2 = new AggregationHolder(Functions.COUNT_DISTINCT.get(), "m4");
        AggregationHolder percentileAgg1 = new AggregationHolder(Functions.Percentile.of(), "m1");
        AggregationHolder percentileAgg2 = new AggregationHolder(Functions.Percentile.of(2.0), "m2");
        AggregationHolder percentileAgg3 = new AggregationHolder(Functions.Percentile.of(), "m3");
        AggregationHolder approxCDAgg = new AggregationHolder(Functions.APPROX_COUNT_DISTINCT.get(), "m2");
        insertDataForTestTable();
        List<AggregationHolder> measuresAgg = new ArrayList<>(Arrays.asList(countDistinctAgg1, countDistinctAgg2, percentileAgg1, percentileAgg2, percentileAgg3, approxCDAgg));
        table.updateAggregationIndexSpec()
                .addAggregationIndex("index1", Arrays.asList("dm1", "dm2"), measuresAgg)
                .commit();

        buildAggIndexFiles(table);
        Map<String, String> properties = ImmutableMap.of("allow_read_agg_index_files", "false");
        Session testDisableAggIndexSession = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("tpch")
                .setSystemProperties(properties).build();

        String sql = "select count(distinct m1), count(distinct m4), approx_distinct(m2), approx_percentile(m1, 1.0, 0.5), " +
                "approx_percentile(m2, 2.0, 0.5), approx_percentile(m3, 1.0, 0.5)  from test";
        List<MaterializedRow> aggIndexDataResult = queryRunner.execute(TEST_SESSION, sql).getMaterializedRows();
        List<MaterializedRow> dataResult = queryRunner.execute(testDisableAggIndexSession, sql).getMaterializedRows();
        Assert.assertEquals(dataResult, aggIndexDataResult);

        sql = "select count(distinct m1), count(distinct m4), approx_distinct(m2), approx_percentile(m1, 1.0, 0.5), " +
                "approx_percentile(m2, 2.0, 0.5), approx_percentile(m3, 1.0, 0.5)  from test group by dm1";
        assertExplain(TEST_SESSION, format("EXPLAIN %s", sql), "\\Qiceberg:tpch.test$dataAggIndex{aggIndexId=1");
        aggIndexDataResult = queryRunner.execute(TEST_SESSION, sql).getMaterializedRows();
        dataResult = queryRunner.execute(testDisableAggIndexSession, sql).getMaterializedRows();
        Assert.assertTrue(aggIndexDataResult.containsAll(dataResult));
        Assert.assertTrue(dataResult.containsAll(aggIndexDataResult));

        sql = "select count(distinct m1), count(distinct m4), approx_distinct(m2), approx_percentile(m1, 1.0, 0.5), " +
                "approx_percentile(m2, 2.0, 0.5), approx_percentile(m3, 1.0, 0.5)  from test where dm1 <= 3 group by dm1";
        assertExplain(TEST_SESSION, format("EXPLAIN %s", sql), "\\Qiceberg:tpch.test$dataAggIndex{aggIndexId=1");
        aggIndexDataResult = queryRunner.execute(TEST_SESSION, sql).getMaterializedRows();
        dataResult = queryRunner.execute(testDisableAggIndexSession, sql).getMaterializedRows();
        Assert.assertTrue(aggIndexDataResult.containsAll(dataResult));
        Assert.assertTrue(dataResult.containsAll(aggIndexDataResult));

        sql = "select count(distinct m1), count(distinct m4), approx_distinct(m2), approx_percentile(m1, 1.0, array[0.3, 0.7, 0.9]), " +
                "approx_percentile(m2, 2.0, array[0.2, 0.6, 0.9, 0.5]), approx_percentile(m3, 1.0, array[0.3, 0.4, 0.9, 0.7])  from test where dm1 <= 3 group by dm1";
        assertExplain(TEST_SESSION, format("EXPLAIN %s", sql), "\\Qiceberg:tpch.test$dataAggIndex{aggIndexId=1");
        aggIndexDataResult = queryRunner.execute(TEST_SESSION, sql).getMaterializedRows();
        dataResult = queryRunner.execute(testDisableAggIndexSession, sql).getMaterializedRows();
        Assert.assertTrue(aggIndexDataResult.containsAll(dataResult));
        Assert.assertTrue(dataResult.containsAll(aggIndexDataResult));
    }

    private Table loadIcebergTable(String tableName)
    {
        return IcebergUtil.loadIcebergTable(metastore, new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment)),
                SESSION, new SchemaTableName("tpch", tableName));
    }

    private void addAggIndexFiles(Table table, String sourceFilePath, String fileName)
    {
        String aggIndexDir = Joiner.on(File.separator).join(table.location(), "agg-index");
        String aggIndexPath = Joiner.on(File.separator).join(aggIndexDir, fileName);
        int specId = table.aggregationIndexSpec().specId();
        File sourceFile = new File(this.getClass().getClassLoader().getResource(
                Joiner.on(File.separator).join(sourceFilePath, fileName)).getFile());
        DataFile dataFile = DataFiles.builder(table.spec()).withPath(aggIndexPath)
                .withFileSizeInBytes(1000) // any value
                .withRecordCount(4) // any value
                .withAggIndexFiles(Arrays.asList(new AggIndexFile(specId, aggIndexPath, sourceFile.length()))).build();
        copyFiles(sourceFile, aggIndexDir, fileName);
        table.newAppend().appendFile(dataFile).commit();
    }

    private void buildAggIndexFiles(Table table)
    {
        String aggFileFolder = table.location() + "/agg-index/";
        String sourceFolder = "aggindex/test/";
        String aggFileName = "agg_index_file.orc";
        File aggFile = new File(this.getClass().getClassLoader().getResource(sourceFolder + aggFileName).getFile());
        copyFiles(aggFile, aggFileFolder, aggFileName);
        DataFile dataFile = table.newScan().planFiles().iterator().next().file();
        DataFile newFile = DataFiles.builder(table.spec())
                .copy(dataFile)
                .withAggIndexFiles(List.of(new AggIndexFile(table.aggregationIndexSpec().specId(), aggFileFolder + aggFileName, aggFile.length())))
                .build();
        table.newWriteAggIndices().rewriteFiles(Set.of(dataFile), Set.of(newFile))
                .commit();
    }

    private void copyFiles(File file, String targetDir, String fileName)
    {
        try {
            File targetPath = new File(targetDir);
            if (!targetPath.exists()) {
                targetPath.mkdirs();
            }
            Files.copy(file.toPath(), Path.of(Joiner.on(File.separator).join(targetDir, fileName)));
        }
        catch (IOException e) {
            throw new RuntimeException("Copy agg index file error", e);
        }
    }
}
