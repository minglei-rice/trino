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
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.CorrelatedColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.cube.AggregationHolder;
import org.apache.iceberg.cube.Functions;
import org.apache.iceberg.types.Types;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.iceberg.types.Types.NestedField.optional;

@Test(singleThreaded = true)
public class TestCube
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

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
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

    private void createTables()
    {
        // p_lineorder is a fact table
        queryRunner.execute(TEST_SESSION, "create table p_lineorder(v_revenue bigint, lo_orderdate bigint, lo_discount bigint, lo_quantity bigint, lo_custkey bigint, lo_suppkey bigint, lo_revenue bigint) with (partitioning = ARRAY['lo_revenue'])");
        queryRunner.execute(TEST_SESSION, "create table dates(d_datekey bigint, d_year bigint, d_yearmonth varchar)");
        queryRunner.execute(TEST_SESSION, "create table customer(c_custkey bigint, c_city varchar)");
        queryRunner.execute(TEST_SESSION, "create table supplier(s_city varchar, s_suppkey bigint)");

        queryRunner.execute(TEST_SESSION, "INSERT INTO p_lineorder (v_revenue, lo_orderdate, lo_discount, lo_quantity, lo_custkey, lo_suppkey, lo_revenue) VALUES (100, 20230101, 5, 10, 1, 1, 500)");
        queryRunner.execute(TEST_SESSION, "INSERT INTO dates (d_datekey, d_year, d_yearmonth) VALUES (20230101, 2023, '2023-01')");
        queryRunner.execute(TEST_SESSION, "INSERT INTO customer (c_custkey, c_city) VALUES (1, 'New York')");
        queryRunner.execute(TEST_SESSION, "INSERT INTO supplier (s_city, s_suppkey) VALUES ('Los Angeles', 1)");

        // t1 is a fact table
        queryRunner.execute(TEST_SESSION, "create table t1(id bigint, id1 bigint, id2 bigint, foo bigint, log_date varchar) with (partitioning = ARRAY['log_date'])");
        queryRunner.execute(TEST_SESSION, "create table t2(id bigint, age bigint, flag bigint, miles bigint)");
        queryRunner.execute(TEST_SESSION, "create table t3(id bigint, height bigint)");

        queryRunner.execute(TEST_SESSION, "INSERT INTO t1 (id, id1, log_date) VALUES (1, 1, '2023-01-01')");
        queryRunner.execute(TEST_SESSION, "INSERT INTO t2 (id, age, flag, miles) VALUES (1, 30, 1, 1000)");
        queryRunner.execute(TEST_SESSION, "INSERT INTO t3 (id, height) VALUES (1, 180)");
    }

    private void dropTables()
    {
        queryRunner.execute(TEST_SESSION, "drop table p_lineorder");
        queryRunner.execute(TEST_SESSION, "drop table dates");
        queryRunner.execute(TEST_SESSION, "drop table customer");
        queryRunner.execute(TEST_SESSION, "drop table supplier");

        queryRunner.execute(TEST_SESSION, "drop table t1");
        queryRunner.execute(TEST_SESSION, "drop table t2");
        queryRunner.execute(TEST_SESSION, "drop table t3");
    }

    private Schema buildFactTable()
    {
        return new Schema(
                optional(1, "v_revenue", Types.LongType.get()),
                optional(2, "lo_orderdate", Types.LongType.get()),
                optional(3, "lo_discount", Types.LongType.get()),
                optional(4, "lo_quantity", Types.LongType.get()),
                optional(5, "lo_custkey", Types.LongType.get()),
                optional(6, "lo_suppkey", Types.LongType.get()),
                optional(7, "lo_revenue", Types.LongType.get()));
    }

    private Schema buildFactT1()
    {
        return new Schema(
                optional(1, "id", Types.LongType.get()),
                optional(2, "id1", Types.LongType.get()),
                optional(3, "id2", Types.LongType.get()),
                optional(4, "foo", Types.LongType.get()),
                optional(5, "log_date", Types.StringType.get()));
    }

    @Test
    public void testSingleTable()
    {
        createTables();
        Table table = loadIcebergTable("t1");
        List<AggregationHolder> measuresAgg = new ArrayList<>();
        AggregationHolder aggMin = new AggregationHolder(Functions.Min.get(), "id1");
        AggregationHolder aggMax = new AggregationHolder(Functions.Max.get(), "id");
        measuresAgg.add(aggMin);
        measuresAgg.add(aggMax);
        table.updateAggregationIndexSpec()
                .addAggregationIndex("aggIndex1", Arrays.asList("id2", "log_date"), measuresAgg)
                .commit();
        assertExplain(TEST_SESSION,
                "EXPLAIN select min(id1),MAX(id) from t1 where log_date='20220101'", "\\Qiceberg:tpch.t1$dataAggIndex{aggIndexId=1");
        assertExplain(TEST_SESSION,
                "EXPLAIN select min(id1),MAX(id) from t1 where id2 = 17 and log_date='20220101'",
                "\\Qiceberg:tpch.t1$dataAggIndex{aggIndexId=1");
        dropTables();
    }

    @Test
    public void testStarSchema()
    {
        createTables();
        Table table = loadIcebergTable("t1");
        List<AggregationHolder> measuresAgg = new ArrayList<>();
        AggregationHolder aggMin = new AggregationHolder(Functions.Min.get(), "id1");
        AggregationHolder aggMax = new AggregationHolder(Functions.Max.get(), "id");
        measuresAgg.add(aggMin);
        measuresAgg.add(aggMax);
        table.updateCorrelatedColumnsSpec().addCorrelatedColumns(buildCorrColsForDuplicateName_t2()).commit();
        table.updateCorrelatedColumnsSpec().addCorrelatedColumns(buildCorrColsForDuplicateName_t3()).commit();
        table.updateAggregationIndexSpec()
                .addAggregationIndex("aggIndex1", Arrays.asList("age", "flag", "height", "log_date", "id2", "foo"), measuresAgg)
                .commit();
        assertExplain(TEST_SESSION,
                "EXPLAIN select min(id1),MAX(t1.id) from t1 left join t2 on t2.id = t1.id where t2.age = 16 and log_date='20220101'",
                "\\Qiceberg:tpch.t1$dataAggIndex{aggIndexId=1");
        assertExplain(TEST_SESSION,
                "EXPLAIN select age, count(*) from t1 left join t2 on t2.id = t1.id  group by age",
                "\\QLeftJoin");
        assertExplain(TEST_SESSION,
                "EXPLAIN select min(id1) from t1 left join t2 on t2.id = t1.id where t2.age = 16 and log_date='20220101' group by age",
                "\\Qiceberg:tpch.t1$dataAggIndex{aggIndexId=1");
        assertExplain(TEST_SESSION,
                "EXPLAIN select max(t1.id) from t1 left join t2 on t1.id = t2.id left join t3 on t1.id1 = t3.id where t2.age = 16 and t3.height = 170 and log_date='20220101'",
                "\\Qiceberg:tpch.t1$dataAggIndex{aggIndexId=1");
        // grouping sets is not supported
        assertExplain(TEST_SESSION,
                "EXPLAIN select max(t1.id) from t1 left join t2 on t2.id = t1.id where t2.age = 16 group by grouping sets(age,flag)",
                "\\QInnerJoin");
        // do not support subquery expression in filter
        assertExplain(TEST_SESSION,
                "EXPLAIN select min(id1) from t1 left join t2 on t2.id = t1.id where t2.age in (select age from t2) group by age",
                "\\QInnerJoin");
        // or operator
        // TODO actually, it can work with cube, Due to the presence of data in the table, planFile will be generate data file,
        // TODO but our tests do not have actual corresponding cube files. Therefore, the final determination is that rewriting
        // TODO cannot be done. NOT supporting the OR operator now.
        assertExplain(TEST_SESSION,
                "EXPLAIN select min(id1) from t1 left join t2 on t2.id = t1.id where t2.age = 16 or t1.id2 = 28 group by age",
                "\\QLeftJoin");
        dropTables();
    }

    private CorrelatedColumns buildCorrColsForDuplicateName_t2()
    {
        return new CorrelatedColumns.Builder(buildFactT1(), false)
                .correlationId(1)
                .corrTableId(TableIdentifier.of("t2"))
                .joinConstraint(CorrelatedColumns.JoinConstraint.PK_FK)
                .joinType(CorrelatedColumns.JoinType.LEFT)
                .leftKeyIds(Collections.singletonList(1))
                .rightKeys(Collections.singletonList("id"))
                .addColumn(2, "age", Types.LongType.get())
                .addColumn(3, "flag", Types.LongType.get())
                .build();
    }

    private CorrelatedColumns buildCorrColsForDuplicateName_t3()
    {
        return new CorrelatedColumns.Builder(buildFactT1(), false)
                .correlationId(1)
                .corrTableId(TableIdentifier.of("t3"))
                .joinConstraint(CorrelatedColumns.JoinConstraint.PK_FK)
                .joinType(CorrelatedColumns.JoinType.LEFT)
                .leftKeyIds(Collections.singletonList(2))
                .rightKeys(Collections.singletonList("id"))
                .addColumn(2, "height", Types.LongType.get())
                .build();
    }

    /**
     * Q1.1
     * select sum(v_revenue) as revenue
     * from p_lineorder
     * left join dates on lo_orderdate = d_datekey
     * where d_year = 1993
     * and lo_discount between 1 and 3
     * and lo_quantity < 25;
     */
    @Test
    public void testQueryRewriteForQ1_1_And_Others()
    {
        createTables();
        Table table = loadIcebergTable("p_lineorder");
        List<AggregationHolder> measuresAgg = new ArrayList<>();
        AggregationHolder aggregationHolder = new AggregationHolder(Functions.Sum.get(), "v_revenue");
        measuresAgg.add(aggregationHolder);
        table.updateCorrelatedColumnsSpec().addCorrelatedColumns(buildCorrColsForQ1_1()).commit();
        table.updateAggregationIndexSpec()
                .addAggregationIndex("q1.1", Arrays.asList("d_year", "lo_discount", "lo_quantity", "lo_orderdate"), measuresAgg)
                .commit();
        assertExplain(TEST_SESSION, "EXPLAIN select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where d_year + 5 = 1993 and lo_revenue=2",
                "\\Qiceberg:tpch.p_lineorder$dataAggIndex{aggIndexId=1");
        assertExplain(TEST_SESSION, "EXPLAIN select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where 1993 = d_year and lo_revenue=2",
                "\\Qiceberg:tpch.p_lineorder$dataAggIndex{aggIndexId=1");
        assertExplain(TEST_SESSION, "EXPLAIN select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25 and lo_revenue=2",
                "\\Qiceberg:tpch.p_lineorder$dataAggIndex{aggIndexId=1");
        // do not support right join now
        assertExplain(TEST_SESSION, "EXPLAIN select sum(v_revenue) as revenue from p_lineorder right join dates on lo_orderdate = d_datekey where d_year = 1993",
                "\\QRightJoin");
        // join node can not match
        assertExplain(TEST_SESSION, "EXPLAIN select sum(v_revenue) as revenue from p_lineorder left join dates on lo_suppkey = d_datekey where d_year = 1993",
                "\\QInnerJoin");
        // left join right outputs has a filter
        assertExplain(TEST_SESSION, "EXPLAIN select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where lo_orderdate = 1993",
                "\\QLeftJoin");
        // filter node can not match
        assertExplain(TEST_SESSION, "EXPLAIN select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where d_yearmonth = '1993'",
                "\\QInnerJoin");
        // aggregation node can not match
        assertExplain(TEST_SESSION, "EXPLAIN select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where d_year = 1993 group by lo_revenue",
                "\\QInnerJoin");
        assertExplain(TEST_SESSION, "EXPLAIN select max(lo_orderdate, d_year) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where 1993 = d_year",
                "\\QInnerJoin");
        dropTables();
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
