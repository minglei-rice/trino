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
import com.google.common.io.Files;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.PrincipalType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.sql.planner.assertions.MatchResult;
import io.trino.sql.planner.assertions.Matcher;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.iterative.rule.PushCorrColFilterIntoTableScan;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.LocalQueryRunner;
import org.apache.iceberg.CorrelatedColumns;
import org.apache.iceberg.CorrelatedColumns.JoinConstraint;
import org.apache.iceberg.CorrelatedColumns.JoinType;
import org.apache.iceberg.IndexType;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestIcebergPushCorrColFilter
        extends BasePushdownPlanTest
{
    private static final String CATALOG = "iceberg";
    private static final String SCHEMA = "schema";
    private File metastoreDir;
    private HiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();

        metastoreDir = Files.createTempDir();
        metastore = createTestingFileHiveMetastore(metastoreDir);
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);

        queryRunner.createCatalog(
                CATALOG,
                new TestingIcebergConnectorFactory(Optional.of(metastore), Optional.empty()),
                ImmutableMap.of());

        Database database = Database.builder()
                .setDatabaseName(SCHEMA)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        metastore.createDatabase(new HiveIdentity(session.toConnectorSession()), database);

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (metastoreDir != null) {
            deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
        }
    }

    @Test
    public void testTableAsBothSrcAndDest()
            throws Exception
    {
        String foo = "foo" + randomTableSuffix();
        String bar = "bar" + randomTableSuffix();
        String baz = "baz" + randomTableSuffix();
        getQueryRunner().execute(format("create table %s (k1, i) as select 1, 1", foo));
        getQueryRunner().execute(format("create table %s (k1, k2, d) as select 1, 1, cast(1.1 as double)", bar));
        getQueryRunner().execute(format("create table %s (k2, s) as select 1, 'a'", baz));

        SchemaTableName tableName = new SchemaTableName(SCHEMA, foo);
        Table fooTable = IcebergUtil.loadIcebergTable(metastore, new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment)), SESSION, tableName);
        // alter table foo add correlated column (d) from join bar on foo.k1=bar.k1 with unique
        CorrelatedColumns.Builder corrColBuilder = new CorrelatedColumns.Builder(fooTable.schema(), false)
                .corrTableId(TableIdentifier.of(SCHEMA, bar))
                .joinType(JoinType.INNER)
                .leftKeyNames(Collections.singletonList("k1"))
                .rightKeys(Collections.singletonList("k1"))
                .joinConstraint(JoinConstraint.UNIQUE)
                .addColumn(1, "d", Types.DoubleType.get());
        fooTable.updateCorrelatedColumnsSpec().addCorrelatedColumns(corrColBuilder.build()).commit();
        // create index d_mm using minmax on foo(d)
        fooTable.updateIndexSpec().addIndex("d_mm", IndexType.MINMAX, "d", Collections.emptyMap()).commit();

        tableName = new SchemaTableName(SCHEMA, bar);
        Table barTable = IcebergUtil.loadIcebergTable(metastore, new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment)), SESSION, tableName);
        // alter table bar add correlated column (s) from join baz on bar.k2=baz.k2 with unique
        corrColBuilder = new CorrelatedColumns.Builder(barTable.schema(), false)
                .corrTableId(TableIdentifier.of(SCHEMA, baz))
                .joinType(JoinType.INNER)
                .leftKeyNames(Collections.singletonList("k2"))
                .rightKeys(Collections.singletonList("k2"))
                .joinConstraint(JoinConstraint.UNIQUE)
                .addColumn(1, "s", Types.StringType.get());
        barTable.updateCorrelatedColumnsSpec().addCorrelatedColumns(corrColBuilder.build()).commit();
        // create index s_mm using minmax on bar(s)
        barTable.updateIndexSpec().addIndex("s_mm", IndexType.MINMAX, "s", Collections.emptyMap()).commit();

        QualifiedObjectName completeBar = new QualifiedObjectName(CATALOG, SCHEMA, bar);
        IcebergColumnHandle dHandle = (IcebergColumnHandle) getColumnHandles(getQueryRunner().getDefaultSession(), completeBar).get("d");
        CorrelatedColumns.CorrelatedColumn dCorrCol = fooTable.correlatedColumnsSpec().getCorrelatedColumnsForAlias("d").flatMap(c -> c.getColumnByAlias("d")).get();
        IcebergColumnHandle dCorrColHandle = IcebergMetadata.toCorrColHandle(dHandle, dCorrCol);
        PlanMatchPattern matchFooTS = matchTableScan(foo, TupleDomain.withColumnDomains(
                ImmutableMap.of(dCorrColHandle, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 0.0d, false, 1.0d, false)), false))));

        QualifiedObjectName completeBaz = new QualifiedObjectName(CATALOG, SCHEMA, baz);
        IcebergColumnHandle sHandle = (IcebergColumnHandle) getColumnHandles(getQueryRunner().getDefaultSession(), completeBaz).get("s");
        CorrelatedColumns.CorrelatedColumn sCorrCol = barTable.correlatedColumnsSpec().getCorrelatedColumnsForAlias("s").flatMap(c -> c.getColumnByAlias("s")).get();
        IcebergColumnHandle sCorrColHandle = IcebergMetadata.toCorrColHandle(sHandle, sCorrCol);
        PlanMatchPattern matchBarTS = matchTableScan(bar, TupleDomain.withColumnDomains(ImmutableMap.of(sCorrColHandle, Domain.singleValue(VARCHAR, utf8Slice("a")))));

        @Language("SQL") String query = format("select * from %s join %s on %s.k1=%s.k1 join %s on %s.k2=%s.k2 where d>0 and d<1 and s='a'", foo, bar, foo, bar, baz, bar, baz);
        assertPlan(query, matchFooTS);
        assertPlan(query, matchBarTS);
    }

    @Test
    public void testPushDown()
            throws Exception
    {
        String foo = "foo" + randomTableSuffix();
        String bar = "bar" + randomTableSuffix();
        getQueryRunner().execute(format("create table %s (x, y) as select 1,1", foo));
        getQueryRunner().execute(format("create table %s (x, s, i) as select 1,'a',1", bar));

        SchemaTableName tableName = new SchemaTableName(SCHEMA, foo);
        Table fooTable = IcebergUtil.loadIcebergTable(metastore, new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment)), SESSION, tableName);
        // alter table foo add correlated column (s) from join bar on foo.x=bar.x with unique
        CorrelatedColumns.Builder corrColBuilder = new CorrelatedColumns.Builder(fooTable.schema(), false)
                .corrTableId(TableIdentifier.of(SCHEMA, bar))
                .joinType(JoinType.INNER)
                .leftKeyNames(Collections.singletonList("x"))
                .rightKeys(Collections.singletonList("x"))
                .joinConstraint(JoinConstraint.UNIQUE)
                .addColumn(1, "s", Types.StringType.get());
        fooTable.updateCorrelatedColumnsSpec().addCorrelatedColumns(corrColBuilder.build()).commit();

        QualifiedObjectName completeBar = new QualifiedObjectName(CATALOG, SCHEMA, bar);
        IcebergColumnHandle sHandle = (IcebergColumnHandle) getColumnHandles(getQueryRunner().getDefaultSession(), completeBar).get("s");
        CorrelatedColumns.CorrelatedColumn sCorrCol = fooTable.correlatedColumnsSpec().getCorrelatedColumnsForAlias("s").flatMap(c -> c.getColumnByAlias("s")).get();
        IcebergColumnHandle sCorrColHandle = IcebergMetadata.toCorrColHandle(sHandle, sCorrCol);

        PlanMatchPattern matchFooTS = matchTableScan(foo, TupleDomain.all());
        // won't push down because no index is defined on the correlated column
        assertPlan(format("select y from %s join %s on %s.x=%s.x and s='a'", foo, bar, foo, bar), matchFooTS);

        // create index s_mm using minmax on foo(s)
        fooTable.updateIndexSpec().addIndex("s_mm", IndexType.MINMAX, "s", Collections.emptyMap()).commit();

        matchFooTS = matchTableScan(foo, TupleDomain.withColumnDomains(ImmutableMap.of(sCorrColHandle, Domain.singleValue(VARCHAR, utf8Slice("a")))));
        // join order doesn't matter
        assertPlan(format("select y from %s join %s on %s.x=%s.x and s='a'", foo, bar, foo, bar), matchFooTS);
        assertPlan(format("select y from %s join %s on %s.x=%s.x and s='a'", bar, foo, bar, foo), matchFooTS);
        // won't push down if the feature is disabled
        getQueryRunner().getSessionPropertyManager().addRuntimeSystemSessionProperty("pushdown_corr_col_filters", "false");
        assertPlan(format("select y from %s join %s on %s.x=%s.x and s='a'", foo, bar, foo, bar), matchTableScan(foo, TupleDomain.all()));
        getQueryRunner().getSessionPropertyManager().removeRuntimeSystemSessionProperty("pushdown_corr_col_filters");
        assertPlan(format("select y from %s join %s on %s.x=%s.x and s='a'", bar, foo, foo, bar), matchFooTS);

        // won't push down if join type is different
        matchFooTS = matchTableScan(foo, TupleDomain.all());
        assertPlan(format("select y from %s left join %s on %s.x=%s.x and s='a'", foo, bar, foo, bar), matchFooTS);
        // won't push down if join key is different
        assertPlan(format("select y from %s join %s on %s.x=%s.i and s='a'", foo, bar, foo, bar), matchFooTS);

        String baz = "baz" + randomTableSuffix();
        getQueryRunner().execute(format("create table %s (k, x, y) as select 1,'a',cast(1.1 as double)", baz));
        // alter table foo add correlated column (x as corr_x, y as corr_y) from left join baz on foo.x=baz.k with pk_fk
        corrColBuilder = new CorrelatedColumns.Builder(fooTable.schema(), false)
                .corrTableId(TableIdentifier.of(SCHEMA, baz))
                .joinType(JoinType.LEFT)
                .leftKeyNames(Collections.singletonList("x"))
                .rightKeys(Collections.singletonList("k"))
                .joinConstraint(JoinConstraint.PK_FK)
                .addColumn(1, "x", Types.StringType.get(), "corr_x")
                .addColumn(2, "y", Types.DoubleType.get(), "corr_y");
        fooTable.updateCorrelatedColumnsSpec().addCorrelatedColumns(corrColBuilder.build()).commit();

        // create index corr_x_bf using bloomfilter on foo(corr_x)
        fooTable.updateIndexSpec().addIndex("corr_x_bf", IndexType.BLOOMFILTER, "corr_x", Collections.emptyMap()).commit();
        // create index corr_y_bm using bitmap on foo(corr_y)
        fooTable.updateIndexSpec().addIndex("corr_y_bm", IndexType.BITMAP, "corr_y", Collections.emptyMap()).commit();

        // won't push down because filter is on right table of a LEFT join
        assertPlan(format("select * from %s left join %s on %s.x=%s.k and %s.y>0", foo, baz, foo, baz, baz), matchFooTS);

        QualifiedObjectName completeBaz = new QualifiedObjectName(CATALOG, SCHEMA, baz);
        IcebergColumnHandle yHandle = (IcebergColumnHandle) getColumnHandles(getQueryRunner().getDefaultSession(), completeBaz).get("y");
        CorrelatedColumns.CorrelatedColumn yCorrCol = fooTable.correlatedColumnsSpec().getCorrelatedColumnsForAlias("corr_y").flatMap(c -> c.getColumnByAlias("corr_y")).get();
        IcebergColumnHandle yCorrColHandle = IcebergMetadata.toCorrColHandle(yHandle, yCorrCol);
        matchFooTS = matchTableScan(foo, TupleDomain.withColumnDomains(ImmutableMap.of(sCorrColHandle, Domain.singleValue(VARCHAR, utf8Slice("a")),
                yCorrColHandle, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 0.0d)), false))));
        // left join will be converted to inner join and will be supported because it's PK_FK join
        assertPlan(format("select * from %s join %s on %s.x=%s.x and s='a' left join %s on %s.x=%s.k where %s.y>0", foo, bar, foo, bar, baz, foo, baz, baz), matchFooTS);
    }

    @Override
    protected void assertPlan(String sql, PlanMatchPattern pattern)
    {
        LocalQueryRunner queryRunner = getQueryRunner();
        RuleStatsRecorder ruleStatsRecorder = new RuleStatsRecorder();
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true, ruleStatsRecorder);
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers, OPTIMIZED_AND_VALIDATED, WarningCollector.NOOP);
            try {
                PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            }
            catch (Throwable t) {
                System.out.println();
                throw new AssertionError(
                        String.format("Plan assertion error, rule stats: [%s]", ruleStatsRecorder.getStats().get(PushCorrColFilterIntoTableScan.class)), t);
            }
            return null;
        });
    }

    private PlanMatchPattern matchTableScan(String tableName, TupleDomain<IcebergColumnHandle> expectedCorrColDomain)
    {
        return anyTree(node(TableScanNode.class)
                .with(new TableHandleMatcher(table -> {
                    IcebergTableHandle icebergTableHandle = (IcebergTableHandle) table;
                    // we don't care about other tables
                    if (!icebergTableHandle.getTableName().equals(tableName)) {
                        return true;
                    }
                    TupleDomain<IcebergColumnHandle> corrColDomain = icebergTableHandle.getCorrColPredicate();
                    return corrColDomain.equals(expectedCorrColDomain);
                })));
    }

    private static class TableHandleMatcher
            implements Matcher
    {
        private final Predicate<ConnectorTableHandle> expectedTable;

        private TableHandleMatcher(Predicate<ConnectorTableHandle> expectedTable)
        {
            this.expectedTable = expectedTable;
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            checkState(shapeMatches(node));
            ConnectorTableHandle tableHandle = ((TableScanNode) node).getTable().getConnectorHandle();
            return new MatchResult(expectedTable.test(tableHandle));
        }
    }
}
