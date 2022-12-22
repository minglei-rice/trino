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

import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.hive.HiveApplyProjectionUtil;
import io.trino.plugin.hive.HiveApplyProjectionUtil.ProjectedColumnRepresentation;
import io.trino.plugin.hive.HiveWrittenPartitions;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.aggindex.AggFunctionDesc;
import io.trino.spi.aggindex.AggIndex;
import io.trino.spi.aggindex.CorrColumns;
import io.trino.spi.aggindex.TableColumnIdentify;
import io.trino.spi.connector.AggIndexApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.CorrColFilterApplicationResult;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.AggIndexFile;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CorrelatedColumns;
import org.apache.iceberg.CorrelatedColumns.CorrelatedColumn;
import org.apache.iceberg.CorrelatedColumns.Correlation;
import org.apache.iceberg.CorrelatedColumnsSpec;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IndexField;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.actions.WriteAggIndexUtils;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.cube.AggregationDesc;
import org.apache.iceberg.cube.AggregationIndex;
import org.apache.iceberg.cube.Functions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.hive.HiveApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.hive.util.HiveUtil.isStructuralType;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isComplexExpressionsOnPartitionKeysPushdownEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isReadIndicesSwitchOn;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isStatisticsEnabled;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.IcebergUtil.getIdentityPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getTableComment;
import static io.trino.plugin.iceberg.IcebergUtil.newCreateTableTransaction;
import static io.trino.plugin.iceberg.IcebergUtil.partitionMatchesConstraint;
import static io.trino.plugin.iceberg.IcebergUtil.toIcebergSchema;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TrinoHiveCatalog.DEPENDS_ON_TABLES;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class IcebergMetadata
        implements ConnectorMetadata
{
    private static final Logger LOG = Logger.get(IcebergMetadata.class);

    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoCatalog catalog;

    private final Map<String, Long> snapshotIds = new ConcurrentHashMap<>();

    private Transaction transaction;

    public IcebergMetadata(
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalog catalog)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listNamespaces(session);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return catalog.loadNamespaceMetadata(session, schemaName.getSchemaName());
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return catalog.getNamespacePrincipal(session, schemaName.getSchemaName());
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        verify(name.getTableType() == DATA, "Wrong table type: " + name.getTableNameWithType());

        Table table;
        try {
            table = catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), name.getTableName()));
        }
        catch (TableNotFoundException e) {
            return null;
        }
        Optional<Long> snapshotId = getSnapshotId(table, name.getSnapshotId());

        String nameMappingJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
        return new IcebergTableHandle(
                tableName.getSchemaName(),
                name.getTableName(),
                name.getTableType(),
                snapshotId,
                TupleDomain.all(),
                TupleDomain.all(),
                ImmutableSet.of(),
                Optional.ofNullable(nameMappingJson),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public List<AggIndex> getAggregationIndices(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        Table table;
        try {
            table = catalog.loadTable(session, icebergTableHandle.getSchemaTableName());
        }
        catch (TableNotFoundException e) {
            return List.of();
        }
        List<AggregationIndex> aggregationIndices = table.aggregationIndexSpec().aggIndex();
        return aggregationIndices.stream()
                .map(icebergAggIndex -> toTrinoAggIndex(icebergAggIndex, table, icebergTableHandle))
                .collect(toImmutableList());
    }

    @Override
    public Optional<AggIndexApplicationResult<ConnectorTableHandle>> applyAggIndex(
            ConnectorSession session,
            ConnectorTableHandle handle,
            AggIndex aggIndex)
    {
        if (aggIndex == null) {
            return Optional.empty();
        }
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) handle;
        Table table;
        try {
            table = catalog.loadTable(session, icebergTableHandle.getSchemaTableName());
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }
        Schema schema = table.schema();

        IcebergTableHandle newIcebergTableHandle = new IcebergTableHandle(
                icebergTableHandle.getSchemaName(),
                icebergTableHandle.getTableName(),
                icebergTableHandle.getTableType(),
                icebergTableHandle.getSnapshotId(),
                TupleDomain.all(),
                icebergTableHandle.getEnforcedPredicate(),
                icebergTableHandle.getProjectedColumns(),
                icebergTableHandle.getNameMappingJson(),
                icebergTableHandle.getCorrColPredicate(),
                icebergTableHandle.getStringPredicates(),
                Optional.of(aggIndex));

        Map<String, TableColumnIdentify> aggIndexFileColumnNameToColumnIdent = new HashMap<>();
        AggregationIndex aggregationIndex =
                table.aggregationIndexSpec().aggIndex()
                        .stream()
                        .filter(x -> x.getAggIndexId() == aggIndex.getAggIndexId())
                        .findFirst()
                        .orElse(null);
        if (aggregationIndex == null) {
            LOG.info("Can not find an AggIndex %s in connector, maybe it was dropped by some reasons.", aggIndex.getAggIndexId());
            return Optional.empty();
        }
        for (Integer dimColumnId : aggregationIndex.getDims().getColumnIds()) {
            Types.NestedField field = schema.findField(dimColumnId);
            if (field != null) {
                String tableName = icebergTableHandle.getTableName();
                TableColumnIdentify identify = new TableColumnIdentify(tableName, schema.findColumnName(dimColumnId));
                aggIndexFileColumnNameToColumnIdent.put(WriteAggIndexUtils.factDimCol(dimColumnId, schema), identify);
            }
            else {
                Optional<CorrelatedColumns> correlatedColumns = table.correlatedColumnsSpec().getCorrelatedColumnsForId(dimColumnId);
                if (correlatedColumns.isPresent()) {
                    String tableName = correlatedColumns.get().getCorrelation().getCorrTable().getId().name();
                    CorrelatedColumns corrColumns = correlatedColumns.get();
                    CorrelatedColumn correlatedColumn = corrColumns.getColumnById(dimColumnId).get();
                    String columnName = correlatedColumn.getName();
                    aggIndexFileColumnNameToColumnIdent.put(WriteAggIndexUtils.corrDimCol(correlatedColumn),
                            new TableColumnIdentify(tableName, columnName));
                }
            }
        }

        List<AggregationDesc> aggDesc = aggregationIndex.getAggDesc();
        for (AggregationDesc aggregationDesc : aggDesc) {
            Integer sourceColumnId = aggregationDesc.getSourceColumnId();
            TableColumnIdentify identify;
            if (sourceColumnId == null) {
                identify = null;
            }
            else {
                identify = new TableColumnIdentify(icebergTableHandle.getTableName(), schema.findColumnName(sourceColumnId));
            }
            aggIndexFileColumnNameToColumnIdent.put(WriteAggIndexUtils.aggExprAlias(aggregationDesc, schema).toLowerCase(Locale.ENGLISH), identify);
        }

        // verify data file whether it has cube file
        try (CloseableIterable<FileScanTask> fileScanTasks =
                     table.newScan()
                             .filter(toIcebergExpression(icebergTableHandle.getEnforcedPredicate()))
                             .includeAggIndexStats()
                             .planFiles()) {
            for (FileScanTask scanTask : fileScanTasks) {
                if (!scanTask.deletes().isEmpty()) {
                    return Optional.empty();
                }
                AggIndexFile aggIndexFile = DataFiles.getAggIndexFiles(scanTask.file())
                        .stream()
                        .filter(cubeFile -> cubeFile.getAggIndexId() == aggIndex.getAggIndexId())
                        .findAny()
                        .orElse(null);
                if (aggIndexFile == null) {
                    // data file does not have cube file
                    return Optional.empty();
                }
            }
        }
        catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return Optional.of(new AggIndexApplicationResult<>(newIcebergTableHandle, aggIndexFileColumnNameToColumnIdent));
    }

    /**
     * Transform iceberg aggregation index to trino aggregation index.
     * @param icebergAggIndex an iceberg aggregation index
     * @param table an iceberg table
     * @param icebergTableHandle iceberg table handle
     * @return Trino AggIndex
     */
    private AggIndex toTrinoAggIndex(AggregationIndex icebergAggIndex, Table table, IcebergTableHandle icebergTableHandle)
    {
        List<TableColumnIdentify> dimFields = new ArrayList<>();
        for (Integer dimColumnId : icebergAggIndex.getDims().getColumnIds()) {
            String name = table.schema().findColumnName(dimColumnId);
            // fact column
            if (name != null) {
                dimFields.add(new TableColumnIdentify(icebergTableHandle.getTableName(), name));
            }
            // dim column
            else {
                Optional<CorrelatedColumns> correlatedColumns = table.correlatedColumnsSpec().getCorrelatedColumnsForId(dimColumnId);
                CorrelatedColumns corrColumns = correlatedColumns.get();
                String tableName = corrColumns.getCorrelation().getCorrTable().getId().name();
                Optional<CorrelatedColumn> columnById = corrColumns.getColumnById(dimColumnId);
                String columnName = columnById.orElseThrow().getName();
                dimFields.add(new TableColumnIdentify(tableName, columnName));
            }
        }

        // aggregation function
        final Function<AggregationDesc, AggFunctionDesc> toTrinoAggFunc = (icebergAggFunc -> {
            String funcName = icebergAggFunc.getFunction().functionId().toString();
            Integer sourceColumnId = icebergAggFunc.getSourceColumnId();
            // count(*)
            if (sourceColumnId == null) {
                return new AggFunctionDesc(funcName, null, new ArrayList<>());
            }
            return new AggFunctionDesc(funcName, new TableColumnIdentify(icebergTableHandle.getTableName(),
                    table.schema().findColumnName(sourceColumnId)), getAggFunctionAttributes(icebergAggFunc.getFunction()));
        });

        Map<AggFunctionDesc, String> trinoAggFuncDescToName = icebergAggIndex.getAggDesc().stream()
                .collect(Collectors.toMap(toTrinoAggFunc, aggDesc -> WriteAggIndexUtils.aggExprAlias(aggDesc, table.schema())));

        Function<CorrelatedColumns.Correlation, CorrColumns.Corr> toTrinoCorrelation = icebergCorrelation -> {
            CorrelatedColumns.JoinType icebergJoinType = icebergCorrelation.getJoinType();
            CorrelatedColumns.JoinConstraint icebergJoinConstraint = icebergCorrelation.getJoinConstraint();

            List<TableColumnIdentify> leftKeysIdentify = icebergCorrelation.getLeftKeys().stream()
                    .map(joinKeyId -> new TableColumnIdentify(icebergTableHandle.getTableName(), table.schema().findColumnName(joinKeyId)))
                    .collect(toImmutableList());

            List<TableColumnIdentify> rightKeysIdentify =
                    icebergCorrelation.getRightKeys().stream()
                            .map(rightKey -> new TableColumnIdentify(icebergCorrelation.getCorrTable().getId().name(), rightKey))
                            .collect(toImmutableList());

            JoinType trinoJoinType;
            CorrColumns.Corr.JoinConstraint trinoJoinConstraint;
            switch (icebergJoinType) {
                case INNER:
                    trinoJoinType = JoinType.INNER;
                    break;
                case LEFT:
                    trinoJoinType = JoinType.LEFT_OUTER;
                    break;
                default:
                    throw new RuntimeException(String.format("Unsupported join type %s", icebergJoinType));
            }

            switch (icebergJoinConstraint) {
                case PK_FK:
                    trinoJoinConstraint = CorrColumns.Corr.JoinConstraint.PK_FK;
                    break;
                case UNIQUE:
                    trinoJoinConstraint = CorrColumns.Corr.JoinConstraint.UNIQUE;
                    break;
                case NONE:
                    trinoJoinConstraint = CorrColumns.Corr.JoinConstraint.NONE;
                    break;
                default:
                    throw new RuntimeException(String.format("Unsupported join constraint %s", icebergJoinConstraint));
            }
            return new CorrColumns.Corr(leftKeysIdentify, rightKeysIdentify, trinoJoinType, trinoJoinConstraint);
        };
        List<CorrColumns> corrColumns = table.correlatedColumnsSpec().getCorrelatedColumns().stream()
                .map(correlation -> new CorrColumns(toTrinoCorrelation.apply(correlation.getCorrelation())))
                .collect(toImmutableList());
        return new AggIndex(icebergAggIndex.getAggIndexId(), dimFields, trinoAggFuncDescToName, corrColumns);
    }

    private List<Object> getAggFunctionAttributes(org.apache.iceberg.cube.Function function)
    {
        List<Object> attributes = new ArrayList<>();
        switch (function.functionId()) {
            case PERCENTILE:
                attributes.add(((Functions.Percentile) function).getWeight());
                break;
            default:
                break;
        }
        return attributes;
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(session, tableName)
                .map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        if (name.getTableType() == DATA) {
            return Optional.empty();
        }

        // load the base table for the system table
        Table table;
        try {
            table = catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), name.getTableName()));
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }

        SchemaTableName systemTableName = new SchemaTableName(tableName.getSchemaName(), name.getTableNameWithType());
        switch (name.getTableType()) {
            case DATA:
                // Handled above.
                break;
            case HISTORY:
                if (name.getSnapshotId().isPresent()) {
                    throw new TrinoException(NOT_SUPPORTED, "Snapshot ID not supported for history table: " + systemTableName);
                }
                return Optional.of(new HistoryTable(systemTableName, table));
            case SNAPSHOTS:
                if (name.getSnapshotId().isPresent()) {
                    throw new TrinoException(NOT_SUPPORTED, "Snapshot ID not supported for snapshots table: " + systemTableName);
                }
                return Optional.of(new SnapshotsTable(systemTableName, typeManager, table));
            case PARTITIONS:
                return Optional.of(new PartitionTable(systemTableName, typeManager, table, getSnapshotId(table, name.getSnapshotId())));
            case MANIFESTS:
                return Optional.of(new ManifestsTable(systemTableName, table, getSnapshotId(table, name.getSnapshotId())));
            case FILES:
                return Optional.of(new FilesTable(systemTableName, typeManager, table, getSnapshotId(table, name.getSnapshotId())));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;

        if (table.getSnapshotId().isEmpty()) {
            // A table with missing snapshot id produces no splits, so we optimize here by returning
            // TupleDomain.none() as the predicate
            return new ConnectorTableProperties(TupleDomain.none(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of());
        }

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        // Extract identity partition fields that are present in all partition specs, for creating the discrete predicates.
        Set<Integer> partitionSourceIds = identityPartitionColumnsInAllSpecs(icebergTable);

        TupleDomain<IcebergColumnHandle> enforcedPredicate = table.getEnforcedPredicate();

        DiscretePredicates discretePredicates = null;
        if (!partitionSourceIds.isEmpty()) {
            // Extract identity partition columns
            Map<Integer, IcebergColumnHandle> columns = getColumns(icebergTable.schema(), typeManager).stream()
                    .filter(column -> partitionSourceIds.contains(column.getId()))
                    .collect(toImmutableMap(IcebergColumnHandle::getId, Function.identity()));

            Supplier<List<FileScanTask>> lazyFiles = Suppliers.memoize(() -> {
                TableScan tableScan = icebergTable.newScan()
                        .useSnapshot(table.getSnapshotId().get())
                        .filter(toIcebergExpression(enforcedPredicate))
                        .includeColumnStats();

                try (CloseableIterable<FileScanTask> iterator = tableScan.planFiles()) {
                    return ImmutableList.copyOf(iterator);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            Iterable<FileScanTask> files = () -> lazyFiles.get().iterator();

            Iterable<TupleDomain<ColumnHandle>> discreteTupleDomain = Iterables.transform(files, fileScan -> {
                // Extract partition values in the data file
                Map<Integer, Optional<String>> partitionColumnValueStrings = getPartitionKeys(fileScan);
                Map<ColumnHandle, NullableValue> partitionValues = partitionSourceIds.stream()
                        .filter(partitionColumnValueStrings::containsKey)
                        .collect(toImmutableMap(
                                columns::get,
                                columnId -> {
                                    IcebergColumnHandle column = columns.get(columnId);
                                    Object prestoValue = deserializePartitionValue(
                                            column.getType(),
                                            partitionColumnValueStrings.get(columnId).orElse(null),
                                            column.getName());

                                    return NullableValue.of(column.getType(), prestoValue);
                                }));

                return TupleDomain.fromFixedValues(partitionValues);
            });

            discretePredicates = new DiscretePredicates(
                    columns.values().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toImmutableList()),
                    discreteTupleDomain);
        }

        return new ConnectorTableProperties(
                // Using the predicate here directly avoids eagerly loading all partition values. Logically, this
                // still keeps predicate and discretePredicates evaluation the same on every row of the table. This
                // can be further optimized by intersecting with partition values at the cost of iterating
                // over all tableScan.planFiles() and caching partition values in table handle.
                enforcedPredicate.transformKeys(ColumnHandle.class::cast),
                // TODO: implement table partitioning
                Optional.empty(),
                Optional.empty(),
                Optional.ofNullable(discretePredicates),
                ImmutableList.of(),
                Optional.of(table.getUnenforcedPredicate().transformKeys(ColumnHandle.class::cast)));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(session, ((IcebergTableHandle) table).getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        if (table.getAggIndex().isPresent()) {
            AggregationIndex aggregationIndex =
                    icebergTable.aggregationIndexSpec().aggIndex()
                            .stream()
                            .filter(icebergAggIndex -> icebergAggIndex.getAggIndexId() == table.getAggIndex().get().getAggIndexId())
                            .findFirst()
                            .orElseThrow(() -> new RuntimeException("Can not find aggregation index."));
            // cube schema
            Schema schema = WriteAggIndexUtils.aggIndexSchema(
                    aggregationIndex, icebergTable.schema(), icebergTable.correlatedColumnsSpec());
            return getColumns(schema, typeManager).stream()
                    .collect(toImmutableMap(IcebergColumnHandle::getName, identity()));
        }
        // original table schema
        return getColumns(icebergTable.schema(), typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, identity()));
    }

    @Override
    public Map<String, ColumnHandle> getPartitionColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        Set<Integer> identityPartitionFields = identityPartitionColumnsInAllSpecs(icebergTable);
        return getColumns(icebergTable.schema(), typeManager)
                .stream()
                .filter(column -> identityPartitionFields.contains(column.getId()))
                .collect(toImmutableMap(IcebergColumnHandle::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setComment(column.getComment())
                .build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTable()
                .map(ignored -> singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName table : tables) {
            try {
                columns.put(table, getTableMetadata(session, table).getColumns());
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
            catch (UnknownTableTypeException e) {
                // ignore table of unknown type
            }
        }
        return columns.build();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        catalog.createNamespace(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        catalog.dropNamespace(session, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        catalog.renameNamespace(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String source, TrinoPrincipal principal)
    {
        catalog.setNamespacePrincipal(session, source, principal);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        catalog.updateTableComment(session, ((IcebergTableHandle) tableHandle).getSchemaTableName(), comment);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Schema schema = toIcebergSchema(tableMetadata.getColumns());
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));
        return getWriteLayout(schema, partitionSpec);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        verify(transaction == null, "transaction already set");
        transaction = newCreateTableTransaction(catalog, tableMetadata, session);
        return new IcebergWritableTableHandle(
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                SchemaParser.toJson(transaction.table().schema()),
                PartitionSpecParser.toJson(transaction.table().spec()),
                getColumns(transaction.table().schema(), typeManager),
                transaction.table().location(),
                getFileFormat(transaction.table()),
                transaction.table().properties());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsert(session, (IcebergWritableTableHandle) tableHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        return getWriteLayout(icebergTable.schema(), icebergTable.spec());
    }

    private Optional<ConnectorNewTableLayout> getWriteLayout(Schema tableSchema, PartitionSpec partitionSpec)
    {
        if (partitionSpec.isUnpartitioned()) {
            return Optional.empty();
        }

        Map<Integer, IcebergColumnHandle> columnById = getColumns(tableSchema, typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));

        List<IcebergColumnHandle> partitioningColumns = partitionSpec.fields().stream()
                .sorted(Comparator.comparing(PartitionField::sourceId))
                .map(field -> requireNonNull(columnById.get(field.sourceId()), () -> "Cannot find source column for partitioning field " + field))
                .distinct()
                .collect(toImmutableList());
        List<String> partitioningColumnNames = partitioningColumns.stream()
                .map(IcebergColumnHandle::getName)
                .collect(toImmutableList());

        if (partitionSpec.fields().stream().allMatch(field -> field.transform().isIdentity())) {
            // Do not set partitioningHandle, to let engine determine whether to repartition data or not, on stat-based basis.
            return Optional.of(new ConnectorNewTableLayout(partitioningColumnNames));
        }
        IcebergPartitioningHandle partitioningHandle = new IcebergPartitioningHandle(toPartitionFields(partitionSpec), partitioningColumns);
        return Optional.of(new ConnectorNewTableLayout(partitioningHandle, partitioningColumnNames));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        verify(transaction == null, "transaction already set");
        transaction = icebergTable.newTransaction();

        return new IcebergWritableTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), typeManager),
                icebergTable.location(),
                getFileFormat(icebergTable),
                icebergTable.properties());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;
        Table icebergTable = transaction.table();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newFastAppend();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.getPath())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withFormat(table.getFileFormat())
                    .withMetrics(task.getMetrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
        }

        appendFiles.commit();
        transaction.commitTransaction();
        transaction = null;

        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getPath)
                .collect(toImmutableList())));
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new IcebergColumnHandle(primitiveColumnIdentity(0, "$row_id"), BIGINT, ImmutableList.of(), BIGINT, Optional.empty());
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        return Optional.of(new IcebergInputInfo(table.getSnapshotId()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        catalog.dropTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName(), true);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        catalog.renameTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName(), newTable);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        icebergTable.updateSchema().addColumn(column.getName(), toIcebergType(column.getType()), column.getComment()).commit();
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        icebergTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) source;
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        icebergTable.updateSchema().renameColumn(columnHandle.getName(), target).commit();
    }

    /**
     * @throws TableNotFoundException when table cannot be found
     */
    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName table)
    {
        Table icebergTable = catalog.loadTable(session, table);

        List<ColumnMetadata> columns = getColumnMetadatas(icebergTable);

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, getFileFormat(icebergTable));
        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        return new ConnectorTableMetadata(table, columns, properties.build(), getTableComment(icebergTable));
    }

    private List<ColumnMetadata> getColumnMetadatas(Table table)
    {
        return table.schema().columns().stream()
                .map(column -> {
                    return ColumnMetadata.builder()
                            .setName(column.name())
                            .setType(toTrinoType(column.type(), typeManager))
                            .setNullable(column.isOptional())
                            .setComment(Optional.ofNullable(column.doc()))
                            .build();
                })
                .collect(toImmutableList());
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        catalog.createView(session, viewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        catalog.renameView(session, source, target);
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        catalog.setViewPrincipal(session, viewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.getViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getView(session, viewName);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;

        Table icebergTable = catalog.loadTable(session, handle.getSchemaTableName());

        icebergTable.newDelete()
                .deleteFromRowFilter(toIcebergExpression(handle.getEnforcedPredicate()))
                .commit();

        // TODO: it should be possible to return number of deleted records
        return OptionalLong.empty();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }

    @Override
    public boolean supportsPruningWithPredicateExpression(ConnectorSession session, ConnectorTableHandle handle)
    {
        return isComplexExpressionsOnPartitionKeysPushdownEnabled(session);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        Set<Integer> partitionSourceIds = identityPartitionColumnsInAllSpecs(icebergTable);
        BiPredicate<IcebergColumnHandle, Domain> isIdentityPartition = (column, domain) -> partitionSourceIds.contains(column.getId());

        TupleDomain<IcebergColumnHandle> newEnforcedConstraint = constraint.getSummary()
                .transformKeys(IcebergColumnHandle.class::cast)
                .filter(isIdentityPartition)
                .intersect(table.getEnforcedPredicate());

        TupleDomain<IcebergColumnHandle> remainingConstraint = constraint.getSummary()
                .transformKeys(IcebergColumnHandle.class::cast)
                .filter(isIdentityPartition.negate());

        TupleDomain<IcebergColumnHandle> newUnenforcedConstraint = remainingConstraint
                // TODO: Remove after completing https://github.com/trinodb/trino/issues/8759
                // Only applies to the unenforced constraint because structural types cannot be partition keys
                .filter((columnHandle, predicate) -> !isStructuralType(columnHandle.getType()))
                .intersect(table.getUnenforcedPredicate());

        if (constraint.getEvaluator().isEmpty() && newEnforcedConstraint.equals(table.getEnforcedPredicate())
                && newUnenforcedConstraint.equals(table.getUnenforcedPredicate())
                && constraint.getStringPredicates().equals(table.getStringPredicates())) {
            return Optional.empty();
        }

        // Constraint has an evaluator supplier implies Iceberg should push it down to table scan node
        // along with the summary.
        Optional<BiPredicate<PartitionSpec, StructLike>> enforcedEvaluator;
        // Considering the supplier is constructed from the remaining expression which could not be
        // translated into tuple domains in each iteration of filter applying, it's safe to override
        // the previous enforced evaluator.
        enforcedEvaluator = constraint.getEvaluator()
                .map(supplier -> new EnforcedEvaluator(icebergTable.schema(), icebergTable.spec(), typeManager, supplier.get()))
                .map(evaluator -> (BiPredicate<PartitionSpec, StructLike>) evaluator)
                .or(table::getEnforcedEvaluator);
        return Optional.of(new ConstraintApplicationResult<>(
                new IcebergTableHandle(
                        table.getSchemaName(),
                        table.getTableName(),
                        table.getTableType(),
                        table.getSnapshotId(),
                        newUnenforcedConstraint,
                        newEnforcedConstraint,
                        table.getProjectedColumns(),
                        table.getNameMappingJson(),
                        table.getCorrColPredicate(),
                        enforcedEvaluator,
                        constraint.getStringPredicates(),
                        table.getAggIndex()),
                remainingConstraint.transformKeys(ColumnHandle.class::cast),
                false));
    }

    static class EnforcedEvaluator
            implements BiPredicate<PartitionSpec, StructLike>
    {
        Schema schema;
        PartitionSpec spec;
        TypeManager typeManager;
        Constraint constraint;

        public EnforcedEvaluator(Schema schema, PartitionSpec spec, TypeManager typeManager, Constraint constraint)
        {
            this.schema = schema;
            this.spec = spec;
            this.typeManager = typeManager;
            this.constraint = constraint;
        }

        @Override
        public boolean test(PartitionSpec spec, StructLike partition)
        {
            Map<Integer, Optional<String>> partitionKeys = getIdentityPartitionKeys(spec, partition);
            Set<Integer> identityPartitionFieldIds = getIdentityPartitions(spec).keySet().stream()
                    .map(PartitionField::sourceId)
                    .collect(toImmutableSet());
            Set<IcebergColumnHandle> identityPartitionColumns = getColumns(schema, typeManager).stream()
                    .filter(column -> identityPartitionFieldIds.contains(column.getId()))
                    .collect(toImmutableSet());
            Supplier<Map<ColumnHandle, NullableValue>> partitionValues = memoize(() -> {
                Map<ColumnHandle, NullableValue> bindings = new HashMap<>();
                for (IcebergColumnHandle partitionColumn : identityPartitionColumns) {
                    Object partitionValue = deserializePartitionValue(
                            partitionColumn.getType(),
                            partitionKeys.get(partitionColumn.getId()).orElse(null),
                            partitionColumn.getName());
                    NullableValue bindingValue = new NullableValue(partitionColumn.getType(), partitionValue);
                    bindings.put(partitionColumn, bindingValue);
                }
                return bindings;
            });
            return partitionMatchesConstraint(identityPartitionColumns, partitionValues, constraint);
        }
    }

    @Override
    public Optional<CorrColFilterApplicationResult<ConnectorTableHandle>> applyCorrColFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            ConnectorTableHandle corrTable,
            JoinType joinType,
            List<JoinCondition> joinConditions,
            Map<String, ColumnHandle> tableAssignments,
            Map<String, ColumnHandle> corrTableAssignments,
            boolean tableIsLeft,
            Constraint corrColConstraint)
    {
        LOG.info("Trying to push down corr col filter %s", corrColConstraint.getSummary());
        // TODO: support non-iceberg correlated table
        if (!(corrTable instanceof IcebergTableHandle)) {
            LOG.info("Skip push down corr col filter because correlated table is not an iceberg table");
            return Optional.empty();
        }
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) table;
        Table icebergTable = catalog.loadTable(session, icebergTableHandle.getSchemaTableName());
        CorrelatedColumnsSpec corrColSpec = icebergTable.correlatedColumnsSpec();
        if (corrColSpec.isEmptySpec()) {
            LOG.info("Skip push down corr col filter because no correlated column is defined");
            return Optional.empty();
        }
        // it's not correct to push filters on the right table to the left table in a LEFT join
        if (joinType != JoinType.INNER) {
            LOG.info("Skip push down corr col filter because join type (%s) not supported", joinType);
            return Optional.empty();
        }
        TableIdentifier corrTableId = TableIdentifier.of(((IcebergTableHandle) corrTable).getSchemaName(), ((IcebergTableHandle) corrTable).getTableName());
        TupleDomain<IcebergColumnHandle> corrColDomain = corrColConstraint.getSummary().transformKeys(IcebergColumnHandle.class::cast);
        TupleDomain<IcebergColumnHandle> pushableDomain = icebergTableHandle.getCorrColPredicate();
        Set<Integer> colsWithIndex = icebergTable.indexSpec().fields().stream().map(IndexField::sourceId).collect(Collectors.toSet());
        for (CorrelatedColumns corrCols : corrColSpec.getCorrelatedColumns()) {
            Correlation correlation = corrCols.getCorrelation();
            // TODO: filter on dimension table may cause the join type being changed from LEFT to INNER, need take that into consideration
            if ((joinType != toTrinoJoinType(correlation.getJoinType()) && correlation.getJoinConstraint() != CorrelatedColumns.JoinConstraint.PK_FK) ||
                    !corrTableId.equals(correlation.getCorrTable().getId()) ||
                    !sameJoinCondition(joinConditions, correlation, icebergTable, tableAssignments, corrTableAssignments, tableIsLeft)) {
                LOG.info("CorrelatedColumns %s doesn't match because join is not same", corrCols);
                continue;
            }
            Map<String, CorrelatedColumn> nameToCorrCol = corrCols.getColumns().stream().collect(Collectors.toMap(CorrelatedColumn::getName, c -> c));
            TupleDomain<IcebergColumnHandle> matchedDomain = corrColDomain.filter((handle, domain) -> nameToCorrCol.containsKey(handle.getName()));
            if (matchedDomain.isAll()) {
                LOG.info("CorrelatedColumns %s doesn't match because filter doesn't contain its columns", corrCols);
                continue;
            }
            matchedDomain = matchedDomain
                    .transformKeys(srcHandle -> toCorrColHandle(srcHandle, nameToCorrCol.get(srcHandle.getName())))
                    .filter((handle, domain) -> colsWithIndex.contains(handle.getId()));
            if (matchedDomain.isAll()) {
                LOG.info("CorrelatedColumns %s doesn't match because no index is defined on the columns", corrCols);
                continue;
            }
            pushableDomain = pushableDomain.intersect(matchedDomain);
        }
        if (pushableDomain.equals(icebergTableHandle.getCorrColPredicate())) {
            LOG.info("Skip push down corr col filter because new filter is same as current one");
            return Optional.empty();
        }
        LOG.info("Pushed corr col filter %s into table scan", corrColConstraint.getSummary());
        return Optional.of(new CorrColFilterApplicationResult<>(
                new IcebergTableHandle(
                        icebergTableHandle.getSchemaName(),
                        icebergTableHandle.getTableName(),
                        icebergTableHandle.getTableType(),
                        icebergTableHandle.getSnapshotId(),
                        icebergTableHandle.getUnenforcedPredicate(),
                        icebergTableHandle.getEnforcedPredicate(),
                        icebergTableHandle.getProjectedColumns(),
                        icebergTableHandle.getNameMappingJson(),
                        pushableDomain,
                        icebergTableHandle.getStringPredicates(),
                        icebergTableHandle.getAggIndex())));
    }

    // converts column handle in correlated table to correlated column in left table
    public static IcebergColumnHandle toCorrColHandle(IcebergColumnHandle srcHandle, CorrelatedColumn corrCol)
    {
        ColumnIdentity newIdentity = new ColumnIdentity(corrCol.getId(), corrCol.getAlias(), srcHandle.getBaseColumnIdentity().getTypeCategory(),
                srcHandle.getBaseColumnIdentity().getChildren());
        return new IcebergColumnHandle(newIdentity, srcHandle.getBaseType(), srcHandle.getPath(), srcHandle.getType(), srcHandle.getComment());
    }

    private static boolean sameJoinCondition(List<JoinCondition> joinConditions, Correlation correlation, Table icebergTable, Map<String, ColumnHandle> tableAssignments,
            Map<String, ColumnHandle> corrTableAssignments, boolean isLeft)
    {
        if (correlation.getLeftKeys().size() != joinConditions.size()) {
            return false;
        }
        List<String> leftKeys = correlation.getLeftKeys().stream().map(id -> icebergTable.schema().findColumnName(id)).collect(toImmutableList());
        List<String> rightKeys = correlation.getRightKeys();
        for (JoinCondition condition : joinConditions) {
            if (condition.getOperator() != JoinCondition.Operator.EQUAL || !(condition.getLeftExpression() instanceof Variable) ||
                    !(condition.getRightExpression() instanceof Variable)) {
                return false;
            }
            // swap the keys if iceberg table is not left (can happen in case of inner join)
            Variable leftVar = (Variable) (isLeft ? condition.getLeftExpression() : condition.getRightExpression());
            Variable rightVar = (Variable) (isLeft ? condition.getRightExpression() : condition.getLeftExpression());
            IcebergColumnHandle leftHandle = (IcebergColumnHandle) tableAssignments.get(leftVar.getName());
            IcebergColumnHandle rightHandle = (IcebergColumnHandle) corrTableAssignments.get(rightVar.getName());
            if (leftHandle == null || rightHandle == null) {
                return false;
            }
            int index = leftKeys.indexOf(leftHandle.getName());
            if (index == -1 || !rightKeys.get(index).equals(rightHandle.getName())) {
                return false;
            }
        }
        return true;
    }

    private static JoinType toTrinoJoinType(CorrelatedColumns.JoinType icebergJoinType)
    {
        switch (icebergJoinType) {
            case LEFT:
                return JoinType.LEFT_OUTER;
            case INNER:
                return JoinType.INNER;
            default:
                throw new TrinoException(NOT_SUPPORTED, "Unknown correlation join type " + icebergJoinType.name());
        }
    }

    private static Set<Integer> identityPartitionColumnsInAllSpecs(Table table)
    {
        // Extract identity partition column source ids common to ALL specs
        return table.spec().fields().stream()
                .filter(field -> field.transform().isIdentity())
                .filter(field -> table.specs().values().stream().allMatch(spec -> spec.fields().contains(field)))
                .map(PartitionField::sourceId)
                .collect(toImmutableSet());
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (!isProjectionPushdownEnabled(session)) {
            return Optional.empty();
        }

        // Create projected column representations for supported sub expressions. Simple column references and chain of
        // dereferences on a variable are supported right now.
        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(Function.identity(), HiveApplyProjectionUtil::createProjectedColumnRepresentation));

        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) handle;

        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<IcebergColumnHandle> projectedColumns = assignments.values().stream()
                    .map(IcebergColumnHandle.class::cast)
                    .collect(toImmutableSet());
            if (icebergTableHandle.getProjectedColumns().equals(projectedColumns)) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((IcebergColumnHandle) assignment.getValue()).getType()))
                    .collect(toImmutableList());

            return Optional.of(new ProjectionApplicationResult<>(
                    icebergTableHandle.withProjectedColumns(projectedColumns),
                    projections,
                    assignmentsList,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<IcebergColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Map.Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            IcebergColumnHandle baseColumnHandle = (IcebergColumnHandle) assignments.get(projectedColumn.getVariable().getName());
            IcebergColumnHandle projectedColumnHandle = createProjectedColumnHandle(baseColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType());
            String projectedColumnName = projectedColumnHandle.getQualifiedName();

            Variable projectedColumnVariable = new Variable(projectedColumnName, expression.getType());
            Assignment newAssignment = new Assignment(projectedColumnName, projectedColumnHandle, expression.getType());
            newAssignments.putIfAbsent(projectedColumnName, newAssignment);

            newVariablesBuilder.put(expression, projectedColumnVariable);
            projectedColumnsBuilder.add(projectedColumnHandle);
        }

        // Modify projections to refer to new variables
        Map<ConnectorExpression, Variable> newVariables = newVariablesBuilder.build();
        List<ConnectorExpression> newProjections = projections.stream()
                .map(expression -> replaceWithNewVariables(expression, newVariables))
                .collect(toImmutableList());

        List<Assignment> outputAssignments = newAssignments.values().stream().collect(toImmutableList());
        return Optional.of(new ProjectionApplicationResult<>(
                icebergTableHandle.withProjectedColumns(projectedColumnsBuilder.build()),
                newProjections,
                outputAssignments,
                false));
    }

    /**
     * Determine if there is a filter on a table. For partition table require at least one partition filter,
     * for non-partitioned table no filter is required.
     */
    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        if (icebergTable.spec().isUnpartitioned()) {
            return;
        }
        if (IcebergSessionProperties.isFilterOnPartitionTable(session)
                && table.getEnforcedPredicate().equals(TupleDomain.all())) {
            throw new TrinoException(
                    StandardErrorCode.QUERY_REJECTED,
                    format("Filter required on %s.%s for at least one partition column, if you actually want query run without partition filter, " +
                            "please set session query_partition_filter_required to false.",
                            table.getSchemaName(),
                            table.getTableName()));
        }
    }

    private static IcebergColumnHandle createProjectedColumnHandle(IcebergColumnHandle column, List<Integer> indices, io.trino.spi.type.Type projectedColumnType)
    {
        ImmutableList.Builder<Integer> fullPath = ImmutableList.builder();
        fullPath.addAll(column.getPath());

        ColumnIdentity projectedColumnIdentity = column.getColumnIdentity();
        for (int index : indices) {
            // Position based lookup, not FieldId based
            projectedColumnIdentity = projectedColumnIdentity.getChildren().get(index);
            fullPath.add(projectedColumnIdentity.getId());
        }

        return new IcebergColumnHandle(
                column.getBaseColumnIdentity(),
                column.getBaseType(),
                fullPath.build(),
                projectedColumnType,
                Optional.empty());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }

        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, handle.getSchemaTableName());
        return TableStatisticsMaker.getTableStatistics(typeManager, constraint, handle, icebergTable);
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        catalog.setTablePrincipal(session, tableName, principal);
    }

    private Optional<Long> getSnapshotId(Table table, Optional<Long> snapshotId)
    {
        // table.name() is an encoded version of SchemaTableName
        return snapshotId
                .map(id ->
                        snapshotIds.computeIfAbsent(
                                table.name() + "@" + id,
                                ignored -> IcebergUtil.resolveSnapshotId(table, id)))
                .or(() -> Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId));
    }

    public Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return catalog.loadTable(session, schemaTableName);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        catalog.createMaterializedView(session, viewName, definition, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.dropMaterializedView(session, viewName);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return false;
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        verify(transaction == null, "transaction already set");
        transaction = icebergTable.newTransaction();

        return new IcebergWritableTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), typeManager),
                icebergTable.location(),
                getFileFormat(icebergTable),
                icebergTable.properties());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles)
    {
        // delete before insert .. simulating overwrite
        executeDelete(session, tableHandle);

        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;

        Table icebergTable = transaction.table();
        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newFastAppend();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.getPath())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withFormat(table.getFileFormat())
                    .withMetrics(task.getMetrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
        }

        String dependencies = sourceTableHandles.stream()
                .map(handle -> (IcebergTableHandle) handle)
                .filter(handle -> handle.getSnapshotId().isPresent())
                .map(handle -> handle.getSchemaTableName() + "=" + handle.getSnapshotId().get())
                .collect(joining(","));

        // Update the 'dependsOnTables' property that tracks tables on which the materialized view depends and the corresponding snapshot ids of the tables
        appendFiles.set(DEPENDS_ON_TABLES, dependencies);
        appendFiles.commit();

        transaction.commitTransaction();
        transaction = null;
        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getPath)
                .collect(toImmutableList())));
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listMaterializedViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getMaterializedView(session, viewName);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // TODO (https://github.com/trinodb/trino/issues/9594) support rename across schemas
        if (!source.getSchemaName().equals(target.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "Materialized View rename across schemas is not supported");
        }
        catalog.renameMaterializedView(session, source, target);
    }

    @Override
    public boolean supportsPruningStringPredicate(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return isReadIndicesSwitchOn(session);
    }

    public Optional<TableToken> getTableToken(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        return Optional.ofNullable(icebergTable.currentSnapshot())
                .map(snapshot -> new TableToken(snapshot.snapshotId()));
    }

    public boolean isTableCurrent(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<TableToken> tableToken)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Optional<TableToken> currentToken = getTableToken(session, handle);

        if (tableToken.isEmpty() || currentToken.isEmpty()) {
            return false;
        }

        return tableToken.get().getSnapshotId() == currentToken.get().getSnapshotId();
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName)
    {
        Map<String, Optional<TableToken>> refreshStateMap = getMaterializedViewToken(session, materializedViewName);
        if (refreshStateMap.isEmpty()) {
            return new MaterializedViewFreshness(false);
        }

        for (Map.Entry<String, Optional<TableToken>> entry : refreshStateMap.entrySet()) {
            List<String> strings = Splitter.on(".").splitToList(entry.getKey());
            if (strings.size() == 3) {
                strings = strings.subList(1, 3);
            }
            else if (strings.size() != 2) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, String.format("Invalid table name in '%s' property: %s'", DEPENDS_ON_TABLES, strings));
            }
            String schema = strings.get(0);
            String name = strings.get(1);
            SchemaTableName schemaTableName = new SchemaTableName(schema, name);
            IcebergTableHandle tableHandle = getTableHandle(session, schemaTableName);

            if (tableHandle == null) {
                throw new MaterializedViewNotFoundException(materializedViewName);
            }
            if (!isTableCurrent(session, tableHandle, entry.getValue())) {
                return new MaterializedViewFreshness(false);
            }
        }
        return new MaterializedViewFreshness(true);
    }

    private Map<String, Optional<TableToken>> getMaterializedViewToken(ConnectorSession session, SchemaTableName name)
    {
        Map<String, Optional<TableToken>> viewToken = new HashMap<>();
        Optional<ConnectorMaterializedViewDefinition> materializedViewDefinition = getMaterializedView(session, name);
        if (materializedViewDefinition.isEmpty()) {
            return viewToken;
        }

        SchemaTableName storageTableName = materializedViewDefinition.get().getStorageTable()
                .map(CatalogSchemaTableName::getSchemaTableName)
                .orElseThrow(() -> new IllegalStateException("Storage table missing in definition of materialized view " + name));

        Table icebergTable = catalog.loadTable(session, storageTableName);
        String dependsOnTables = icebergTable.currentSnapshot().summary().getOrDefault(DEPENDS_ON_TABLES, "");
        if (!dependsOnTables.isEmpty()) {
            Map<String, String> tableToSnapshotIdMap = Splitter.on(',').withKeyValueSeparator('=').split(dependsOnTables);
            for (Map.Entry<String, String> entry : tableToSnapshotIdMap.entrySet()) {
                viewToken.put(entry.getKey(), Optional.of(new TableToken(Long.parseLong(entry.getValue()))));
            }
        }
        return viewToken;
    }

    private static class TableToken
    {
        // Current Snapshot ID of the table
        private long snapshotId;

        public TableToken(long snapshotId)
        {
            this.snapshotId = snapshotId;
        }

        public long getSnapshotId()
        {
            return this.snapshotId;
        }
    }
}
