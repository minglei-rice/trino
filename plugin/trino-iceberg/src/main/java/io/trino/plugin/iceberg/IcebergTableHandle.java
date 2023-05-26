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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.spi.aggindex.AggIndex;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Can work in two modes, If there is an available AggIndex to answer, use AggIndexFile to response.
 * If there is no AggIndex to find, use on site computation to response.
 */
public class IcebergTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final TableType tableType;
    private final Optional<Long> snapshotId;
    private final String tableSchemaJson;
    // Empty means the partitioning spec is not known (can be the case for certain time travel queries).
    private final Optional<String> partitionSpecJson;
    private final int formatVersion;
    private final String tableLocation;
    private final Map<String, String> storageProperties;
    private final RetryMode retryMode;

    // UPDATE only
    private final List<IcebergColumnHandle> updatedColumns;

    // Filter used during split generation and table scan, but not required to be strictly enforced by Iceberg Connector
    private final TupleDomain<IcebergColumnHandle> unenforcedPredicate;

    // Filter guaranteed to be enforced by Iceberg connector
    private final TupleDomain<IcebergColumnHandle> enforcedPredicate;

    private final Set<IcebergColumnHandle> projectedColumns;
    private final Optional<String> nameMappingJson;

    // OPTIMIZE only. Coordinator-only
    private final boolean recordScannedFiles;
    private final Optional<DataSize> maxScannedFileSize;
    private final ConnectorExpression connectorExpression;
    private final Map<String, IcebergColumnHandle> conExprAssignments;
    private final TupleDomain<IcebergColumnHandle> corrColPredicate;
    private final List<IcebergTableHandle> corrTables;
    private final Optional<AggIndex> aggIndex;
    private final Set<ColumnHandle> constraintColumns;

    private boolean readPartialFiles;
    // used for tableHandle without agg index file
    private int aggIndexId;

    @JsonCreator
    public static IcebergTableHandle fromJsonForDeserializationOnly(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("tableSchemaJson") String tableSchemaJson,
            @JsonProperty("partitionSpecJson") Optional<String> partitionSpecJson,
            @JsonProperty("formatVersion") int formatVersion,
            @JsonProperty("unenforcedPredicate") TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            @JsonProperty("enforcedPredicate") TupleDomain<IcebergColumnHandle> enforcedPredicate,
            @JsonProperty("projectedColumns") Set<IcebergColumnHandle> projectedColumns,
            @JsonProperty("nameMappingJson") Optional<String> nameMappingJson,
            @JsonProperty("tableLocation") String tableLocation,
            @JsonProperty("storageProperties") Map<String, String> storageProperties,
            @JsonProperty("retryMode") RetryMode retryMode,
            @JsonProperty("updatedColumns") List<IcebergColumnHandle> updatedColumns,
            @JsonProperty("corrColPredicate") TupleDomain<IcebergColumnHandle> corrColPredicate,
            @JsonProperty("aggIndex") Optional<AggIndex> aggIndex,
            @JsonProperty("connectorExpressions") ConnectorExpression connectorExpression,
            @JsonProperty("connectorAssignments") Map<String, IcebergColumnHandle> conExprAssignments,
            @JsonProperty("readPartialFiles") boolean readPartialFiles,
            @JsonProperty("aggIndexId") int aggIndexId)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                retryMode,
                updatedColumns,
                false,
                Optional.empty(),
                corrColPredicate,
                Collections.emptyList(),
                aggIndex,
                Collections.emptySet(),
                connectorExpression,
                conExprAssignments,
                readPartialFiles,
                aggIndexId);
    }

    public IcebergTableHandle(
            String schemaName,
            String tableName,
            TableType tableType,
            Optional<Long> snapshotId,
            String tableSchemaJson,
            Optional<String> partitionSpecJson,
            int formatVersion,
            TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            TupleDomain<IcebergColumnHandle> enforcedPredicate,
            Set<IcebergColumnHandle> projectedColumns,
            Optional<String> nameMappingJson,
            String tableLocation,
            Map<String, String> storageProperties,
            RetryMode retryMode,
            List<IcebergColumnHandle> updatedColumns,
            boolean recordScannedFiles,
            Optional<DataSize> maxScannedFileSize,
            TupleDomain<IcebergColumnHandle> corrColPredicate,
            Optional<AggIndex> aggIndex,
            Set<ColumnHandle> constraintColumns,
            boolean readPartialFiles,
            int aggIndexId)
    {
        this(schemaName, tableName, tableType, snapshotId, tableSchemaJson, partitionSpecJson, formatVersion, unenforcedPredicate, enforcedPredicate, projectedColumns, nameMappingJson,
                tableLocation, storageProperties, retryMode, updatedColumns, recordScannedFiles, maxScannedFileSize, corrColPredicate, Collections.emptyList(), aggIndex, constraintColumns, null, Collections.emptyMap(), readPartialFiles, aggIndexId);
    }

    public IcebergTableHandle(
            String schemaName,
            String tableName,
            TableType tableType,
            Optional<Long> snapshotId,
            String tableSchemaJson,
            Optional<String> partitionSpecJson,
            int formatVersion,
            TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            TupleDomain<IcebergColumnHandle> enforcedPredicate,
            Set<IcebergColumnHandle> projectedColumns,
            Optional<String> nameMappingJson,
            String tableLocation,
            Map<String, String> storageProperties,
            RetryMode retryMode,
            List<IcebergColumnHandle> updatedColumns,
            boolean recordScannedFiles,
            Optional<DataSize> maxScannedFileSize,
            TupleDomain<IcebergColumnHandle> corrColPredicate,
            List<IcebergTableHandle> corrTables,
            Optional<AggIndex> aggIndex,
            Set<ColumnHandle> constraintColumns,
            ConnectorExpression connectorExpression,
            Map<String, IcebergColumnHandle> conExprAssignments,
            boolean readPartialFiles,
            int aggIndexId)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.tableSchemaJson = requireNonNull(tableSchemaJson, "schemaJson is null");
        this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        this.formatVersion = formatVersion;
        this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
        this.enforcedPredicate = requireNonNull(enforcedPredicate, "enforcedPredicate is null");
        this.projectedColumns = ImmutableSet.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.nameMappingJson = requireNonNull(nameMappingJson, "nameMappingJson is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.storageProperties = ImmutableMap.copyOf(requireNonNull(storageProperties, "storageProperties is null"));
        this.retryMode = requireNonNull(retryMode, "retryMode is null");
        this.updatedColumns = ImmutableList.copyOf(requireNonNull(updatedColumns, "updatedColumns is null"));
        this.recordScannedFiles = recordScannedFiles;
        this.maxScannedFileSize = requireNonNull(maxScannedFileSize, "maxScannedFileSize is null");
        this.corrColPredicate = requireNonNull(corrColPredicate, "corrColPredicate is null");
        this.corrTables = requireNonNull(corrTables, "corrTableSnapshots is null");
        this.aggIndex = aggIndex;
        this.constraintColumns = constraintColumns;
        this.connectorExpression = connectorExpression;
        this.conExprAssignments = conExprAssignments;
        this.readPartialFiles = readPartialFiles;
        this.aggIndexId = aggIndexId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public String getTableSchemaJson()
    {
        return tableSchemaJson;
    }

    @JsonProperty
    public Optional<String> getPartitionSpecJson()
    {
        return partitionSpecJson;
    }

    @JsonProperty
    public int getFormatVersion()
    {
        return formatVersion;
    }

    @JsonProperty
    public TupleDomain<IcebergColumnHandle> getUnenforcedPredicate()
    {
        return unenforcedPredicate;
    }

    @JsonProperty
    public TupleDomain<IcebergColumnHandle> getEnforcedPredicate()
    {
        return enforcedPredicate;
    }

    @JsonProperty
    public Set<IcebergColumnHandle> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonIgnore
    public Set<ColumnHandle> getConstraintColumns()
    {
        return constraintColumns;
    }

    @JsonProperty
    public Optional<String> getNameMappingJson()
    {
        return nameMappingJson;
    }

    @JsonProperty
    public String getTableLocation()
    {
        return tableLocation;
    }

    @JsonProperty
    public Map<String, String> getStorageProperties()
    {
        return storageProperties;
    }

    @JsonProperty
    public RetryMode getRetryMode()
    {
        return retryMode;
    }

    @JsonProperty
    public List<IcebergColumnHandle> getUpdatedColumns()
    {
        return updatedColumns;
    }

    @JsonIgnore
    public boolean isRecordScannedFiles()
    {
        return recordScannedFiles;
    }

    @JsonIgnore
    public Optional<DataSize> getMaxScannedFileSize()
    {
        return maxScannedFileSize;
    }

    @JsonProperty
    public TupleDomain<IcebergColumnHandle> getCorrColPredicate()
    {
        return corrColPredicate;
    }

    @JsonIgnore
    public List<IcebergTableHandle> getCorrTables()
    {
        return corrTables;
    }

    @JsonIgnore
    public ConnectorExpression getConnectorExpression()
    {
        return this.connectorExpression;
    }

    @JsonIgnore
    public Map<String, IcebergColumnHandle> getConExprAssignments()
    {
        return this.conExprAssignments;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public SchemaTableName getSchemaTableNameWithType()
    {
        return new SchemaTableName(schemaName, tableName + "$" + tableType.name().toLowerCase(Locale.ROOT));
    }

    public Optional<AggIndex> getAggIndex()
    {
        return aggIndex;
    }

    public IcebergTableHandle withProjectedColumns(Set<IcebergColumnHandle> projectedColumns)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                maxScannedFileSize,
                corrColPredicate,
                corrTables,
                aggIndex,
                constraintColumns,
                connectorExpression,
                conExprAssignments,
                readPartialFiles,
                aggIndexId);
    }

    public IcebergTableHandle withRetryMode(RetryMode retryMode)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                maxScannedFileSize,
                corrColPredicate,
                corrTables,
                aggIndex,
                constraintColumns,
                connectorExpression,
                conExprAssignments,
                readPartialFiles,
                aggIndexId);
    }

    public IcebergTableHandle withUpdatedColumns(List<IcebergColumnHandle> updatedColumns)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                maxScannedFileSize,
                corrColPredicate,
                corrTables,
                aggIndex,
                constraintColumns,
                connectorExpression,
                conExprAssignments,
                readPartialFiles,
                aggIndexId);
    }

    public IcebergTableHandle forOptimize(boolean recordScannedFiles, DataSize maxScannedFileSize)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                Optional.of(maxScannedFileSize),
                corrColPredicate,
                corrTables,
                aggIndex,
                constraintColumns,
                connectorExpression,
                conExprAssignments,
                readPartialFiles,
                aggIndexId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IcebergTableHandle that = (IcebergTableHandle) o;
        return recordScannedFiles == that.recordScannedFiles &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                tableType == that.tableType &&
                Objects.equals(snapshotId, that.snapshotId) &&
                Objects.equals(tableSchemaJson, that.tableSchemaJson) &&
                Objects.equals(partitionSpecJson, that.partitionSpecJson) &&
                formatVersion == that.formatVersion &&
                Objects.equals(unenforcedPredicate, that.unenforcedPredicate) &&
                Objects.equals(enforcedPredicate, that.enforcedPredicate) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(nameMappingJson, that.nameMappingJson) &&
                Objects.equals(tableLocation, that.tableLocation) &&
                Objects.equals(retryMode, that.retryMode) &&
                Objects.equals(updatedColumns, that.updatedColumns) &&
                Objects.equals(storageProperties, that.storageProperties) &&
                Objects.equals(maxScannedFileSize, that.maxScannedFileSize) &&
                Objects.equals(corrColPredicate, that.corrColPredicate) &&
                Objects.equals(corrTables, that.corrTables) &&
                Objects.equals(aggIndex, that.aggIndex) &&
                Objects.equals(constraintColumns, that.constraintColumns) &&
                Objects.equals(connectorExpression, that.connectorExpression) &&
                Objects.equals(conExprAssignments, that.conExprAssignments) &&
                Objects.equals(readPartialFiles, that.readPartialFiles) &&
                Objects.equals(aggIndexId, that.aggIndexId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableType, snapshotId, tableSchemaJson, partitionSpecJson, formatVersion, unenforcedPredicate, enforcedPredicate,
                projectedColumns, nameMappingJson, tableLocation, storageProperties, retryMode, updatedColumns, recordScannedFiles, maxScannedFileSize,
                corrColPredicate, corrTables, aggIndex, constraintColumns, connectorExpression, conExprAssignments, readPartialFiles, aggIndexId);
    }

    @Override
    public String toString()
    {
        if (getAggIndex().isPresent()) {
            return getSchemaTableNameWithType()
                    + getAggIndex().get().toString()
                    + getUnenforcedPredicate().toString()
                    + getEnforcedPredicate().toString()
                    + snapshotId.map(v -> "@" + v).orElse("");
        }
        StringBuilder builder = new StringBuilder(getSchemaTableNameWithType().toString());
        snapshotId.ifPresent(snapshotId -> builder.append("@").append(snapshotId));
        if (enforcedPredicate.isNone()) {
            builder.append(" constraint=FALSE");
        }
        else if (!enforcedPredicate.isAll()) {
            builder.append(" constraint on ");
            builder.append(enforcedPredicate.getDomains().orElseThrow().keySet().stream()
                    .map(IcebergColumnHandle::getQualifiedName)
                    .collect(joining(", ", "[", "]")));
        }
        return builder.toString();
    }

    @JsonProperty
    public boolean isReadPartialFiles()
    {
        return readPartialFiles;
    }

    public void setReadPartialFiles(boolean partial)
    {
        this.readPartialFiles = partial;
    }

    public int getAggIndexId()
    {
        return aggIndexId;
    }

    public void setAggIndexId(int id)
    {
        this.aggIndexId = id;
    }
}
