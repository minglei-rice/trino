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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.aggindex.AggIndex;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.StringPredicate;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

import static java.util.Objects.requireNonNull;

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

    // Filter used during split generation and table scan, but not required to be strictly enforced by Iceberg Connector
    private final TupleDomain<IcebergColumnHandle> unenforcedPredicate;

    // Filter guaranteed to be enforced by Iceberg connector
    private final TupleDomain<IcebergColumnHandle> enforcedPredicate;
    // The evaluator, as the extra Filter which could not be translated to tuple domains,
    // should contain partition columns and is guaranteed to be enforced by Iceberg connector
    private Optional<BiPredicate<PartitionSpec, StructLike>> enforcedEvaluator = Optional.empty();

    private final Set<IcebergColumnHandle> projectedColumns;
    private final Optional<String> nameMappingJson;
    private final Optional<AggIndex> aggIndex;

    private final TupleDomain<IcebergColumnHandle> corrColPredicate;

    private final Optional<Set<StringPredicate>> stringPredicates;

    @JsonCreator
    public IcebergTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("unenforcedPredicate") TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            @JsonProperty("enforcedPredicate") TupleDomain<IcebergColumnHandle> enforcedPredicate,
            @JsonProperty("projectedColumns") Set<IcebergColumnHandle> projectedColumns,
            @JsonProperty("nameMappingJson") Optional<String> nameMappingJson,
            @JsonProperty("corrColPredicate") TupleDomain<IcebergColumnHandle> corrColPredicate,
            @JsonProperty("stringPredicate") Optional<Set<StringPredicate>> stringPredicates,
            @JsonProperty("aggIndex") Optional<AggIndex> aggIndex)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
        this.enforcedPredicate = requireNonNull(enforcedPredicate, "enforcedPredicate is null");
        this.projectedColumns = ImmutableSet.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.nameMappingJson = requireNonNull(nameMappingJson, "nameMappingJson is null");
        this.corrColPredicate = requireNonNull(corrColPredicate, "corrColPredicate is null");
        this.stringPredicates = requireNonNull(stringPredicates, "stringPredicate is null");
        this.aggIndex = aggIndex;
    }

    public IcebergTableHandle(
            String schemaName,
            String tableName,
            TableType tableType,
            Optional<Long> snapshotId,
            TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            TupleDomain<IcebergColumnHandle> enforcedPredicate,
            Set<IcebergColumnHandle> projectedColumns,
            Optional<String> nameMappingJson,
            TupleDomain<IcebergColumnHandle> corrColPredicate,
            Optional<BiPredicate<PartitionSpec, StructLike>> enforcedEvaluator,
            Optional<Set<StringPredicate>> stringPredicates,
            Optional<AggIndex> aggIndex)
    {
        this(schemaName, tableName, tableType, snapshotId, unenforcedPredicate, enforcedPredicate, projectedColumns, nameMappingJson, corrColPredicate, stringPredicates, aggIndex);
        this.enforcedEvaluator = requireNonNull(enforcedEvaluator, "evaluator is null");
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

    @JsonProperty
    public Optional<String> getNameMappingJson()
    {
        return nameMappingJson;
    }

    @JsonProperty
    public TupleDomain<IcebergColumnHandle> getCorrColPredicate()
    {
        return corrColPredicate;
    }

    @JsonProperty
    public Optional<Set<StringPredicate>> getStringPredicates()
    {
        return stringPredicates;
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
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                corrColPredicate,
                enforcedEvaluator,
                stringPredicates,
                aggIndex);
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
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                tableType == that.tableType &&
                Objects.equals(snapshotId, that.snapshotId) &&
                Objects.equals(unenforcedPredicate, that.unenforcedPredicate) &&
                Objects.equals(enforcedPredicate, that.enforcedPredicate) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(nameMappingJson, that.nameMappingJson) &&
                Objects.equals(aggIndex, that.aggIndex);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableType, snapshotId, unenforcedPredicate, enforcedPredicate, projectedColumns, nameMappingJson, aggIndex);
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
        else {
            return getSchemaTableNameWithType()
                    + getUnenforcedPredicate().toString()
                    + getEnforcedPredicate().toString()
                    + snapshotId.map(v -> "@" + v).orElse("");
        }
    }

    public Optional<BiPredicate<PartitionSpec, StructLike>> getEnforcedEvaluator()
    {
        return enforcedEvaluator;
    }
}
