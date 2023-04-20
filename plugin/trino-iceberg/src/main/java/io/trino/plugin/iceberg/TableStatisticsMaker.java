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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isExtendedStatisticsEnabled;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class TableStatisticsMaker
{
    public static final String TRINO_STATS_PREFIX = "trino.stats.ndv.";
    public static final String TRINO_STATS_NDV_FORMAT = TRINO_STATS_PREFIX + "%d.ndv";
    public static final Pattern TRINO_STATS_COLUMN_ID_PATTERN = Pattern.compile(Pattern.quote(TRINO_STATS_PREFIX) + "(?<columnId>\\d+)\\..*");
    public static final Pattern TRINO_STATS_NDV_PATTERN = Pattern.compile(Pattern.quote(TRINO_STATS_PREFIX) + "(?<columnId>\\d+)\\.ndv");

    private final TypeManager typeManager;
    private final ConnectorSession session;
    private final Table icebergTable;

    private TableStatisticsMaker(TypeManager typeManager, ConnectorSession session, Table icebergTable)
    {
        this.typeManager = typeManager;
        this.session = session;
        this.icebergTable = icebergTable;
    }

    public static TableStatistics getTableStatistics(TypeManager typeManager, ConnectorSession session, IcebergTableHandle tableHandle, Table icebergTable)
    {
        return new TableStatisticsMaker(typeManager, session, icebergTable).makeTableStatistics(tableHandle);
    }

    private TableStatistics makeTableStatistics(IcebergTableHandle tableHandle)
    {
        if (tableHandle.getSnapshotId().isEmpty()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        TupleDomain<IcebergColumnHandle> enforcedPredicate = tableHandle.getEnforcedPredicate();

        if (enforcedPredicate.isNone()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        Schema icebergTableSchema = icebergTable.schema();
        List<Types.NestedField> columns = icebergTableSchema.columns();

        List<IcebergColumnHandle> columnHandles = getColumns(icebergTableSchema, typeManager);
        Map<Integer, IcebergColumnHandle> idToColumnHandle = columnHandles.stream()
                .collect(toUnmodifiableMap(IcebergColumnHandle::getId, identity()));

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(enforcedPredicate))
                .useSnapshot(tableHandle.getSnapshotId().get())
                .includeColumnStats();
        IcebergStatistics.Builder icebergStatisticsBuilder = new IcebergStatistics.Builder(columns, typeManager);
        boolean accurate = tableHandle.getUnenforcedPredicate().isAll();
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                icebergStatisticsBuilder.acceptDataFile(fileScanTask.file(), fileScanTask.spec());
                if (!fileScanTask.deletes().isEmpty()) {
                    accurate = false;
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        IcebergStatistics summary = icebergStatisticsBuilder.build();

        if (summary.getFileCount() == 0) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        Map<Integer, Long> ndvs = readNdvs(icebergTable);

        ImmutableMap.Builder<ColumnHandle, ColumnStatistics> columnHandleBuilder = ImmutableMap.builder();
        double recordCount = summary.getRecordCount();
        for (IcebergColumnHandle columnHandle : idToColumnHandle.values()) {
            int fieldId = columnHandle.getId();
            ColumnStatistics.Builder columnBuilder = new ColumnStatistics.Builder();
            if (summary.getNullCounts() != null) {
                Long nullCount = summary.getNullCounts().get(fieldId);
                if (nullCount != null) {
                    // If nulls count was collected on this field, then set column count with it.
                    columnBuilder.setAccurateNullsCount(Estimate.of(nullCount));
                    columnBuilder.setNullsFraction(Estimate.of(nullCount / recordCount));
                }
            }
            if (summary.getColumnSizes() != null) {
                Long columnSize = summary.getColumnSizes().get(fieldId);
                if (columnSize != null) {
                    columnBuilder.setDataSize(Estimate.of(columnSize));
                }
            }
            Object min = summary.getMinValues().get(fieldId);
            Object max = summary.getMaxValues().get(fieldId);
            if (min != null && max != null) {
                columnBuilder.setRange(DoubleRange.from(columnHandle.getType(), min, max));
            }
            columnBuilder.setDistinctValuesCount(
                    Optional.ofNullable(ndvs.get(fieldId))
                            .map(Estimate::of)
                            .orElseGet(Estimate::unknown));
            columnHandleBuilder.put(columnHandle, columnBuilder.build());
        }
        return new TableStatistics(Estimate.of(recordCount), columnHandleBuilder.buildOrThrow(), accurate);
    }

    private Map<Integer, Long> readNdvs(Table icebergTable)
    {
        if (!isExtendedStatisticsEnabled(session)) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<Integer, Long> ndvByColumnId = ImmutableMap.builder();
        icebergTable.properties().forEach((key, value) -> {
            if (key.startsWith(TRINO_STATS_PREFIX)) {
                Matcher matcher = TRINO_STATS_NDV_PATTERN.matcher(key);
                if (matcher.matches()) {
                    int columnId = Integer.parseInt(matcher.group("columnId"));
                    long ndv = Long.parseLong(value);
                    ndvByColumnId.put(columnId, ndv);
                }
            }
        });
        return ndvByColumnId.buildOrThrow();
    }
}
