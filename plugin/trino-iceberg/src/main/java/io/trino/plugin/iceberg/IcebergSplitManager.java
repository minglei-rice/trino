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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DataChangeValidator;
import org.apache.iceberg.IndexSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.PropertyUtil;

import javax.inject.Inject;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.plugin.iceberg.IcebergSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isGenerateSplitsAsync;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isReadIndicesSwitchOn;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isValidateCorrTableDataChange;
import static java.util.Objects.requireNonNull;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(IcebergSplitManager.class);
    public static final int ICEBERG_DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final IcebergTransactionManager transactionManager;
    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public IcebergSplitManager(IcebergTransactionManager transactionManager, TypeManager typeManager, HdfsEnvironment hdfsEnvironment)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;

        if (table.getSnapshotId().isEmpty()) {
            if (table.isRecordScannedFiles()) {
                return new FixedSplitSource(ImmutableList.of(), ImmutableList.of());
            }
            return new FixedSplitSource(ImmutableList.of());
        }

        Table icebergTable = transactionManager.get(transaction, session.getIdentity()).getIcebergTable(session, table.getSchemaTableName());
        Duration dynamicFilteringWaitTimeout = getDynamicFilteringWaitTimeout(session);

        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(table.getSnapshotId().get());
        boolean indicesEnabled = isIndicesEnabled(tableScan, session);
        if (indicesEnabled) {
            log.info("Index enabled, including index stats");
            tableScan = tableScan.includeIndexStats();
            if (isValidateCorrTableDataChange(session) && !table.getCorrTables().isEmpty()) {
                Function<SchemaTableName, Table> tableResolver = t -> transactionManager.get(transaction, session.getIdentity()).getIcebergTable(session, t);
                Map<TableIdentifier, Long> corrSnapshots = table.getCorrTables().stream().collect(Collectors.toMap(entry -> TableIdentifier.of(entry.getSchemaName(), entry.getTableName()), entry -> entry.getSnapshotId().get()));
                tableScan = tableScan.withDataChangeValidator(new DataChangeValidator(t -> tableResolver.apply(new SchemaTableName(t.namespace().level(0), t.name())), corrSnapshots));
            }
        }
        else {
            tableScan = tableScan.disableInPlaceIndex(true);
        }

        IcebergSplitSource splitSource = new IcebergSplitSource(
                session,
                hdfsEnvironment,
                new HdfsContext(session),
                table,
                tableScan,
                table.getMaxScannedFileSize(),
                dynamicFilter,
                dynamicFilteringWaitTimeout,
                constraint,
                typeManager,
                table.isRecordScannedFiles(),
                getMinimumAssignedSplitWeight(session),
                indicesEnabled,
                isGenerateSplitsAsync(session));

        return new ClassLoaderSafeConnectorSplitSource(splitSource, IcebergSplitManager.class.getClassLoader());
    }

    private boolean isIndicesEnabled(TableScan tableScan, ConnectorSession session)
    {
        // Only when both table level indices and trino session indices property are enabled, then indices is enabled
        boolean indicesEnable = PropertyUtil.propertyAsBoolean(tableScan.table().properties(), TableProperties.HEURISTIC_INDEX_ENABLED_ON_READ,
                TableProperties.HEURISTIC_INDEX_ENABLED_ON_READ_DEFAULT) && isReadIndicesSwitchOn(session);
        if (indicesEnable) {
            // check the table has a valid IndexSpec
            IndexSpec indexSpec = tableScan.table().indexSpec();
            return indexSpec != null && !indexSpec.isUnIndexed();
        }

        return false;
    }
}
