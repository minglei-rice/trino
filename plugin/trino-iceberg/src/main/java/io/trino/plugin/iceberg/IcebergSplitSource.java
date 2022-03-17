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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.iceberg.util.DataFileWithDeleteFiles;
import io.trino.plugin.iceberg.util.MetricsUtils;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.FilterMetrics;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SystemProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.TableScanUtil;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnHandle;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.types.Conversions.fromByteBuffer;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(IcebergSplitSource.class);

    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private static final ConnectorSplitBatch NO_MORE_SPLITS_BATCH = new ConnectorSplitBatch(ImmutableList.of(), true);

    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final IcebergTableHandle tableHandle;
    private final TableScan tableScan;
    private final Optional<Long> maxScannedFileSizeInBytes;
    private final Map<Integer, Type.PrimitiveType> fieldIdToType;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final Constraint constraint;
    private final boolean indicesEnable;
    private final TypeManager typeManager;
    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;
    private final TupleDomain<IcebergColumnHandle> dataColumnPredicate;
    private final Domain pathDomain;
    private final Domain fileModifiedTimeDomain;
    private final ExecutorService splitEnumeratorPool;
    private final String threadNamePrefix;
    private final ReentrantReadWriteLock scanLock = new ReentrantReadWriteLock();

    private CloseableIterable<FileScanTask> fileScanTaskIterable;
    private CloseableIterator<FileScanTask> fileScanTaskIterator;
    private TupleDomain<IcebergColumnHandle> pushedDownDynamicFilterPredicate;
    private volatile boolean closed;

    private final boolean recordScannedFiles;
    private final ImmutableSet.Builder<DataFileWithDeleteFiles> scannedFiles = ImmutableSet.builder();

    private FilterMetrics filterMetrics;
    private int skippedSplitsByDynamicFilter;
    private long skippedDataSizeByDynamicFilter;
    private int skippedSplitsByPartitionFilter;
    private long skippedDataSizeByPartitionFilter;

    public IcebergSplitSource(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            IcebergTableHandle tableHandle,
            TableScan tableScan,
            Optional<DataSize> maxScannedFileSize,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            Constraint constraint,
            TypeManager typeManager,
            boolean recordScannedFiles,
            double minimumAssignedSplitWeight,
            boolean indicesEnabled,
            boolean generateSplitsAsync)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.maxScannedFileSizeInBytes = maxScannedFileSize.map(DataSize::toBytes);
        this.fieldIdToType = primitiveFieldTypes(tableScan.schema());
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeout.toMillis();
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.recordScannedFiles = recordScannedFiles;
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        this.dataColumnPredicate = tableHandle.getEnforcedPredicate().filter((column, domain) -> !isMetadataColumnId(column.getId()));
        this.pathDomain = getPathDomain(tableHandle.getEnforcedPredicate());
        this.fileModifiedTimeDomain = getFileModifiedTimePathDomain(tableHandle.getEnforcedPredicate());
        this.indicesEnable = indicesEnabled;
        threadNamePrefix = String.format("Query-%s-%s-", session.getQueryId(), IcebergSplitSource.class.getSimpleName());
        // use a single thread pool so that file scan iterator won't be accessed concurrently
        this.splitEnumeratorPool = !generateSplitsAsync ? null : Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(threadNamePrefix + "consumer")
                        .build());
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            return dynamicFilter.isBlocked()
                    .thenApply(ignored -> EMPTY_BATCH)
                    .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
        }

        if (fileScanTaskIterable == null) {
            // Used to avoid duplicating work if the Dynamic Filter was already pushed down to the Iceberg API
            boolean dynamicFilterIsComplete = dynamicFilter.isComplete();
            this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate().transformKeys(IcebergColumnHandle.class::cast);
            TupleDomain<IcebergColumnHandle> fullPredicate = tableHandle.getUnenforcedPredicate()
                    .intersect(tableHandle.getCorrColPredicate())
                    .intersect(pushedDownDynamicFilterPredicate);
            // TODO: (https://github.com/trinodb/trino/issues/9743): Consider removing TupleDomain#simplify
            TupleDomain<IcebergColumnHandle> simplifiedPredicate = fullPredicate.simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
            boolean usedSimplifiedPredicate = !simplifiedPredicate.equals(fullPredicate);
            if (usedSimplifiedPredicate) {
                // Pushed down predicate was simplified, always evaluate it against individual splits
                this.pushedDownDynamicFilterPredicate = TupleDomain.all();
            }

            TupleDomain<IcebergColumnHandle> effectivePredicate = dataColumnPredicate
                    .intersect(simplifiedPredicate);

            if (effectivePredicate.isNone()) {
                finish();
                return completedFuture(NO_MORE_SPLITS_BATCH);
            }

            Expression filterExpression = toIcebergExpression(effectivePredicate);
            // If the Dynamic Filter will be evaluated against each file, stats are required. Otherwise, skip them.
            boolean requiresColumnStats = usedSimplifiedPredicate || !dynamicFilterIsComplete;
            TableScan scan = tableScan.filter(filterExpression)
                    .option(SystemProperties.SCAN_FILTER_METRICS_ENABLED, "true")
                    .withThreadName(threadNamePrefix + "producer");
            if (requiresColumnStats) {
                scan = scan.includeColumnStats();
            }
            scanLock.writeLock().lock();
            try {
                this.fileScanTaskIterable = TableScanUtil.splitFiles(scan.planFiles(), tableScan.targetSplitSize());
                closer.register(fileScanTaskIterable);
                this.fileScanTaskIterator = fileScanTaskIterable.iterator();
                closer.register(fileScanTaskIterator);
                this.filterMetrics = scan.filterMetrics();
            }
            finally {
                scanLock.writeLock().unlock();
            }
        }

        TupleDomain<IcebergColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(IcebergColumnHandle.class::cast);
        if (dynamicFilterPredicate.isNone()) {
            finish();
            return completedFuture(NO_MORE_SPLITS_BATCH);
        }

        return splitEnumeratorPool != null ?
                new CompletableFuture<ConnectorSplitBatch>().completeAsync(() -> doGetNextBatch(maxSize, dynamicFilterPredicate), splitEnumeratorPool) :
                completedFuture(doGetNextBatch(maxSize, dynamicFilterPredicate));
    }

    private ConnectorSplitBatch doGetNextBatch(int maxSize, TupleDomain<IcebergColumnHandle> dynamicFilterPredicate)
    {
        Iterator<FileScanTask> fileScanTasks = Iterators.limit(fileScanTaskIterator, maxSize);
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (fileScanTasks.hasNext()) {
            if (closed) {
                // close() may be called by other threads before fileScanIterator is bond to an iterator,
                // so this iteration should be broken by checking closed state.
                finish();
                return NO_MORE_SPLITS_BATCH;
            }
            FileScanTask scanTask = fileScanTasks.next();
            if (scanTask.deletes().isEmpty() &&
                    maxScannedFileSizeInBytes.isPresent() &&
                    scanTask.file().fileSizeInBytes() > maxScannedFileSizeInBytes.get()) {
                continue;
            }

            if (!pathDomain.includesNullableValue(utf8Slice(scanTask.file().path().toString()))) {
                continue;
            }
            if (!fileModifiedTimeDomain.isAll()) {
                long fileModifiedTime = getModificationTime(new Path(scanTask.file().path().toString()));
                if (!fileModifiedTimeDomain.includesNullableValue(packDateTimeWithZone(fileModifiedTime, UTC_KEY))) {
                    continue;
                }
            }
            IcebergSplit icebergSplit = toIcebergSplit(scanTask, indicesEnable);

            Schema fileSchema = scanTask.spec().schema();
            Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(scanTask);

            Set<IcebergColumnHandle> identityPartitionColumns = partitionKeys.keySet().stream()
                    .map(fieldId -> getColumnHandle(fileSchema.findField(fieldId), typeManager))
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

            if (!dynamicFilterPredicate.isAll() && !dynamicFilterPredicate.equals(pushedDownDynamicFilterPredicate)) {
                if (!partitionMatchesPredicate(
                        identityPartitionColumns,
                        partitionValues,
                        dynamicFilterPredicate)) {
                    recordSkipMetricsForDynamicFilter(1, icebergSplit.getLength());
                    continue;
                }
                if (!fileMatchesPredicate(
                        fieldIdToType,
                        dynamicFilterPredicate,
                        scanTask.file().lowerBounds(),
                        scanTask.file().upperBounds(),
                        scanTask.file().nullValueCounts())) {
                    recordSkipMetricsForDynamicFilter(1, icebergSplit.getLength());
                    continue;
                }
            }
            if (!partitionMatchesConstraint(identityPartitionColumns, partitionValues, constraint)) {
                recordSkipMetricsForPartitionFilter(1, icebergSplit.getLength());
                continue;
            }
            if (recordScannedFiles) {
                // Positional and Equality deletes can only be cleaned up if the whole table has been optimized.
                // Equality deletes may apply to many files, and position deletes may be grouped together. This makes it difficult to know if they are obsolete.
                List<org.apache.iceberg.DeleteFile> fullyAppliedDeletes = tableHandle.getEnforcedPredicate().isAll() ? scanTask.deletes() : ImmutableList.of();
                scannedFiles.add(new DataFileWithDeleteFiles(scanTask.file(), fullyAppliedDeletes));
            }
            splits.add(icebergSplit);
        }
        return new ConnectorSplitBatch(splits.build(), isFinished());
    }

    private long getModificationTime(Path path)
    {
        try {
            FileStatus fileStatus = hdfsEnvironment.doAs(hdfsContext.getIdentity(), () -> hdfsEnvironment.getFileSystem(hdfsContext, path).getFileStatus(path));
            return fileStatus.getModificationTime();
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to get file modification time: " + path, e);
        }
    }

    private void recordSkipMetricsForDynamicFilter(int splitCount, long dataSize)
    {
        skippedSplitsByDynamicFilter += splitCount;
        skippedDataSizeByDynamicFilter += dataSize;
    }

    private void recordSkipMetricsForPartitionFilter(int splitCount, long dataSize)
    {
        skippedSplitsByPartitionFilter += splitCount;
        skippedDataSizeByPartitionFilter += dataSize;
    }

    private void finish()
    {
        close();
        scanLock.writeLock().lock();
        try {
            this.fileScanTaskIterable = CloseableIterable.empty();
            this.fileScanTaskIterator = CloseableIterator.empty();
        }
        finally {
            scanLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isFinished()
    {
        scanLock.readLock().lock();
        try {
            return fileScanTaskIterator != null && !fileScanTaskIterator.hasNext();
        }
        finally {
            scanLock.readLock().unlock();
        }
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        checkState(isFinished(), "Split source must be finished before TableExecuteSplitsInfo is read");
        if (!recordScannedFiles) {
            return Optional.empty();
        }
        return Optional.of(ImmutableList.copyOf(scannedFiles.build()));
    }

    @Override
    public Optional<Metrics> getMetrics()
    {
        Metrics metrics;
        scanLock.readLock().lock();
        try {
            metrics = MetricsUtils.makeMetrics(
                    filterMetrics,
                    skippedSplitsByDynamicFilter,
                    skippedDataSizeByDynamicFilter,
                    skippedSplitsByPartitionFilter,
                    skippedDataSizeByPartitionFilter);
        }
        finally {
            scanLock.readLock().unlock();
        }
        return Optional.of(metrics);
    }

    @Override
    public void close()
    {
        closed = true;
        if (splitEnumeratorPool != null) {
            splitEnumeratorPool.shutdownNow();
        }
        // close may be invoked before combinedScanIterable is initialized
        scanLock.readLock().lock();
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            scanLock.readLock().unlock();
        }
    }

    @VisibleForTesting
    static boolean fileMatchesPredicate(
            Map<Integer, Type.PrimitiveType> primitiveTypeForFieldId,
            TupleDomain<IcebergColumnHandle> dynamicFilterPredicate,
            @Nullable Map<Integer, ByteBuffer> lowerBounds,
            @Nullable Map<Integer, ByteBuffer> upperBounds,
            @Nullable Map<Integer, Long> nullValueCounts)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }
        Map<IcebergColumnHandle, Domain> domains = dynamicFilterPredicate.getDomains().orElseThrow();

        for (Map.Entry<IcebergColumnHandle, Domain> domainEntry : domains.entrySet()) {
            IcebergColumnHandle column = domainEntry.getKey();
            Domain domain = domainEntry.getValue();

            int fieldId = column.getId();
            boolean mayContainNulls;
            if (nullValueCounts == null) {
                mayContainNulls = true;
            }
            else {
                Long nullValueCount = nullValueCounts.get(fieldId);
                mayContainNulls = nullValueCount == null || nullValueCount > 0;
            }
            Type type = primitiveTypeForFieldId.get(fieldId);
            Domain statisticsDomain = domainForStatistics(
                    column.getType(),
                    lowerBounds == null ? null : fromByteBuffer(type, lowerBounds.get(fieldId)),
                    upperBounds == null ? null : fromByteBuffer(type, upperBounds.get(fieldId)),
                    mayContainNulls);
            if (!domain.overlaps(statisticsDomain)) {
                return false;
            }
        }
        return true;
    }

    private static Domain domainForStatistics(
            io.trino.spi.type.Type type,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            boolean mayContainNulls)
    {
        Type icebergType = toIcebergType(type);
        if (lowerBound == null && upperBound == null) {
            return Domain.create(ValueSet.all(type), mayContainNulls);
        }

        Range statisticsRange;
        if (lowerBound != null && upperBound != null) {
            statisticsRange = Range.range(
                    type,
                    convertIcebergValueToTrino(icebergType, lowerBound),
                    true,
                    convertIcebergValueToTrino(icebergType, upperBound),
                    true);
        }
        else if (upperBound != null) {
            statisticsRange = Range.lessThanOrEqual(type, convertIcebergValueToTrino(icebergType, upperBound));
        }
        else {
            statisticsRange = Range.greaterThanOrEqual(type, convertIcebergValueToTrino(icebergType, lowerBound));
        }
        return Domain.create(ValueSet.ofRanges(statisticsRange), mayContainNulls);
    }

    static boolean partitionMatchesConstraint(
            Set<IcebergColumnHandle> identityPartitionColumns,
            Supplier<Map<ColumnHandle, NullableValue>> partitionValues,
            Constraint constraint)
    {
        // We use Constraint just to pass functional predicate here from DistributedExecutionPlanner
        verify(constraint.getSummary().isAll());

        if (constraint.predicate().isEmpty() ||
                intersection(constraint.getPredicateColumns().orElseThrow(), identityPartitionColumns).isEmpty()) {
            return true;
        }
        return constraint.predicate().get().test(partitionValues.get());
    }

    @VisibleForTesting
    static boolean partitionMatchesPredicate(
            Set<IcebergColumnHandle> identityPartitionColumns,
            Supplier<Map<ColumnHandle, NullableValue>> partitionValues,
            TupleDomain<IcebergColumnHandle> dynamicFilterPredicate)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }
        Map<IcebergColumnHandle, Domain> domains = dynamicFilterPredicate.getDomains().orElseThrow();

        for (IcebergColumnHandle partitionColumn : identityPartitionColumns) {
            Domain allowedDomain = domains.get(partitionColumn);
            if (allowedDomain != null) {
                if (!allowedDomain.includesNullableValue(partitionValues.get().get(partitionColumn).getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    private IcebergSplit toIcebergSplit(FileScanTask task, boolean enabled)
    {
        String fileScanTaskEncode = null;
        if (enabled && task.file().indices() != null && !task.file().indices().isEmpty()) {
            try (ByteArrayOutputStream ba = new ByteArrayOutputStream(); ObjectOutputStream ob = new ObjectOutputStream(ba)) {
                ob.writeObject(task);
                fileScanTaskEncode = Base64.getEncoder().encodeToString(ba.toByteArray());
            }
            catch (Exception e) {
                log.error(e, "Encode fileScanTask error %s", task.file().path());
            }
        }
        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                task.file().recordCount(),
                IcebergFileFormat.fromIceberg(task.file().format()),
                ImmutableList.of(),
                PartitionSpecParser.toJson(task.spec()),
                PartitionData.toJson(task.file().partition()),
                task.deletes().stream()
                        .map(DeleteFile::fromIceberg)
                        .collect(toImmutableList()),
                SplitWeight.fromProportion(Math.min(Math.max((double) task.length() / tableScan.targetSplitSize(), minimumAssignedSplitWeight), 1.0)),
                fileScanTaskEncode);
    }

    private static Domain getPathDomain(TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        IcebergColumnHandle pathColumn = pathColumnHandle();
        Domain domain = effectivePredicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain"))
                .get(pathColumn);
        if (domain == null) {
            return Domain.all(pathColumn.getType());
        }
        return domain;
    }

    private static Domain getFileModifiedTimePathDomain(TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        IcebergColumnHandle fileModifiedTimeColumn = fileModifiedTimeColumnHandle();
        Domain domain = effectivePredicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain"))
                .get(fileModifiedTimeColumn);
        if (domain == null) {
            return Domain.all(fileModifiedTimeColumn.getType());
        }
        return domain;
    }
}
