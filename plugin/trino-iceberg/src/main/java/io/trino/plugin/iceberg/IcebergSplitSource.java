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
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.iceberg.util.MetricsUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPartitionHandle;
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
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.FilterMetrics;
import org.apache.iceberg.IndexField;
import org.apache.iceberg.IndexSpec;
import org.apache.iceberg.SystemProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.index.util.UsableIndicesVisitor;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.CorrelationUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Sets.intersection;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
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

    private final IcebergTableHandle tableHandle;
    private final Set<IcebergColumnHandle> identityPartitionColumns;
    private final TableScan tableScan;
    private final Map<Integer, Type.PrimitiveType> fieldIdToType;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final Constraint constraint;
    private final boolean indicesEnable;
    private final ExecutorService splitEnumeratorPool;
    private final String threadNamePrefix;

    private volatile boolean closed;
    private final ReentrantReadWriteLock scanLock = new ReentrantReadWriteLock();

    @GuardedBy("scanLock")
    private CloseableIterable<CombinedScanTask> combinedScanIterable;

    @GuardedBy("scanLock")
    private FilterMetrics filterMetrics;

    @GuardedBy("scanLock")
    private Iterator<FileScanTask> fileScanIterator;

    private TupleDomain<IcebergColumnHandle> pushedDownDynamicFilterPredicate;
    private int skippedSplitsByDynamicFilter;
    private boolean includeIndex;

    public IcebergSplitSource(
            ConnectorSession session,
            IcebergTableHandle tableHandle,
            Set<IcebergColumnHandle> identityPartitionColumns,
            TableScan tableScan,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            Constraint constraint,
            boolean indicesEnabled,
            boolean generateSplitsAsync)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.identityPartitionColumns = requireNonNull(identityPartitionColumns, "identityPartitionColumns is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.fieldIdToType = primitiveFieldTypes(tableScan.schema());
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeoutMillis = requireNonNull(dynamicFilteringWaitTimeout, "dynamicFilteringWaitTimeout is null").toMillis();
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
        this.constraint = requireNonNull(constraint, "constraint is null");
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
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            return dynamicFilter.isBlocked()
                    .thenApply(ignored -> EMPTY_BATCH)
                    .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
        }

        if (combinedScanIterable == null) {
            // Used to avoid duplicating work if the Dynamic Filter was already pushed down to the Iceberg API
            this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate().transformKeys(IcebergColumnHandle.class::cast);
            TupleDomain<IcebergColumnHandle> fullPredicate = tableHandle.getUnenforcedPredicate()
                    .intersect(tableHandle.getCorrColPredicate())
                    .intersect(pushedDownDynamicFilterPredicate);
            // TODO: (https://github.com/trinodb/trino/issues/9743): Consider removing TupleDomain#simplify
            TupleDomain<IcebergColumnHandle> simplifiedPredicate = fullPredicate.simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
            if (!simplifiedPredicate.equals(fullPredicate)) {
                // Pushed down predicate was simplified, always evaluate it against individual splits
                this.pushedDownDynamicFilterPredicate = TupleDomain.all();
            }

            TupleDomain<IcebergColumnHandle> effectivePredicate = tableHandle.getEnforcedPredicate()
                    .intersect(simplifiedPredicate);

            if (effectivePredicate.isNone()) {
                finish();
                return completedFuture(NO_MORE_SPLITS_BATCH);
            }

            Expression filterExpression = toIcebergExpression(effectivePredicate);
            if (indicesEnable) {
                IndexSpec indexSpec = tableScan.table().indexSpec();
                UsableIndicesVisitor visitor = new UsableIndicesVisitor(
                        CorrelationUtils.schemaWithCorrCols(tableScan.table().schema(), tableScan.table().correlatedColumnsSpec()).asStruct(),
                        indexSpec.fields());
                Set<IndexField> usableIndices = ExpressionVisitors.visit(filterExpression, visitor);
                includeIndex = usableIndices != null && !usableIndices.isEmpty();
                if (includeIndex) {
                    log.info("Found usable indices %s", usableIndices);
                }
            }

            TableScan refinedScan = tableScan
                    .filter(filterExpression)
                    .includeColumnStats()
                    .option(SystemProperties.SCAN_FILTER_METRICS_ENABLED, "true")
                    .withThreadName(threadNamePrefix + "producer");

            scanLock.writeLock().lock();
            try {
                this.combinedScanIterable = refinedScan.planTasks();
                this.filterMetrics = refinedScan.filterMetrics();
                this.fileScanIterator = Streams.stream(combinedScanIterable)
                        .map(CombinedScanTask::files)
                        .flatMap(Collection::stream)
                        .iterator();
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
        Iterator<FileScanTask> fileScanTasks = Iterators.limit(fileScanIterator, maxSize);
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (fileScanTasks.hasNext()) {
            if (closed) {
                // close() may be called by other threads before fileScanIterator is bond to an iterator,
                // so this iteration should be broken by checking closed state.
                finish();
                return NO_MORE_SPLITS_BATCH;
            }

            FileScanTask scanTask = fileScanTasks.next();

            if (!scanTask.deletes().isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Iceberg tables with delete files are not supported: " + tableHandle.getSchemaTableName());
            }

            IcebergSplit icebergSplit = toIcebergSplit(scanTask, includeIndex);

            Supplier<Map<ColumnHandle, NullableValue>> partitionValues = memoize(() -> {
                Map<ColumnHandle, NullableValue> bindings = new HashMap<>();
                for (IcebergColumnHandle partitionColumn : identityPartitionColumns) {
                    Object partitionValue = deserializePartitionValue(
                            partitionColumn.getType(),
                            icebergSplit.getPartitionKeys().get(partitionColumn.getId()).orElse(null),
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
                    skippedSplitsByDynamicFilter++;
                    continue;
                }
                if (!fileMatchesPredicate(
                        fieldIdToType,
                        dynamicFilterPredicate,
                        scanTask.file().lowerBounds(),
                        scanTask.file().upperBounds(),
                        scanTask.file().nullValueCounts())) {
                    skippedSplitsByDynamicFilter++;
                    continue;
                }
            }
            if (!partitionMatchesConstraint(identityPartitionColumns, partitionValues, constraint)) {
                continue;
            }
            splits.add(icebergSplit);
        }
        return new ConnectorSplitBatch(splits.build(), isFinished());
    }

    private void finish()
    {
        close();

        // combinedScanIterable and filScanIterator should be reset automatically and semantic consistency.
        scanLock.writeLock().lock();
        try {
            this.combinedScanIterable = CloseableIterable.empty();
            this.fileScanIterator = Collections.emptyIterator();
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
            return fileScanIterator != null && !fileScanIterator.hasNext();
        }
        finally {
            scanLock.readLock().unlock();
        }
    }

    @Override
    public Optional<Metrics> getMetrics()
    {
        Metrics.Accumulator metricsAccumulator = Metrics.accumulator();

        scanLock.readLock().lock();
        try {
            metricsAccumulator.add(MetricsUtils.makeMetricsFromFilterMetrics(filterMetrics));
        }
        finally {
            scanLock.readLock().unlock();
        }

        metricsAccumulator.add(MetricsUtils.makeLongCountMetrics(
                MetricsUtils.SKIPPED_SPLITS_BY_DF_IN_COORDINATOR, skippedSplitsByDynamicFilter));
        return Optional.of(metricsAccumulator.get());
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
            if (combinedScanIterable != null) {
                try {
                    combinedScanIterable.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
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

    private static IcebergSplit toIcebergSplit(FileScanTask task, boolean includeIndexInSplit)
    {
        String fileScanTaskEncode = null;
        if (includeIndexInSplit && task.file().indices() != null && !task.file().indices().isEmpty()) {
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
                task.file().format(),
                ImmutableList.of(),
                getPartitionKeys(task),
                fileScanTaskEncode);
    }
}
