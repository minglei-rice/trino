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
package io.trino.sql.planner;

import io.trino.Session;
import io.trino.execution.Column;
import io.trino.execution.ColumnsInPredicate;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableSchema;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class ColumnsInUnenforcedPredicateExtractor
{
    private final Metadata metadata;
    private final Session session;

    public ColumnsInUnenforcedPredicateExtractor(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
    }

    public Set<ColumnsInPredicate> extract(Plan plan)
    {
        Visitor visitor = new Visitor();
        plan.getRoot().accept(visitor, null);
        return visitor.getColumnsInPredicateSet();
    }

    private class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final Map<QualifiedObjectName, ColumnsInPredicate> columnsInPredicateMap = new HashMap<>();

        public Set<ColumnsInPredicate> getColumnsInPredicateSet()
        {
            return columnsInPredicateMap.values().stream().collect(toImmutableSet());
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            TableHandle tableHandle = node.getTable();

            Set<Column> colsInDiscretePredicate = new HashSet<>();
            Set<Column> colsInRangePredicate = new HashSet<>();
            metadata.getTableProperties(session, tableHandle)
                    .getUnenforcedPredicates()
                    .flatMap(TupleDomain::getDomains)
                    .ifPresent(domains ->
                            domains.forEach(((columnHandle, domain) -> {
                                ValueSet valueSet = domain.getValues();
                                if (!valueSet.isAll() && !valueSet.isNone()) {
                                    ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, columnHandle);
                                    Column column = new Column(columnMetadata.getName(), columnMetadata.getType().toString());
                                    if (valueSet.isDiscreteSet()) {
                                        colsInDiscretePredicate.add(column);
                                    }
                                    else {
                                        colsInRangePredicate.add(column);
                                    }
                                }
                            })));

            // merge result from same source table
            TableSchema tableSchema = metadata.getTableSchema(session, tableHandle);
            QualifiedObjectName qualifiedTableName = tableSchema.getQualifiedName();
            ColumnsInPredicate previous = columnsInPredicateMap.get(qualifiedTableName);
            if (previous != null) {
                colsInDiscretePredicate.addAll(previous.getColsInDiscretePredicate());
                colsInRangePredicate.addAll(previous.getColsInRangePredicate());
            }

            // put into map even if this node has no unenforced predicates
            // so that we can obtain all source tables from the result
            columnsInPredicateMap.put(
                    qualifiedTableName,
                    new ColumnsInPredicate(
                            qualifiedTableName.getCatalogName(),
                            qualifiedTableName.getSchemaName(),
                            qualifiedTableName.getObjectName(),
                            colsInDiscretePredicate,
                            colsInRangePredicate));
            return null;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }
            return null;
        }
    }
}