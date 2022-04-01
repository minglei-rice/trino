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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.Column;
import io.trino.execution.ColumnsInPredicate;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableSchema;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.HashSet;
import java.util.Set;

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
        return visitor.getcolumnsInPredicateSet();
    }

    private class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final ImmutableSet.Builder<ColumnsInPredicate> columnsInPredicateSet = ImmutableSet.builder();

        public Set<ColumnsInPredicate> getcolumnsInPredicateSet()
        {
            return columnsInPredicateSet.build();
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

            TableSchema tableSchema = metadata.getTableSchema(session, tableHandle);
            if (!colsInDiscretePredicate.isEmpty() || !colsInRangePredicate.isEmpty()) {
                columnsInPredicateSet.add(
                        new ColumnsInPredicate(
                                tableSchema.getCatalogName().toString(),
                                tableSchema.getTable().getSchemaName(),
                                tableSchema.getTable().getTableName(),
                                colsInDiscretePredicate,
                                colsInRangePredicate));
            }
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
