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
package io.trino.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public final class ColumnsInPredicate
{
    private final String catalog;
    private final String schema;
    private final String table;
    private final Set<Column> colsInDiscretePredicate;
    private final Set<Column> colsInRangePredicate;

    @JsonCreator
    public ColumnsInPredicate(
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("colsInDiscretePredicate") Set<Column> colsInDiscretePredicate,
            @JsonProperty("colsInRangePredicate") Set<Column> colsInRangePredicate)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        requireNonNull(colsInDiscretePredicate, "colsInDiscretePredicate is null");
        this.colsInDiscretePredicate = ImmutableSet.copyOf(colsInDiscretePredicate);
        requireNonNull(colsInRangePredicate, "colsInRangePredicate is null");
        this.colsInRangePredicate = ImmutableSet.copyOf(colsInRangePredicate);
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public Set<Column> getColsInDiscretePredicate()
    {
        return colsInDiscretePredicate;
    }

    @JsonProperty
    public Set<Column> getColsInRangePredicate()
    {
        return colsInRangePredicate;
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
        ColumnsInPredicate other = (ColumnsInPredicate) o;
        return Objects.equals(catalog, other.catalog) &&
                Objects.equals(schema, other.schema) &&
                Objects.equals(table, other.table) &&
                Objects.equals(colsInDiscretePredicate, other.colsInDiscretePredicate) &&
                Objects.equals(colsInRangePredicate, other.colsInRangePredicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, schema, table, colsInDiscretePredicate, colsInRangePredicate);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(catalog)
                .addValue(schema)
                .addValue(table)
                .addValue(colsInDiscretePredicate)
                .addValue(colsInRangePredicate)
                .toString();
    }
}
