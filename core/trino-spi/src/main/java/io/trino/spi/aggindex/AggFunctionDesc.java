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
package io.trino.spi.aggindex;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

public class AggFunctionDesc
        implements Serializable
{
    // aggregation function name
    private final String function;

    /**
     * Aggregate functions are only allowed to be defined on fact table columns
     */
    private final TableColumnIdentify columnIdentify;

    public AggFunctionDesc(String function, TableColumnIdentify columnIdentify)
    {
        this.function = function.toLowerCase(Locale.ENGLISH);
        this.columnIdentify = columnIdentify;
    }

    public String getFunction()
    {
        return function;
    }

    public TableColumnIdentify getColumnIdentify()
    {
        return columnIdentify;
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
        AggFunctionDesc that = (AggFunctionDesc) o;
        return Objects.equals(function, that.function)
                && Objects.equals(columnIdentify, that.columnIdentify);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function.toLowerCase(Locale.ENGLISH), columnIdentify);
    }

    @Override
    public String toString()
    {
        if (columnIdentify != null) {
            return "functionName=" + function + ",columnName=" + columnIdentify.getTableName() + "." + columnIdentify.getColumnName();
        }
        else {
            // count(*)
            return "functionName=" + function;
        }
    }
}
