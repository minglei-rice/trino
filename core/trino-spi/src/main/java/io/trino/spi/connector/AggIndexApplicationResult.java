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
package io.trino.spi.connector;

import io.trino.spi.aggindex.TableColumnIdentify;

import java.util.Map;

public class AggIndexApplicationResult<T>
{
    /**
     * A new table handle with an agg index.
     */
    private final T handle;

    /**
     * key is cube schema column name.
     * value is to its original table column name, not include join indicator.
     */
    private final Map<String, TableColumnIdentify> aggIndexColumnNameToIdentify;

    public AggIndexApplicationResult(
            T handle,
            Map<String, TableColumnIdentify> aggIndexColumnNameToIdentify)
    {
        this.handle = handle;
        this.aggIndexColumnNameToIdentify = aggIndexColumnNameToIdentify;
    }

    public T getHandle()
    {
        return handle;
    }

    public Map<String, TableColumnIdentify> getAggIndexColumnNameToIdentify()
    {
        return aggIndexColumnNameToIdentify;
    }

    @Override
    public String toString()
    {
        return "Table Handle is " + handle.toString()
                + ", AggIndex file column name " + aggIndexColumnNameToIdentify;
    }
}
