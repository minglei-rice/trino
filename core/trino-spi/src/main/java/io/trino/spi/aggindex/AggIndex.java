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

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AggIndex
        implements java.io.Serializable
{
    private final int aggIndexId;

    private final List<TableColumnIdentify> dimFields;

    private final Map<AggFunctionDesc, String> aggFunctionDescToName;

    private final List<CorrColumns> corrColumns;

    private boolean allowPartialAggIndex = true;

    public AggIndex(
            int aggIndexId,
            List<TableColumnIdentify> dimFields,
            Map<AggFunctionDesc, String> aggFunctionDescToName,
            List<CorrColumns> corrColumns)
    {
        this.aggIndexId = aggIndexId;
        this.dimFields = dimFields;
        this.aggFunctionDescToName = aggFunctionDescToName;
        this.corrColumns = corrColumns;
    }

    public List<CorrColumns> getCorrColumns()
    {
        return corrColumns;
    }

    public int getAggIndexId()
    {
        return aggIndexId;
    }

    public List<TableColumnIdentify> getDimFields()
    {
        return dimFields;
    }

    public Map<AggFunctionDesc, String> getAggFunctionDescToName()
    {
        return aggFunctionDescToName;
    }

    public boolean isAllowPartialAggIndex()
    {
        return allowPartialAggIndex;
    }

    public void setAllowPartialAggIndex(boolean allowed)
    {
        this.allowPartialAggIndex = allowed;
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
        AggIndex aggIndex = (AggIndex) o;
        return aggIndexId == aggIndex.aggIndexId && Objects.equals(dimFields, aggIndex.dimFields)
                && Objects.equals(aggFunctionDescToName, aggIndex.aggFunctionDescToName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(aggIndexId, dimFields, aggFunctionDescToName);
    }

    @Override
    public String toString()
    {
        return "AggIndex{" +
                "aggIndexId=" + aggIndexId +
                ", dimFields=" + dimFields +
                ", aggFunctionDescToName=" + aggFunctionDescToName +
                ", corrColumns=" + corrColumns +
                '}';
    }

    public enum AggFunctionType
    {
        AVG("avg"),
        COUNT_DISTINCT("count_distinct"),
        APPROX_COUNT_DISTINCT("approx_count_distinct"),
        PERCENTILE("percentile");

        String name;
        AggFunctionType(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }
    }
}
