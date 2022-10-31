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
import java.util.Objects;

public class AggIndex
        implements java.io.Serializable
{
    private final int aggIndexId;

    private final List<TableColumnIdentify> dimFields;

    private final List<AggFunctionDesc> aggFunctionDescs;

    private final List<CorrColumns> corrColumns;

    public AggIndex(
            int aggIndexId,
            List<TableColumnIdentify> dimFields,
            List<AggFunctionDesc> aggFunctionDescs,
            List<CorrColumns> corrColumns)
    {
        this.aggIndexId = aggIndexId;
        this.dimFields = dimFields;
        this.aggFunctionDescs = aggFunctionDescs;
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

    public List<AggFunctionDesc> getAggFunctionDescs()
    {
        return aggFunctionDescs;
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
                && Objects.equals(aggFunctionDescs, aggIndex.aggFunctionDescs);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(aggIndexId, dimFields, aggFunctionDescs);
    }

    @Override
    public String toString()
    {
        return "AggIndex{" +
                "aggIndexId=" + aggIndexId +
                ", dimFields=" + dimFields +
                ", aggFunctionDescs=" + aggFunctionDescs +
                ", corrColumns=" + corrColumns +
                '}';
    }
}
