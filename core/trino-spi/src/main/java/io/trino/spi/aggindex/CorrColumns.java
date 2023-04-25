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

import io.trino.spi.connector.JoinType;

import java.io.Serializable;
import java.util.List;

public class CorrColumns
        implements Serializable
{
    private final Corr correlation;

    public CorrColumns(Corr correlation)
    {
        this.correlation = correlation;
    }

    public Corr getCorrelation()
    {
        return correlation;
    }

    public static class Corr
            implements Serializable
    {
        // join key names in the left table
        private final List<TableColumnIdentify> leftKeys;
        // join key names in the right table
        private final List<TableColumnIdentify> rightKeys;
        private final JoinType joinType;
        private final JoinConstraint joinConstraint;

        public Corr(List<TableColumnIdentify> leftKeys, List<TableColumnIdentify> rightKeys, JoinType joinType, JoinConstraint joinConstraint)
        {
            this.leftKeys = leftKeys;
            this.rightKeys = rightKeys;
            this.joinType = joinType;
            this.joinConstraint = joinConstraint;
        }

        public List<TableColumnIdentify> getLeftKeys()
        {
            return leftKeys;
        }

        public List<TableColumnIdentify> getRightKeys()
        {
            return rightKeys;
        }

        public JoinType getJoinType()
        {
            return joinType;
        }

        public JoinConstraint getJoinConstraint()
        {
            return joinConstraint;
        }

        public enum JoinConstraint {
            NONE,
            UNIQUE,
            PK_FK
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("leftKeys: ").append("\n");
            for (TableColumnIdentify identify : leftKeys) {
                sb.append(identify.getTableName()).append(".").append(identify.getColumnName());
            }
            sb.append("rightKeys: ").append("\n");
            for (TableColumnIdentify identify : rightKeys) {
                sb.append(identify.getTableName()).append(".").append(identify.getColumnName());
            }
            sb.append("join type: \n").append(joinType.toString());
            sb.append("join constraint: \n").append(joinConstraint.toString());
            return sb.toString();
        }
    }
}
