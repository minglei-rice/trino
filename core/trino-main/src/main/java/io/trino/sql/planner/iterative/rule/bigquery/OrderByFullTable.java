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
package io.trino.sql.planner.iterative.rule.bigquery;

import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.SortNode;

import static io.trino.SystemSessionProperties.orderByFullTable;

/**
 * - Limit (limit = x)
 *    - Project (identity, narrowing)
 *       - Sort (order by a, b)
 * or
 * - Limit (limit = x)
 *       - Sort (order by a, b)
 * or
 * - Limit (1+ties)
 *       - Sort (order by a, b)
 *
 *  The optimizer can eliminate these sort node by rules like `MergeLimitWithSort`, `MergeLimitOverProjectWithSort`. If the
 *  logic plan still has a SortNode after apply `RemoveRedundantSort` rule, then we can think the query scan a full table.
 */
public class OrderByFullTable
        implements Rule<SortNode>
{
    public OrderByFullTable() {}

    @Override
    public Pattern<SortNode> getPattern()
    {
        return Patterns.sort();
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return orderByFullTable(session);
    }

    @Override
    public Result apply(SortNode node, Captures captures, Context context)
    {
        throw new TrinoException(StandardErrorCode.QUERY_REJECTED, "OrderBy without limit is not allowed, " +
                "if you actually want query run order by without limit, please set session order_by_full_table to false.");
    }
}
