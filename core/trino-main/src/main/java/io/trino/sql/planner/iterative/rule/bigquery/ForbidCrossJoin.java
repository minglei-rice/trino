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
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.Patterns;

import static io.trino.SystemSessionProperties.forbidCrossJoin;

/**
 * Queries that optimized to CrossJoin should not be forbidden.
 */
public class ForbidCrossJoin
        implements Rule<JoinNode>
{
    @Override
    public boolean isEnabled(Session session)
    {
        return forbidCrossJoin(session);
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return Patterns.join();
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        if (node.isCrossJoin()) {
            throw new TrinoException(StandardErrorCode.QUERY_REJECTED, "Cross join is not allowed," +
                    " if you actually want query run cross join, please set session forbid_cross_join to false.");
        }
        return Result.empty();
    }
}
