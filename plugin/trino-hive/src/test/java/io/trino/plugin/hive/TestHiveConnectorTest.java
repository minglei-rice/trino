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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

public class TestHiveConnectorTest
        extends BaseHiveConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BaseHiveConnectorTest.createHiveQueryRunner(ImmutableMap.of(), runner -> {});
    }

    @Test
    public void testDependentFilters()
    {
        String tableName = "bad_optimized_table";
        assertUpdate("CREATE TABLE " + tableName + " (keys ARRAY(VARCHAR), mvalues map(VARCHAR, ARRAY(BIGINT)))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (ARRAY['consented', 'a'], MAP(ARRAY['consented`a`1'], ARRAY[ARRAY[10, 1]]))", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (ARRAY['bad', 'a'], MAP(ARRAY['bad`a`1'], ARRAY[ARRAY[10, 1]]))", 1);
        query("select *\n" +
                "from\n" +
                "(\n" +
                "  select keys, mvalues\n" +
                "  FROM " + tableName +
                "  where array_position(keys, 'consented') > 0\n" +
                ") t1\n" +
                "WHERE cardinality (map_filter(mvalues, (key, value) -> CASE\n" +
                "WHEN split(key, '`') [array_position(keys,'consented')] = 'consented'\n" +
                "     AND cardinality(value) > 0 THEN TRUE\n" +
                "     ELSE FALSE\n" +
                "     END)) > 0");
        assertUpdate("DROP TABLE " + tableName);
    }
}
