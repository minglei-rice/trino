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
package io.trino.server;

import io.trino.jdbc.TrinoArray;
import org.assertj.core.api.Assertions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * How to run this test?
 * <p>
 * Start a {@link DevelopmentServer} at first, then to run this class.
 * If the development server has a customized port, please change the default port 8080 to the same.
 * <p>
 * All the hive functions appeared in this class have been registered on the backend Hive
 * metastore, so if you met the exceptions containing message "not registered", it implies
 * the tested functions have not been registered, so to register them; or are not reflected,
 * so to make sure there are no bugs of Trino.
 */
public class TestExternalHiveFunctionsThroughJdbc {
    private static final String host = "localhost";
    private static final int port = 8080;

    public static void main(String[] args)
            throws SQLException
    {
        TestExternalHiveFunctionsThroughJdbc tester = new TestExternalHiveFunctionsThroughJdbc();
        tester.testHiveFunctions();
    }

    public void testHiveFunctions()
            throws SQLException
    {
        Object value = runAndGetSingleValue("select b_parse_url('O%2FLMsMPS9cEHbHgj73LaXaI%2BD%2B')");
        assertEquals(value, "O/LMsMPS9cEHbHgj73LaXaI+D+");
        // with external function resolver name hive
        value = runAndGetSingleValue("select hive.b_parse_url('O%2FLMsMPS9cEHbHgj73LaXaI%2BD%2B')");
        assertEquals(value, "O/LMsMPS9cEHbHgj73LaXaI+D+");
        // with catalog hive and resolver hive
        value = runAndGetSingleValue("select hive.hive.b_parse_url('O%2FLMsMPS9cEHbHgj73LaXaI%2BD%2B')");
        assertEquals(value, "O/LMsMPS9cEHbHgj73LaXaI+D+");

        value = runAndGetSingleValue("select bdecode('http%3A%2F%2Fwww.bilibili.com%2F')");
        assertEquals(value, "http://www.bilibili.com/");

        value = runAndGetSingleValue("select b_gbk_decode('{%22event%22:%22click_banner%22,%22value%22:{%22title%22:%22%E5%A4%A7boss%E7%99%BB%E5%9C%BA%22,%22url%22:%22http://bangumi.bilibili.com/anime/24588%22,%22ord_id%22:1}}')");
        assertEquals(value, "{\"event\":\"click_banner\",\"value\":{\"title\":\"大boss登场\",\"url\":\"http://bangumi.bilibili.com/anime/24588\",\"ord_id\":1}}");

        value = runAndGetSingleValue("select b_line2point(1, 10)");
        assertEquals(Arrays.asList((Object[]) ((TrinoArray) value).getArray()), Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        value = runAndGetSingleValue("select b_json2map('{\"k1\":\"v1\",\"k2\":\"v2\"}')");
        assertEquals(value, Map.of("k1", "v1", "k2", "v2"));

        value = runAndGetSingleValue("select b_map2json(map(array['k1', 'k2'], array[100.0, 1.0]))");
        // b_map2json will prune the precision for the decimal types
        assertEquals((String) value, "{\"k1\":\"100\",\"k2\":\"1\"}");

        // failed on prohibited Hive built-in function
        Assertions.assertThatThrownBy(() -> runAndGetSingleValue("select current_database()"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Could not resolve function current_database");

        // failed on unregistered Hive UDF
        Assertions.assertThatThrownBy(() -> runAndGetSingleValue("select b_typeid(105)"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Could not resolve function b_typeid");
    }

    private Object runAndGetSingleValue(String sql)
            throws SQLException
    {
        Object ret = null;
        try (Connection connection = createConnection(host + ":" + port);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                ret = resultSet.getObject(1);
            }
        }
        return ret;
    }

    private Connection createConnection(String address)
            throws SQLException
    {
        String url = format("jdbc:trino://%s", address);
        return DriverManager.getConnection(url, "admin", null);
    }
}
