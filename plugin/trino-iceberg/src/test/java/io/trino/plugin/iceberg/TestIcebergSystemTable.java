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

package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static org.testng.Assert.assertEquals;

public class TestIcebergSystemTable
        extends AbstractTestQueryFramework
{
    private File metastoreDir;

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.getParentFile().toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tempDir = Files.createTempDirectory("test_iceberg_system_table").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");
        return createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(),
                Collections.emptyList(),
                Optional.of(metastoreDir));
    }

    @Test
    public void testPartitionTable()
    {
        getQueryRunner().execute("create table foo(log_date varchar,f1 bigint) with (partitioning = ARRAY['log_date'])");
        getQueryRunner().execute("insert into foo values ('20220108',1471)");
        getQueryRunner().execute("insert into foo values ('20220109',1471)");
        assertEquals(getQueryRunner().execute("select count(*) from \"foo$partitions\"").getMaterializedRows().size(), 1);
        assertEquals(getQueryRunner().execute("select data from \"foo$partitions\"").getMaterializedRows().size(), 2);
        assertEquals(getQueryRunner().execute("select record_count from \"foo$partitions\"").getMaterializedRows().size(), 2);
    }
}
