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
package io.trino.filesystem.alluxio;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Locale;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

public class TestAlluxioFileSystem
{
    public static final ConnectorSession SESSION = new ConnectorSession()
    {
        @Override
        public String getQueryId()
        {
            return "test_query_id";
        }

        @Override
        public Optional<String> getSource()
        {
            return Optional.of("TestSource");
        }

        @Override
        public ConnectorIdentity getIdentity()
        {
            return ConnectorIdentity.ofUser("user");
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return UTC_KEY;
        }

        @Override
        public Locale getLocale()
        {
            return ENGLISH;
        }

        @Override
        public Instant getStart()
        {
            return Instant.ofEpochMilli(0);
        }

        @Override
        public Optional<String> getTraceToken()
        {
            return Optional.empty();
        }

        @Override
        public <T> T getProperty(String name, Class<T> type)
        {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Unknown session property " + name);
        }
    };

    @Test
    public void testNewInputFile()
    {
        String dirPath = "/user/hive/warehouse";
        String hdfsPath = "hdfs://localhost:9000" + dirPath;
        String alluxioPath = "alluxio://" + dirPath;

        HdfsConfig hdfsConfig = new HdfsConfig();
        DynamicHdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());

        TrinoFileSystemFactory factory = new HdfsFileSystemFactory(hdfsEnvironment);

        // without cache
        TrinoFileSystem fileSystem = factory.create(SESSION);
        assertEquals(fileSystem.newInputFile(hdfsPath).location(), hdfsPath);

        // with cache
        fileSystem = factory.createCachingFileSystem(SESSION);
        assertEquals(fileSystem.newInputFile(hdfsPath).location(), alluxioPath);
    }
}
