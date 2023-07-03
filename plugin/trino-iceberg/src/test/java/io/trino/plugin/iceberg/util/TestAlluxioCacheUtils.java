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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.plugin.iceberg.util.AlluxioCacheUtils.AlluxioCacheLevel.DATA;
import static io.trino.plugin.iceberg.util.AlluxioCacheUtils.AlluxioCacheLevel.DISABLED;
import static io.trino.plugin.iceberg.util.AlluxioCacheUtils.AlluxioCacheLevel.INDEX;
import static io.trino.plugin.iceberg.util.AlluxioCacheUtils.AlluxioCacheLevel.METADATA;
import static io.trino.plugin.iceberg.util.AlluxioCacheUtils.readIndexFromCache;
import static io.trino.plugin.iceberg.util.AlluxioCacheUtils.readMetadataFromCache;
import static org.apache.iceberg.TableProperties.READ_ALLUXIO_CACHE_LEVEL;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAlluxioCacheUtils
{
    private static final OrcReaderConfig ORC_READER_CONFIG = new OrcReaderConfig();
    private static final OrcWriterConfig ORC_WRITER_CONFIG = new OrcWriterConfig();
    private static final ParquetReaderConfig PARQUET_READER_CONFIG = new ParquetReaderConfig();
    private static final ParquetWriterConfig PARQUET_WRITER_CONFIG = new ParquetWriterConfig();

    @Test
    public void testCacheLevel()
    {
        ConnectorSession session = buildSessionWithCacheLevel(DISABLED);
        Map<String, String> tableProperties = ImmutableMap.of();
        assertFalse(readMetadataFromCache(session));
        assertFalse(readIndexFromCache(session, tableProperties));

        session = buildSessionWithCacheLevel(METADATA);
        assertTrue(readMetadataFromCache(session));
        assertFalse(readIndexFromCache(session, tableProperties));

        session = buildSessionWithCacheLevel(INDEX);
        assertTrue(readMetadataFromCache(session));
        assertTrue(readIndexFromCache(session, tableProperties));

        session = buildSessionWithCacheLevel(DATA);
        assertTrue(readMetadataFromCache(session));
        assertTrue(readIndexFromCache(session, tableProperties));

        tableProperties = buildPropertiesWithCacheLevel(METADATA);
        assertFalse(readIndexFromCache(session, tableProperties));
    }

    private ConnectorSession buildSessionWithCacheLevel(AlluxioCacheUtils.AlluxioCacheLevel cacheLevel)
    {
        IcebergConfig icebergConfig = new IcebergConfig();
        icebergConfig.setAlluxioCacheLevel(cacheLevel);
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new IcebergSessionProperties(icebergConfig, ORC_READER_CONFIG, ORC_WRITER_CONFIG, PARQUET_READER_CONFIG, PARQUET_WRITER_CONFIG).getSessionProperties())
                .build();
    }

    private Map<String, String> buildPropertiesWithCacheLevel(AlluxioCacheUtils.AlluxioCacheLevel cacheLevel)
    {
        return ImmutableMap.of(READ_ALLUXIO_CACHE_LEVEL, cacheLevel.name());
    }
}
