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

import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.util.PropertyUtil;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import static org.apache.iceberg.TableProperties.READ_ALLUXIO_CACHE_LEVEL;
import static org.apache.iceberg.TableProperties.READ_ALLUXIO_CACHE_LEVEL_DEFAULT;

public class AlluxioCacheUtils
{
    private AlluxioCacheUtils() {}

    public static boolean readMetadataFromCache(ConnectorSession session)
    {
        AlluxioCacheLevel cacheLevel = IcebergSessionProperties.getAlluxioCacheLevel(session);
        return cacheLevel.ordinal() >= AlluxioCacheLevel.METADATA.ordinal();
    }

    public static boolean readIndexFromCache(ConnectorSession session, Map<String, String> tableProperties)
    {
        AlluxioCacheLevel sessionCacheLevel = IcebergSessionProperties.getAlluxioCacheLevel(session);
        AlluxioCacheLevel tableCacheLevel = getTableAlluxioCacheLevel(tableProperties);
        return Math.min(sessionCacheLevel.ordinal(), tableCacheLevel.ordinal()) >= AlluxioCacheLevel.INDEX.ordinal();
    }

    private static AlluxioCacheLevel getTableAlluxioCacheLevel(Map<String, String> tableProperties)
    {
        String cacheLevel = PropertyUtil.propertyAsString(tableProperties,
                READ_ALLUXIO_CACHE_LEVEL, READ_ALLUXIO_CACHE_LEVEL_DEFAULT).toUpperCase(Locale.ENGLISH);
        return Arrays.stream(AlluxioCacheLevel.values())
                .filter(level -> level.name().equals(cacheLevel))
                .findFirst()
                .orElse(AlluxioCacheLevel.DISABLED);
    }

    public enum AlluxioCacheLevel
    {
        DISABLED,
        METADATA,
        INDEX,
        // TODO: cache aggindices and data files in alluxio
        AGGINDEX,
        DATA
    }
}
