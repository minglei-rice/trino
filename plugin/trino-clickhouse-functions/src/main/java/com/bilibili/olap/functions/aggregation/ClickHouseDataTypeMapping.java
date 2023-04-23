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
package com.bilibili.olap.functions.aggregation;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

import static ru.yandex.clickhouse.domain.ClickHouseDataType.Int16;
import static ru.yandex.clickhouse.domain.ClickHouseDataType.Int32;
import static ru.yandex.clickhouse.domain.ClickHouseDataType.Int64;
import static ru.yandex.clickhouse.domain.ClickHouseDataType.Int8;
import static ru.yandex.clickhouse.domain.ClickHouseDataType.UInt16;
import static ru.yandex.clickhouse.domain.ClickHouseDataType.UInt32;
import static ru.yandex.clickhouse.domain.ClickHouseDataType.UInt64;
import static ru.yandex.clickhouse.domain.ClickHouseDataType.UInt8;

public enum ClickHouseDataTypeMapping
{
    INT8(0, Int8, 1),
    UINT8(1, UInt8, 1),
    INT16(2, Int16, 2),
    UINT16(3, UInt16, 2),
    INT32(4, Int32, 4),
    UINT32(5, UInt32, 4),
    INT64(6, Int64, 8),
    UINT64(7, UInt64, 8);

    private final int index;
    private final ClickHouseDataType dataType;
    private final int byteLength;

    ClickHouseDataTypeMapping(int index, ClickHouseDataType dataType, int byteLength)
    {
        this.dataType = dataType;
        this.byteLength = byteLength;
        this.index = index;
    }

    public int byteLength()
    {
        return byteLength;
    }

    public ClickHouseDataType asClickHouseType()
    {
        return dataType;
    }

    public short getIndex()
    {
        return (short) index;
    }

    public static ClickHouseDataTypeMapping parseFrom(int from)
    {
        for (ClickHouseDataTypeMapping mapping : values()) {
            if (mapping.getIndex() == from) {
                return mapping;
            }
        }

        throw new IllegalArgumentException("Unrecognized clickhouse data type index " + from);
    }
}
