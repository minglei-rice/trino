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
package com.bilibili.olap.functions;

import com.bilibili.olap.functions.aggregation.BitmapStateSerializer;
import com.bilibili.olap.functions.aggregation.BitmapWithSmallSet;
import com.bilibili.olap.functions.aggregation.ClickHouseDataTypeMapping;
import org.roaringbitmap.longlong.ClickHouseRoaring64NavigableMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.util.ClickHouseBitmap;
import ru.yandex.clickhouse.util.Utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class ClickHouseBitmapSerializer
{
    private ClickHouseBitmapSerializer() {}

    /**
     * Override {@link ClickHouseBitmap#deserialize(byte[], ClickHouseDataType)} method to store the data as
     * an instance of {@link ClickHouseRoaring64NavigableMap} instead of {@link Roaring64NavigableMap}.
     */
    public static ClickHouseBitmap deserialize(byte[] bytes, ClickHouseDataTypeMapping dataType) throws IOException
    {
        if (bytes[0] != (byte) 0 && dataType.byteLength() > 4) {
            ByteBuffer buffer = BufferUtils.newLittleEndianBuffer(bytes.length);
            buffer = (ByteBuffer) ((Buffer) buffer.put(bytes)).flip();
            // skip the flag byte
            buffer.get();

            int len = Utils.readVarInt(buffer);
            if (buffer.remaining() < len) {
                throw new IllegalStateException(
                        "Need " + len + " bytes to deserialize ClickHouseBitmap but only got " + buffer.remaining());
            }

            // consume map size(long in little-endian byte order)
            byte[] bitmaps = new byte[4];
            buffer.get(bitmaps);

            if (buffer.get() != 0 || buffer.get() != 0 || buffer.get() != 0 || buffer.get() != 0) {
                throw new IllegalStateException(
                        "Not able to deserialize ClickHouseBitmap for too many bitmaps(>" + 0xFFFFFFFFL + ")!");
            }
            // replace the last 5 bytes to flag(boolean for signed/unsigned) and map
            // size(integer)
            ((Buffer) buffer).position(buffer.position() - 5);
            // always unsigned due to limit of CRoaring
            buffer.put((byte) 0);
            // big-endian -> little-endian
            for (int i = 3; i >= 0; i--) {
                buffer.put(bitmaps[i]);
            }

            ((Buffer) buffer).position(buffer.position() - 5);
            bitmaps = new byte[buffer.remaining()];
            buffer.get(bitmaps);
            Roaring64NavigableMap b = new ClickHouseRoaring64NavigableMap();
            b.deserialize(new DataInputStream(new ByteArrayInputStream(bitmaps)));
            return ClickHouseBitmap.wrap(b, dataType.asClickHouseType());
        }
        else {
            return ClickHouseBitmap.deserialize(bytes, dataType.asClickHouseType());
        }
    }

    public static ByteBuffer serialize(BitmapWithSmallSet bitmap)
    {
        BitmapWithSmallSet ckBitmap = ClickHouseBitmapAdapter.toClickHouseBitmap(bitmap);
        return BitmapStateSerializer.writeBitmap(ckBitmap, BufferUtils::newLittleEndianBuffer);
    }
}
