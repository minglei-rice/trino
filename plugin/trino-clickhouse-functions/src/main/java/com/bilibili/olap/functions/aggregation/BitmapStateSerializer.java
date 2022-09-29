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

import com.bilibili.olap.functions.type.BitmapType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import ru.yandex.clickhouse.util.ClickHouseBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BitmapStateSerializer
        implements AccumulatorStateSerializer<BitmapState>
{
    public static final int HEADER_SIZE_IN_BYTES = 1;

    @Override
    public Type getSerializedType()
    {
        return BitmapType.BITMAP_TYPE;
    }

    @Override
    public void serialize(BitmapState state, BlockBuilder out)
    {
        serialize(state.getBitmap(), out, true);
    }

    /**
     * In order to output the Clickhouse compatible bitmap slice, the header bytes should be ignored.
     */
    public void serialize(BitmapWithSmallSet bitmap, BlockBuilder out, boolean needHeader)
    {
        if (bitmap == null) {
            out.appendNull();
        }
        else {
            if (needHeader) {
                writeHeader(out, bitmap);
            }
            writeBitmap(out, bitmap);
        }
    }

    @Override
    public void deserialize(Block block, int index, BitmapState state)
    {
        Slice slice = block.getSlice(index, 0, block.getSliceLength(index));
        state.setBitmap(deserialize(slice));
    }

    /**
     * Only used to deserialize a Trino bitmap slice to the internal bitmap type.
     */
    public BitmapWithSmallSet deserialize(Slice slice)
    {
        byte dataTypeIndex = slice.getByte(0);
        ClickHouseDataTypeMapping dataTypeMapping = ClickHouseDataTypeMapping.parseFrom(dataTypeIndex);
        return deserialize(slice.slice(HEADER_SIZE_IN_BYTES, slice.length() - HEADER_SIZE_IN_BYTES), dataTypeMapping);
    }

    /**
     * Compatible method to deserialize a Clickhouse Bitmap slice to Trino internal bitmap type.
     */
    public BitmapWithSmallSet deserialize(Slice slice, ClickHouseDataTypeMapping dataTypeMapping)
    {
        BitmapWithSmallSet bitmap = new BitmapWithSmallSet(dataTypeMapping);
        int flag = slice.getByte(0);
        if (0 == flag) { // small set
            bitmap.getSmallSet().read(slice.slice(1, slice.length() - 1));
        }
        else {
            try {
                Object internalBitmap = ClickHouseBitmap.deserialize(slice.getBytes(0, slice.length()), dataTypeMapping.asClickHouseType()).unwrap();
                if (internalBitmap instanceof RoaringBitmap) {
                    bitmap = new BitmapWithSmallSet(new IntRoaringBitmapData((RoaringBitmap) internalBitmap), dataTypeMapping);
                }
                else {
                    bitmap = new BitmapWithSmallSet(new LongRoaringBitmapData((Roaring64NavigableMap) internalBitmap), dataTypeMapping);
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to deserialize clickhouse bitmap from binary data", e);
            }
        }
        return bitmap;
    }

    private static void writeHeader(BlockBuilder out, BitmapWithSmallSet bitmap)
    {
        out.writeByte(bitmap.getDataTypeMapping().getIndex());
    }

    private static void writeBitmap(BlockBuilder out, BitmapWithSmallSet bitmap)
    {
        int byteLength = bitmap.getDataTypeMapping().byteLength();
        ByteBuffer serialized;
        if (bitmap.isSmall()) {
            serialized = newBuffer((int) (2 + bitmap.cardinality() * byteLength));
            serialized.put((byte) 0);
            serialized.put((byte) bitmap.cardinality());
            if (bitmap.getDataTypeMapping().byteLength() == 1) {
                byte[] original = bitmap.getSmallSet().getRawArray(byte[].class);
                for (int i = 0; i < bitmap.cardinality(); i++) {
                    serialized.put(original[i]);
                }
            }
            else if (bitmap.getDataTypeMapping().byteLength() == 2) {
                short[] original = bitmap.getSmallSet().getRawArray(short[].class);
                for (int i = 0; i < bitmap.cardinality(); i++) {
                    serialized.putShort(original[i]);
                }
            }
            else if (bitmap.getDataTypeMapping().byteLength() == 4) {
                int[] original = bitmap.getSmallSet().getRawArray(int[].class);
                for (int i = 0; i < bitmap.cardinality(); i++) {
                    serialized.putInt(original[i]);
                }
            }
            else {
                long[] original = bitmap.getSmallSet().getRawArray(long[].class);
                for (int i = 0; i < bitmap.cardinality(); i++) {
                    serialized.putLong(original[i]);
                }
            }
            serialized.flip();
        }
        else {
            if (bitmap.getBitmapData() instanceof IntRoaringBitmapData) {
                serialized = ClickHouseBitmap.wrap(
                        bitmap.getBitmapData().getData(RoaringBitmap.class),
                        bitmap.getDataTypeMapping().asClickHouseType())
                        .toByteBuffer();
            }
            else {
                serialized = ClickHouseBitmap.wrap(
                        bitmap.getBitmapData().getData(Roaring64NavigableMap.class),
                        bitmap.getDataTypeMapping().asClickHouseType())
                        .toByteBuffer();
            }
        }
        Slice buf = Slices.wrappedBuffer(serialized);
        out.writeBytes(buf, 0, buf.length()).closeEntry();
    }

    private static ByteBuffer newBuffer(int capacity)
    {
        return ByteBuffer.allocate(capacity);
    }
}
