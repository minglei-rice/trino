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
import com.bilibili.olap.functions.aggregation.LongRoaringBitmapData;
import com.bilibili.olap.functions.type.BitmapType;
import com.bilibili.olap.functions.type.ClickHouseBitmapType;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.bilibili.olap.functions.aggregation.ClickHouseDataTypeMapping.INT16;
import static com.bilibili.olap.functions.aggregation.ClickHouseDataTypeMapping.INT64;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createSlicesBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;

public class TestClickhouseFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        functionAssertions.installPlugin(new ClickHouseFunctionsPlugin());
    }

    @Test
    public void testAgg()
    {
        // small set
        // TODO: To test SMALLINT, currently there is no way to create short blocks.
        assertAggregation(functionAssertions.getFunctionResolution(),
                QualifiedName.of("groupBitmap"),
                fromTypes(INTEGER),
                new SqlVarbinary(new byte[] {4, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2}),
                createIntsBlock(1, 2));

        assertAggregation(functionAssertions.getFunctionResolution(),
                QualifiedName.of("groupBitmap"),
                fromTypes(BIGINT),
                new SqlVarbinary(new byte[] {6, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2}),
                createLongsBlock(1, 2));

        // bitmap data
        long[] values = new long[33];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }

        byte[] serializedData = toSlice(true, makeBitmap(BitmapType.BITMAP_TYPE, INT64, values)).getBytes();
        assertEquals(serializedData, toSlice(true, makeBitmap(BitmapType.BITMAP_TYPE, INT64, values)).getBytes());

        assertAggregation(functionAssertions.getFunctionResolution(),
                QualifiedName.of("groupBitmap"),
                fromTypes(BIGINT),
                new SqlVarbinary(serializedData),
                createLongsBlock(Arrays.stream(values).boxed().collect(Collectors.toList())));
    }

    @Test
    public void testMerge()
    {
        assertAggregation(functionAssertions.getFunctionResolution(),
                QualifiedName.of("merge"),
                fromTypes(BitmapType.BITMAP_TYPE),
                new SqlVarbinary(new byte[] {6, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2}),
                createSlicesBlock(Slices.wrappedBuffer(new byte[] {6, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2})));
    }

    @Test
    public void testCastAndCardinality() throws IOException
    {
        short[] shortValues = new short[] {1, 2, 3, 4};
        // test SMALLINT
        Slice serializedSlice16 = readAsBytes(true, INT16, shortValues);
        String castToSmallSet = String.format("CAST(X'%s' AS %s)", BaseEncoding.base16().encode(serializedSlice16.getBytes()), BitmapType.NAME);
        assertFunction("cardinality(" + castToSmallSet + ")", BIGINT, (long) shortValues.length);
        Slice serializedCKSlice16 = readAsBytes(false, INT16, shortValues);
        assertEquals(ClickHouseBitmapSerializer.deserialize(serializedCKSlice16.getBytes(), INT16).getCardinality(), (long) shortValues.length);

        // 33 elements in 2 containers
        long cardinality = 33;
        long[] longValues = new long[(int) cardinality];
        for (int i = 1; i <= longValues.length - 1; i++) {
            longValues[i] = i;
        }
        longValues[longValues.length - 1] = 4294967296L;

        Slice serializedSlice = toSlice(true, makeBitmap(BitmapType.BITMAP_TYPE, INT64, longValues));
        String castToBitmap = String.format("CAST(X'%s' AS %s)", BaseEncoding.base16().encode(serializedSlice.getBytes()), BitmapType.NAME);
        assertFunction(castToBitmap, BitmapType.BITMAP_TYPE, new SqlVarbinary(serializedSlice.getBytes()));
        assertFunction("cardinality(" + castToBitmap + ")", BIGINT, cardinality);

        byte[] serializedCKSlice = toSlice(false, makeBitmap(ClickHouseBitmapType.CLICK_HOUSE_BITMAP_TYPE, INT64, longValues)).getBytes();
        String expectedBytesInBase16 = "01720200000000000000000000003A3000000100000000001F001000000000000100020003000400050006000700080009000A000B000C000D000E000F0010001100120013001400150016001700180019001A001B001C001D001E001F00010000003A3000000100000000000000100000000000";
        assertEquals(BaseEncoding.base16().encode(serializedCKSlice), expectedBytesInBase16);

        String castToCKBitmap = String.format("CAST(X'%s' AS %s)", BaseEncoding.base16().encode(serializedSlice.getBytes()), ClickHouseBitmapType.NAME);
        assertFunction(castToCKBitmap, ClickHouseBitmapType.CLICK_HOUSE_BITMAP_TYPE, new SqlVarbinary(serializedCKSlice));

        String castFromBitmap = String.format("CAST(CAST(X'%s' AS %s) AS %s)",
                BaseEncoding.base16().encode(serializedSlice.getBytes()),
                BitmapType.NAME,
                ClickHouseBitmapType.NAME);
        assertFunction(castFromBitmap, ClickHouseBitmapType.CLICK_HOUSE_BITMAP_TYPE, new SqlVarbinary(serializedCKSlice));

        assertEquals(ClickHouseBitmapSerializer.deserialize(serializedCKSlice, INT64).getCardinality(), cardinality);
    }

    private Slice readAsBytes(boolean needHeader, ClickHouseDataTypeMapping dataType, short... values)
    {
        BitmapWithSmallSet bitmap = new BitmapWithSmallSet(dataType);
        for (short v : values) {
            bitmap.add(v);
        }
        return toSlice(needHeader, bitmap);
    }

    private BitmapWithSmallSet makeBitmap(Type bitmapType, ClickHouseDataTypeMapping dataType, long... values)
    {
        LongRoaringBitmapData longRoaringBitmapData = new LongRoaringBitmapData(values);
        BitmapWithSmallSet bitmap = new BitmapWithSmallSet(longRoaringBitmapData, dataType);
        if (bitmapType == ClickHouseBitmapType.CLICK_HOUSE_BITMAP_TYPE) {
            return ClickHouseBitmapAdapter.toClickHouseBitmap(bitmap);
        }
        return bitmap;
    }

    private Slice toSlice(boolean needHeader, BitmapWithSmallSet bitmap)
    {
        BitmapStateSerializer serializer = new BitmapStateSerializer();
        BlockBuilder blockBuilder = ClickHouseBitmapType.CLICK_HOUSE_BITMAP_TYPE.createBlockBuilder(null, 1);
        serializer.serialize(bitmap, blockBuilder, needHeader);
        Block expectedBlock = blockBuilder.build();
        return expectedBlock.getSlice(0, 0, expectedBlock.getSliceLength(0));
    }
}
