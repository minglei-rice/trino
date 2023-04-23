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
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.util.ClickHouseBitmap;

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

        byte[] serializedData = readAsBytes(true, INT64, values).getBytes();
        assertEquals(serializedData, readAsBytes(true, INT64, values).getBytes());

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
        assertEquals(ClickHouseBitmap.deserialize(serializedCKSlice16.getBytes(), ClickHouseDataType.Int16).getCardinality(), (long) shortValues.length);

        long cardinality = 33;
        long[] longValues = new long[(int) cardinality];
        for (int i = 0; i < longValues.length; i++) {
            longValues[i] = i;
        }

        Slice serializedSlice = readAsBytes(true, INT64, longValues);
        String castToBitmap = String.format("CAST(X'%s' AS %s)", BaseEncoding.base16().encode(serializedSlice.getBytes()), BitmapType.NAME);
        assertFunction(castToBitmap, BitmapType.BITMAP_TYPE, new SqlVarbinary(serializedSlice.getBytes()));
        assertFunction("cardinality(" + castToBitmap + ")", BIGINT, cardinality);

        byte[] serializedCKSlice = readAsBytes(false, INT64, longValues).getBytes();
        String castToCKBitmap = String.format("CAST(X'%s' AS %s)", BaseEncoding.base16().encode(serializedSlice.getBytes()), ClickHouseBitmapType.NAME);
        assertFunction(castToCKBitmap, ClickHouseBitmapType.CLICK_HOUSE_BITMAP_TYPE, new SqlVarbinary(serializedCKSlice));

        String castFromBitmap = String.format("CAST(CAST(X'%s' AS %s) AS %s)",
                BaseEncoding.base16().encode(serializedSlice.getBytes()),
                BitmapType.NAME,
                ClickHouseBitmapType.NAME);
        assertFunction(castFromBitmap, ClickHouseBitmapType.CLICK_HOUSE_BITMAP_TYPE, new SqlVarbinary(serializedCKSlice));

        assertEquals(ClickHouseBitmap.deserialize(serializedCKSlice, ClickHouseDataType.Int64).getCardinality(), cardinality);
    }

    private Slice readAsBytes(boolean needHeader, ClickHouseDataTypeMapping dataType, short... values)
    {
        BitmapWithSmallSet bitmap = new BitmapWithSmallSet(dataType);
        for (short v : values) {
            bitmap.add(v);
        }
        BitmapStateSerializer serializer = new BitmapStateSerializer();
        BlockBuilder blockBuilder = ClickHouseBitmapType.CLICK_HOUSE_BITMAP_TYPE.createBlockBuilder(null, values.length);
        serializer.serialize(bitmap, blockBuilder, needHeader);
        Block expectedBlock = blockBuilder.build();
        return expectedBlock.getSlice(0, 0, expectedBlock.getSliceLength(0));
    }

    private Slice readAsBytes(boolean needHeader, ClickHouseDataTypeMapping dataType, long... values)
    {
        LongRoaringBitmapData longRoaringBitmapData = new LongRoaringBitmapData(values);
        BitmapWithSmallSet bitmap = new BitmapWithSmallSet(longRoaringBitmapData, dataType);
        BitmapStateSerializer serializer = new BitmapStateSerializer();
        BlockBuilder blockBuilder = ClickHouseBitmapType.CLICK_HOUSE_BITMAP_TYPE.createBlockBuilder(null, values.length);
        serializer.serialize(bitmap, blockBuilder, needHeader);
        Block expectedBlock = blockBuilder.build();
        return expectedBlock.getSlice(0, 0, expectedBlock.getSliceLength(0));
    }
}
