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

import com.bilibili.olap.functions.aggregation.BitmapState;
import com.bilibili.olap.functions.aggregation.BitmapStateFactory;
import com.bilibili.olap.functions.aggregation.BitmapStateSerializer;
import com.bilibili.olap.functions.aggregation.BitmapWithSmallSet;
import com.bilibili.olap.functions.aggregation.ByteSet;
import com.bilibili.olap.functions.aggregation.ClickHouseDataTypeMapping;
import com.bilibili.olap.functions.aggregation.IntRoaringBitmapData;
import com.bilibili.olap.functions.aggregation.IntSet;
import com.bilibili.olap.functions.aggregation.LongRoaringBitmapData;
import com.bilibili.olap.functions.aggregation.RoaringBitmapData;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SingleArrayBlockWriter;
import io.trino.spi.block.VariableWidthBlockBuilder;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestBasics
{
    @Test
    public void testGenerateBitmapWithSmallSet()
    {
        BitmapWithSmallSet byteSmallSet = new BitmapWithSmallSet(ClickHouseDataTypeMapping.INT8);
        assertTrue(byteSmallSet.isSmall(), "The initial bitmap should be small");
        byteSmallSet.add((byte) 1);
        byteSmallSet.add((byte) 2);
        assertEquals(byteSmallSet.cardinality(), 2);

        BitmapWithSmallSet shortSmallSet = new BitmapWithSmallSet(ClickHouseDataTypeMapping.INT16);
        assertTrue(shortSmallSet.isSmall(), "The initial bitmap should be small");
        shortSmallSet.add((short) 1);
        shortSmallSet.add((short) 2);
        assertEquals(shortSmallSet.cardinality(), 2);

        BitmapWithSmallSet intSmallSet = new BitmapWithSmallSet(ClickHouseDataTypeMapping.INT32);
        assertTrue(intSmallSet.isSmall(), "The initial bitmap should be small");
        intSmallSet.add(1);
        intSmallSet.add(2);
        assertEquals(intSmallSet.cardinality(), 2);

        BitmapWithSmallSet longSmallSet = new BitmapWithSmallSet(ClickHouseDataTypeMapping.INT64);
        assertTrue(longSmallSet.isSmall(), "The initial bitmap should be small");
        longSmallSet.add(1L);
        longSmallSet.add(2L);
        assertEquals(longSmallSet.cardinality(), 2);

        BitmapWithSmallSet longSmallSet2 = new BitmapWithSmallSet(ClickHouseDataTypeMapping.INT64);
        longSmallSet.add(3L);
        longSmallSet.add(4L);

        longSmallSet.merge(longSmallSet2);
        assertTrue(longSmallSet.isSmall(), "The long set should be small after merged");
        assertEquals(longSmallSet.cardinality(), 4);

        assertThatThrownBy(() -> intSmallSet.merge(byteSmallSet)).isInstanceOf(ClassCastException.class);

        RoaringBitmapData intRoaringBitmapData = new IntRoaringBitmapData();
        BitmapWithSmallSet intBitmap = new BitmapWithSmallSet(intRoaringBitmapData, ClickHouseDataTypeMapping.INT32);
        assertTrue(intBitmap.isLarge(), "The initial bitmap should be large");
        intBitmap.add(1);
        intBitmap.add(2);
        assertEquals(intBitmap.cardinality(), 2);

        intSmallSet.merge(intBitmap);
        assertTrue(intSmallSet.isLarge(), "bitmap data should be large after merged with a large one!");
        assertEquals(intSmallSet.cardinality(), 2);

        RoaringBitmapData longRoaringBitmapData = new LongRoaringBitmapData();
        BitmapWithSmallSet longBitmap = new BitmapWithSmallSet(longRoaringBitmapData, ClickHouseDataTypeMapping.INT64);
        longBitmap.add(1L);
        longBitmap.add(2L);
        assertEquals(longBitmap.cardinality(), 2);

        longBitmap.merge(longSmallSet);
        assertTrue(longBitmap.isLarge(), "bitmap data should be large after merged with a large one!");
        assertEquals(longBitmap.cardinality(), 4);

        assertThatThrownBy(() -> intBitmap.merge(longBitmap)).isInstanceOf(ClassCastException.class);
    }

    @Test
    public void testSerialization()
    {
        // small set
        BitmapWithSmallSet byteSmallSet = new BitmapWithSmallSet(ClickHouseDataTypeMapping.INT8);
        byteSmallSet.add((byte) 1);
        byteSmallSet.add((byte) 2);

        Block binary = serialize(byteSmallSet);
        assertTrue(binary.getPositionCount() > 0);

        BlockBuilder expectedBlock = new VariableWidthBlockBuilder(null, 1, 0);
        SingleArrayBlockWriter writer = new SingleArrayBlockWriter(expectedBlock, 0);
        writer.writeByte(ClickHouseDataTypeMapping.INT8.getIndex());
        writer.writeByte(0); // flag small or bitmap
        writer.writeByte(2); // cardinality
        writer.writeByte(1); // element
        writer.writeByte(2); // ...
        writer.closeEntry();
        blockEquals(binary, expectedBlock.build());

        BitmapState resultState = new BitmapStateFactory.SingleBitmapState();
        deserialize(binary, resultState);

        assertTrue(resultState.getBitmap().isSmall());
        assertNull(resultState.getBitmap().getBitmapData());
        assertTrue(resultState.getBitmap().getSmallSet() instanceof ByteSet);
        assertEquals(resultState.getBitmap().cardinality(), 2);

        // bitmap data to small set
        RoaringBitmapData intRoaringBitmapData = new IntRoaringBitmapData();
        BitmapWithSmallSet intBitmap = new BitmapWithSmallSet(intRoaringBitmapData, ClickHouseDataTypeMapping.INT32);
        intBitmap.add(1);
        intBitmap.add(2);

        binary = serialize(intBitmap);
        resultState = new BitmapStateFactory.SingleBitmapState();
        deserialize(binary, resultState);

        assertTrue(resultState.getBitmap().isSmall(),
                "bitmap with few elements should be serialized and deserialized as small set");
        assertNull(resultState.getBitmap().getBitmapData());
        assertTrue(resultState.getBitmap().getSmallSet() instanceof IntSet);
        assertEquals(resultState.getBitmap().cardinality(), 2);

        // bitmap data
        RoaringBitmapData longRoaringBitmapData = new LongRoaringBitmapData();
        BitmapWithSmallSet longBitmap = new BitmapWithSmallSet(longRoaringBitmapData, ClickHouseDataTypeMapping.INT64);
        for (long i = 0; i <= byteSmallSet.getSmallSet().getCapacity(); i++) {
            longBitmap.add(i);
        }

        binary = serialize(longBitmap);
        resultState = new BitmapStateFactory.SingleBitmapState();
        deserialize(binary, resultState);

        assertTrue(resultState.getBitmap().isLarge());
        assertNull(resultState.getBitmap().getSmallSet());
        assertTrue(resultState.getBitmap().getBitmapData() instanceof LongRoaringBitmapData);
        assertEquals(resultState.getBitmap().cardinality(), byteSmallSet.getSmallSet().getCapacity() + 1);
    }

    private static Block serialize(BitmapWithSmallSet bitmap)
    {
        BitmapStateSerializer serializer = new BitmapStateSerializer();
        BitmapState bitmapState = new BitmapStateFactory().createSingleState();
        bitmapState.setBitmap(bitmap);
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 0);
        serializer.serialize(bitmapState, blockBuilder);
        return blockBuilder.build();
    }

    private static void deserialize(Block binary, BitmapState bitmapState)
    {
        BitmapStateSerializer serializer = new BitmapStateSerializer();
        serializer.deserialize(binary, 0, bitmapState);
    }

    private void blockEquals(Block block, Block expected)
    {
        assertEquals(block.getPositionCount(), expected.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            assertTrue(block.equals(i, 0, expected, i, 0, block.getSliceLength(i)));
        }
    }
}
