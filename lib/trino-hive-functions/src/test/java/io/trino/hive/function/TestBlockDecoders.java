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
package io.trino.hive.function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBlockDecoders
{
    private final TypeOperators typeOperators = new TypeOperators();

    @Test
    public void testBasicTypes()
    {
        //Test Row
        DecimalType decimalType = DecimalType.createDecimalType(30, 10);
        Type rowType = RowType.anonymousRow(
                VARCHAR,
                REAL,
                DOUBLE,
                BIGINT,
                INTEGER,
                SMALLINT,
                TINYINT,
                BOOLEAN,
                decimalType,
                DATE,
                TIMESTAMP_MILLIS,
                VARBINARY);
        ObjectInspector rowInspector = ObjectInspectors.create(rowType, null);

        BlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 12);
        BlockBuilder rowWriter = rowBlockBuilder.beginBlockEntry();
        VARCHAR.writeString(rowWriter, "abc");
        REAL.writeLong(rowWriter, Float.floatToIntBits(1.0001f));
        DOUBLE.writeDouble(rowWriter, 1.0D);
        BIGINT.writeLong(rowWriter, 2L);
        INTEGER.writeLong(rowWriter, 3);
        SMALLINT.writeLong(rowWriter, 4);
        TINYINT.writeLong(rowWriter, 5);
        BOOLEAN.writeBoolean(rowWriter, true);
        BigDecimal bigDecimal = new BigDecimal("-12345678901234567890.1234567890");
        decimalType.writeSlice(rowWriter, encodeScaledValue(bigDecimal));
        LocalDate localDate = LocalDate.now();
        DATE.writeLong(rowWriter, localDate.toEpochDay());
        long time = Calendar.getInstance().getTimeInMillis();
        TIMESTAMP_MILLIS.writeLong(rowWriter, time);
        VARBINARY.writeSlice(rowWriter, Slices.utf8Slice("abc"));
        rowBlockBuilder.closeEntry();

        InputBlockDecoder decoder = InputBlockDecoders.create(rowInspector, rowType);
        List<?> row = (List<?>) decoder.decode(rowBlockBuilder.build(), 0);
        assertEquals(row.size(), 12);
        assertEquals(row.get(0), "abc");
        assertEquals(row.get(1), 1.0001F);
        assertEquals(row.get(2), 1.0D);
        assertEquals(row.get(3), 2L);
        assertEquals(row.get(4), 3);
        assertEquals(row.get(5), (short) 4);
        assertEquals(row.get(6), (byte) 5);
        assertEquals(row.get(7), true);
        assertEquals(row.get(8), HiveDecimal.create(bigDecimal));
        assertEquals(row.get(9), new Date(DAYS.toMillis(localDate.toEpochDay())));
        assertEquals(row.get(10), new Timestamp(time));
        assertEquals(row.get(11), Slices.utf8Slice("abc").getBytes());

        //Test Map
        Type mapType = new MapType(VarcharType.createUnboundedVarcharType(), BIGINT, typeOperators);

        ObjectInspector mapObjectInspector = ObjectInspectors.create(mapType, null);
        assertTrue(mapObjectInspector instanceof StandardMapObjectInspector);

        decoder = InputBlockDecoders.create(mapObjectInspector, mapType);

        BlockBuilder builder = mapType.createBlockBuilder(null, 4);
        // add 3 maps
        for (int i = 0; i < 3; i++) {
            BlockBuilder writer = builder.beginBlockEntry();
            VARCHAR.writeString(writer, "name");
            BIGINT.writeLong(writer, i);
            builder.closeEntry();
        }

        // add 3 keys
        BlockBuilder writer = builder.beginBlockEntry();
        for (int i = 0; i < 3; i++) {
            VARCHAR.writeString(writer, "name" + i);
            BIGINT.writeLong(writer, i);
        }
        builder.closeEntry();

        for (int i = 0; i < 3; i++) {
            Object value = decoder.decode(builder.build(), i);
            assertTrue(value instanceof Map, "mapType should return a map value");
            assertEquals(((Map<?, ?>) value).size(), 1);
            assertTrue(((Map<?, ?>) value).containsKey("name") && ((Map<?, ?>) value).get("name").equals((long) i));
        }

        Map<?, ?> value = (Map<?, ?>) decoder.decode(builder.build(), 3);
        assertEquals(value.size(), 3);

        // Test Array
        Type arrayType = new ArrayType(BIGINT);
        ObjectInspector listObjectInspector = ObjectInspectors.create(arrayType, null);

        BlockBuilder arrayBlockBuilder = arrayType.createBlockBuilder(null, 2);
        BlockBuilder arrayWriter = arrayBlockBuilder.beginBlockEntry();
        BIGINT.writeLong(arrayWriter, 1);
        BIGINT.writeLong(arrayWriter, 2);
        arrayBlockBuilder.closeEntry();

        decoder = InputBlockDecoders.create(listObjectInspector, arrayType);

        List<?> list = (List<?>) decoder.decode(arrayBlockBuilder.build(), 0);
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), 1L);
        assertEquals(list.get(1), 2L);
    }

    @Test
    public void testNestedArray()
    {
        verifyBlockEquals(createNestedArrayType(), buildNestedArray(), new int[][]{new int[] {1, 2, 3, 4, 5, 6}});
    }

    private void verifyBlockEquals(Type arrayType, Block block, int[][] values)
    {
        ObjectInspector arrayObjectInspector = ObjectInspectors.create(arrayType, null);
        assertTrue(arrayObjectInspector instanceof StandardListObjectInspector);

        InputBlockDecoder decoder = InputBlockDecoders.create(arrayObjectInspector, arrayType);
        List<?> list = (List<?>) decoder.decode(block, 0);

        assertEquals(block.getPositionCount(), list.size());
        for (int i = 0; i < list.size(); i++) {
            assertTrue(list.get(i) instanceof List);
            List<?> inner = (List<?>) list.get(i);
            for (int j = 0; j < inner.size(); j++) {
                assertEquals(inner.get(j), values[i][j]);
            }
        }
    }

    private Type createNestedArrayType()
    {
        Type subArrayType = new ArrayType(INTEGER);
        return new ArrayType(subArrayType);
    }

    private Block buildNestedArray()
    {
        Type arrayType = createNestedArrayType();
        Type subArrayType = ((ArrayType) arrayType).getElementType();

        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 1);
        BlockBuilder writer = blockBuilder.beginBlockEntry();

        BlockBuilder subBlockBuilder = subArrayType.createBlockBuilder(null, 3);
        BlockBuilder subWriter = subBlockBuilder.beginBlockEntry();
        subWriter.writeInt(1);
        subWriter.writeInt(2);
        subWriter.writeInt(3);
        subBlockBuilder.closeEntry();

        subWriter = subBlockBuilder.beginBlockEntry();
        subWriter.writeInt(4);
        subWriter.writeInt(5);
        subWriter.writeInt(6);
        subBlockBuilder.closeEntry();

        arrayType.writeObject(writer, subBlockBuilder.build().getChildren().get(0));
        blockBuilder.closeEntry();

        return blockBuilder.build();
    }

    @Test
    public void testNestedMap()
    {
        Map<String, Map<String, Long>> expected = ImmutableMap.of("city", ImmutableMap.of("ShangHai", 1L));
        verifyBlockEquals(createNestedMapType(), buildNestedMap(), expected);
    }

    private void verifyBlockEquals(Type mapType, Block block, Map<String, Map<String, Long>> expected)
    {
        ObjectInspector mapObjectInspector = ObjectInspectors.create(mapType, null);
        assertTrue(mapObjectInspector instanceof StandardMapObjectInspector);

        InputBlockDecoder decoder = InputBlockDecoders.create(mapObjectInspector, mapType);
        Map<?, ?> values = (Map<?, ?>) decoder.decode(block, 0);

        assertEquals(values, expected);
    }

    private Type createNestedMapType()
    {
        Type subMapType = new MapType(VARCHAR, BIGINT, typeOperators);
        return new MapType(VarcharType.createUnboundedVarcharType(), subMapType, typeOperators);
    }

    private Block buildNestedMap()
    {
        Type mapType = createNestedMapType();
        Type valueType = ((MapType) mapType).getValueType();

        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 1);
        BlockBuilder writer = blockBuilder.beginBlockEntry();

        BlockBuilder subBlockBuilder = valueType.createBlockBuilder(null, 1);
        BlockBuilder subWriter = subBlockBuilder.beginBlockEntry();
        VARCHAR.writeString(subWriter, "ShangHai");
        BIGINT.writeLong(subWriter, 1);
        subBlockBuilder.closeEntry();

        VARCHAR.writeString(writer, "city");
        valueType.writeObject(writer, valueType.getObject(subBlockBuilder.build(), 0));
        blockBuilder.closeEntry();

        return blockBuilder.build();
    }

    @Test
    public void testNestedRow()
    {
        Type nestedArrayType = createNestedArrayType();
        Type nestedMapType = createNestedMapType();
        Type rowType = RowType.anonymousRow(nestedArrayType, nestedMapType);

        BlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 1);
        BlockBuilder rowWriter = rowBlockBuilder.beginBlockEntry();
        nestedArrayType.writeObject(rowWriter, nestedArrayType.getObject(buildNestedArray(), 0));
        nestedMapType.writeObject(rowWriter, nestedMapType.getObject(buildNestedMap(), 0));
        rowBlockBuilder.closeEntry();

        ObjectInspector rowInspector = ObjectInspectors.create(rowType, null);
        assertTrue(rowInspector instanceof StructObjectInspector);

        InputBlockDecoder decoder = InputBlockDecoders.create(rowInspector, rowType);
        Object value = decoder.decode(rowBlockBuilder.build(), 0);
        assertTrue(value instanceof List);

        List<?> list = (List<?>) value;
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), ImmutableList.of(ImmutableList.of(1, 2, 3, 4, 5, 6)));
        assertEquals(list.get(1), ImmutableMap.of("city", ImmutableMap.of("ShangHai", 1L)));
    }
}
