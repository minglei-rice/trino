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
package io.trino.external.function.hive;

import com.google.common.collect.Streams;
import io.trino.external.function.InputBlockDecoder;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.external.function.DateTimeUtils.createDate;
import static io.trino.external.function.DecimalUtils.readHiveDecimal;
import static io.trino.external.function.hive.HiveFunctionErrorCode.unsupportedType;
import static io.trino.external.function.hive.HiveTypeTransformer.createHiveChar;
import static io.trino.external.function.hive.HiveTypeTransformer.createHiveVarChar;
import static java.lang.Float.intBitsToFloat;

public class InputBlockDecoders
{
    private InputBlockDecoders()
    {
    }

    public static InputBlockDecoder create(ObjectInspector inspector, Type type)
    {
        if (inspector instanceof ConstantObjectInspector) {
            Object constant = ((ConstantObjectInspector) inspector).getWritableConstantValue();
            return (b, i) -> constant;
        }
        if (inspector instanceof PrimitiveObjectInspector) {
            return createForPrimitive(((PrimitiveObjectInspector) inspector), type);
        }
        else if (inspector instanceof StandardStructObjectInspector) {
            verify(type instanceof RowType);
            return createForStruct(((StandardStructObjectInspector) inspector), ((RowType) type));
        }
        else if (inspector instanceof SettableStructObjectInspector) {
            return createForStruct(((SettableStructObjectInspector) inspector), ((RowType) type));
        }
        else if (inspector instanceof StructObjectInspector) {
            return createForStruct(((StructObjectInspector) inspector), ((RowType) type));
        }
        else if (inspector instanceof ListObjectInspector) {
            verify(type instanceof ArrayType);
            return createForList(((ListObjectInspector) inspector), ((ArrayType) type));
        }
        else if (inspector instanceof MapObjectInspector) {
            verify(type instanceof MapType);
            return createForMap(((MapObjectInspector) inspector), ((MapType) type));
        }
        throw unsupportedType(inspector);
    }

    private static InputBlockDecoder createForPrimitive(PrimitiveObjectInspector inspector, Type type)
    {
        boolean preferWritable = inspector.preferWritable();
        if (inspector instanceof StringObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new Text(type.getSlice(b, i).getBytes()) :
                    (b, i) -> b.isNull(i) ? null : type.getSlice(b, i).toStringUtf8();
        }
        else if (inspector instanceof IntObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new IntWritable(((int) type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : ((int) type.getLong(b, i));
        }
        else if (inspector instanceof BooleanObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new BooleanWritable(type.getBoolean(b, i)) :
                    (b, i) -> b.isNull(i) ? null : type.getBoolean(b, i);
        }
        else if (inspector instanceof FloatObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new FloatWritable(intBitsToFloat((int) type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : intBitsToFloat((int) type.getLong(b, i));
        }
        else if (inspector instanceof DoubleObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new DoubleWritable(type.getDouble(b, i)) :
                    (b, i) -> b.isNull(i) ? null : type.getDouble(b, i);
        }
        else if (inspector instanceof LongObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new LongWritable(type.getLong(b, i)) :
                    (b, i) -> b.isNull(i) ? null : type.getLong(b, i);
        }
        else if (inspector instanceof ShortObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new ShortWritable(((short) type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : ((short) type.getLong(b, i));
        }
        else if (inspector instanceof ByteObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new ByteWritable(((byte) type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : ((byte) type.getLong(b, i));
        }
        else if (inspector instanceof JavaHiveVarcharObjectInspector) {
            return (b, i) -> b.isNull(i) ? null : createHiveVarChar(type.getSlice(b, i).toStringUtf8());
        }
        else if (inspector instanceof JavaHiveCharObjectInspector) {
            return (b, i) -> b.isNull(i) ? null : createHiveChar(type.getSlice(b, i).toStringUtf8());
        }
        else if (inspector instanceof JavaHiveDecimalObjectInspector) {
            verify(type instanceof DecimalType);
            return (b, i) -> b.isNull(i) ? null : readHiveDecimal(((DecimalType) type), b, i);
        }
        else if (inspector instanceof JavaDateObjectInspector) {
            return (b, i) -> b.isNull(i) ? null : new Date(TimeUnit.DAYS.toMillis(type.getLong(b, i)));
        }
        else if (inspector instanceof JavaTimestampObjectInspector) {
            return (b, i) -> b.isNull(i) ? null : new Timestamp(type.getLong(b, i));
        }
        else if (inspector instanceof HiveDecimalObjectInspector) {
            verify(type instanceof DecimalType);
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new HiveDecimalWritable(readHiveDecimal(((DecimalType) type), b, i)) :
                    (b, i) -> b.isNull(i) ? null : readHiveDecimal(((DecimalType) type), b, i);
        }
        else if (inspector instanceof BinaryObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new BytesWritable(type.getSlice(b, i).getBytes()) :
                    (b, i) -> b.isNull(i) ? null : type.getSlice(b, i).getBytes();
        }
        else if (inspector instanceof DateObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new DateWritable(((int) type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : createDate(((int) type.getLong(b, i)));
        }
        else if (inspector instanceof TimestampObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new TimestampWritable(new Timestamp(type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : new Timestamp(type.getLong(b, i));
        }
        else if (inspector instanceof VoidObjectInspector) {
            return (b, i) -> null;
        }
        throw unsupportedType(inspector);
    }

    private static InputBlockDecoder createForStruct(SettableStructObjectInspector structInspector, RowType rowType)
    {
        List<? extends StructField> structFields = structInspector.getAllStructFieldRefs();
        List<RowType.Field> rowFields = rowType.getFields();
        verify(rowFields.size() == structFields.size());
        List<InputBlockDecoder> fieldDecoders = Streams.zip(
                structFields.stream(),
                rowFields.stream(),
                (sf, rf) -> create(sf.getFieldObjectInspector(), rf.getType()))
                .collect(Collectors.toList());
        final int numFields = structFields.size();
        return (b, i) -> {
            checkArgument(b.getChildren().size() == numFields, String.format("Row block should have %s childs", numFields));
            if (b.isNull(i)) {
                return null;
            }
            Block row = b.getObject(i, Block.class);
            Object result = structInspector.create();
            for (int j = 0; j < numFields; j++) {
                structInspector.setStructFieldData(result,
                        structFields.get(j),
                        fieldDecoders.get(j).decode(row, j));
            }
            return result;
        };
    }

    private static InputBlockDecoder createForStruct(StructObjectInspector structInspector, RowType rowType)
    {
        List<? extends StructField> structFields = structInspector.getAllStructFieldRefs();
        List<RowType.Field> rowFields = rowType.getFields();
        verify(rowFields.size() == structFields.size());
        List<InputBlockDecoder> fieldDecoders = Streams.zip(
                structFields.stream(),
                rowFields.stream(),
                (sf, rf) -> create(sf.getFieldObjectInspector(), rf.getType()))
                .collect(Collectors.toList());
        final int numFields = structFields.size();
        return (b, i) -> {
            checkArgument(b.getChildren().size() == numFields, String.format("Row block should have %s childs", numFields));
            if (b.isNull(i)) {
                return null;
            }
            Block row = b.getObject(i, Block.class);
            ArrayList<Object> result = new ArrayList<>(numFields);
            for (int j = 0; j < numFields; j++) {
                result.add(fieldDecoders.get(j).decode(row, j));
            }
            return result;
        };
    }

    private static InputBlockDecoder createForList(ListObjectInspector inspector, ArrayType arrayType)
    {
        Type elementType = arrayType.getElementType();
        ObjectInspector elementInspector = inspector.getListElementObjectInspector();
        InputBlockDecoder decoder = create(elementInspector, elementType);
        return (b, i) -> {
            if (b.isNull(i)) {
                return null;
            }
            Block array = b.getObject(i, Block.class);
            int positions = array.getPositionCount();
            ArrayList<Object> result = new ArrayList<>(positions);
            for (int j = 0; j < positions; j++) {
                // need raw block
                result.add(decoder.decode(array, j));
            }
            return result;
        };
    }

    private static InputBlockDecoder createForMap(MapObjectInspector inspector, MapType mapType)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        ObjectInspector keyInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueInspector = inspector.getMapValueObjectInspector();
        InputBlockDecoder keyDecoder = create(keyInspector, keyType);
        InputBlockDecoder valueDecoder = create(valueInspector, valueType);
        return (b, i) -> {
            // key is null
            if (b.isNull(i)) {
                return null;
            }
            Block map = b.getObject(i, Block.class);
            HashMap<Object, Object> result = new HashMap<>();
            for (int j = 0; j < map.getPositionCount(); j += 2) {
                if (map.isNull(j)) {
                    // skip null keys
                    continue;
                }
                Object key = keyDecoder.decode(map, j);
                Object value = valueDecoder.decode(map, j + 1);
                result.put(key, value);
            }
            return result;
        };
    }
}
