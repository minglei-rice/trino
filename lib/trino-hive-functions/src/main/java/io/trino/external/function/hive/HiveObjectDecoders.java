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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.slice.Slices;
import io.trino.external.function.DecimalUtils;
import io.trino.external.function.OutputObjectDecoder;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DuplicateMapKeyException;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.external.function.hive.HiveFunctionErrorCode.unsupportedType;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Float.floatToRawIntBits;

/**
 * Decode Trino type value to Hive type value.
 */
public class HiveObjectDecoders
{
    private HiveObjectDecoders()
    {
    }

    public static OutputObjectDecoder create(Type type, ObjectInspector inspector)
    {
        String base = type.getTypeSignature().getBase();
        switch (base) {
            case StandardTypes.BIGINT:
                verify(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Long) o));
            case StandardTypes.INTEGER:
                verify(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Integer) o).longValue());
            case StandardTypes.SMALLINT:
                verify(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Short) o).longValue());
            case StandardTypes.TINYINT:
                verify(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Byte) o).longValue());
            case StandardTypes.BOOLEAN:
                verify(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Boolean) o));
            case StandardTypes.DATE:
                verify(inspector instanceof PrimitiveObjectInspector);
                return compose(writableDate -> {
                    if (inspector instanceof WritableDateObjectInspector) {
                        return ((WritableDateObjectInspector) inspector).getPrimitiveWritableObject(writableDate);
                    }
                    else {
                        return primitive(inspector);
                    }
                }, o -> {
                    if (o instanceof DateWritableV2) {
                        return TimeUnit.SECONDS.toDays(((DateWritableV2) o).getTimeInSeconds());
                    }
                    else {
                        return TimeUnit.MICROSECONDS.toDays(((Date) o).getTime());
                    }
                });
            case StandardTypes.DECIMAL:
                if (Decimals.isShortDecimal(type)) {
                    DecimalType decimalType = (DecimalType) type;
                    return compose(decimal(inspector), o -> DecimalUtils.encodeToLong((BigDecimal) o, decimalType));
                }
                else if (Decimals.isLongDecimal(type)) {
                    DecimalType decimalType = (DecimalType) type;
                    return compose(decimal(inspector), o -> DecimalUtils.encodeToSlice((BigDecimal) o, decimalType));
                }
                break;
            case StandardTypes.REAL:
                verify(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> floatToRawIntBits(((Number) o).floatValue()));
            case StandardTypes.DOUBLE:
                verify(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> (Double) o);
            case StandardTypes.TIMESTAMP:
                verify(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Timestamp) o).getTime());
            case StandardTypes.VARBINARY:
                if (inspector instanceof BinaryObjectInspector) {
                    return compose(primitive(inspector), o -> Slices.wrappedBuffer(((byte[]) o)));
                }
                break;
            case StandardTypes.VARCHAR:
                if (inspector instanceof StringObjectInspector) {
                    return compose(primitive(inspector), o -> Slices.utf8Slice(o.toString()));
                }
                else if (inspector instanceof HiveVarcharObjectInspector) {
                    return compose(o -> ((HiveVarcharObjectInspector) inspector).getPrimitiveJavaObject(o).getValue(),
                            o -> Slices.utf8Slice(((String) o)));
                }
                break;
            case StandardTypes.CHAR:
                if (inspector instanceof StringObjectInspector) {
                    return compose(primitive(inspector), o -> Slices.utf8Slice(o.toString()));
                }
                else if (inspector instanceof HiveCharObjectInspector) {
                    return compose(o -> ((HiveCharObjectInspector) inspector).getPrimitiveJavaObject(o).getValue(),
                            o -> Slices.utf8Slice(((String) o)));
                }
                break;
            case StandardTypes.ROW:
                return StructOutputObjectDecoder.create(type, inspector);
            case StandardTypes.ARRAY:
                return ListOutputObjectDecoder.create(type, inspector);
            case StandardTypes.MAP:
                return MapOutputObjectDecoder.create(type, inspector);
        }
        throw unsupportedType(type);
    }

    private static OutputObjectDecoder compose(Function<Object, Object> inspector, Function<Object, Object> encoder)
    {
        return o -> {
            if (o != null) {
                Object inspected = inspector.apply(o);
                if (inspected != null) {
                    return encoder.apply(inspected);
                }
            }
            return null;
        };
    }

    private static Function<Object, Object> primitive(ObjectInspector inspector)
    {
        return ((PrimitiveObjectInspector) inspector)::getPrimitiveJavaObject;
    }

    private static Function<Object, Object> decimal(ObjectInspector inspector)
    {
        return o -> ((HiveDecimalObjectInspector) inspector).getPrimitiveJavaObject(o).bigDecimalValue();
    }

    public static class ListOutputObjectDecoder
            implements OutputObjectDecoder
    {
        private final ListObjectInspector listInspector;
        private final Type elementType;
        private final BlockObjectWriter writer;

        public static ListOutputObjectDecoder create(Type type, ObjectInspector inspector)
        {
            checkArgument(inspector instanceof ListObjectInspector && type instanceof ArrayType);

            Type elementType = ((ArrayType) type).getElementType();
            ListObjectInspector listInspector = (ListObjectInspector) inspector;
            OutputObjectDecoder elementEncoder = HiveObjectDecoders.create(elementType,
                    listInspector.getListElementObjectInspector());
            return new ListOutputObjectDecoder(listInspector, elementType, elementEncoder);
        }

        private ListOutputObjectDecoder(ListObjectInspector listInspector, Type elementType, OutputObjectDecoder elementEncoder)
        {
            this.listInspector = listInspector;
            this.elementType = elementType;
            this.writer = new SimpleBlockObjectWriter(elementEncoder, elementType);
        }

        @Override
        public Object decode(Object o)
        {
            if (o == null) {
                return null;
            }
            final int length = listInspector.getListLength(o);
            final BlockBuilder blockBuilder = elementType.createBlockBuilder(null, length);
            for (int i = 0; i < length; i++) {
                writer.write(blockBuilder, listInspector.getListElement(o, i));
            }
            return blockBuilder.build();
        }
    }

    public static class MapOutputObjectDecoder
            implements OutputObjectDecoder
    {
        private final MapType mapType;
        private final MapObjectInspector mapObjectInspector;
        private final BlockObjectWriter keyWriter;
        private final BlockObjectWriter valueWriter;

        public static MapOutputObjectDecoder create(Type type, Object inspector)
        {
            checkArgument(type instanceof MapType &&
                    inspector instanceof MapObjectInspector);
            return new MapOutputObjectDecoder(((MapType) type), ((MapObjectInspector) inspector));
        }

        private MapOutputObjectDecoder(MapType type, MapObjectInspector inspector)
        {
            this.mapType = type;
            this.mapObjectInspector = inspector;
            Type keyType = type.getKeyType();
            Type valueType = type.getValueType();
            OutputObjectDecoder keyEncoder = HiveObjectDecoders.create(keyType, inspector.getMapKeyObjectInspector());
            OutputObjectDecoder valueEncoder = HiveObjectDecoders.create(valueType, inspector.getMapValueObjectInspector());
            this.keyWriter = createBlockObjectWriter(keyEncoder, keyType);
            this.valueWriter = createBlockObjectWriter(valueEncoder, valueType);
        }

        @Override
        public Object decode(Object object)
        {
            if (object == null) {
                return null;
            }
            Map<?, ?> rawMap = mapObjectInspector.getMap(object);

            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, rawMap.size());
            BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();
            for (Entry<?, ?> entry : rawMap.entrySet()) {
                if (entry.getKey() == null) {
                    mapBlockBuilder.closeEntry();
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
                }
                // TODO check indeterminate
                keyWriter.write(blockBuilder, entry.getKey());
                valueWriter.write(blockBuilder, entry.getValue());
            }
            try {
                mapBlockBuilder.closeEntryStrict();
            }
            catch (DuplicateMapKeyException e) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
            }
            return mapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
        }
    }

    public static class StructOutputObjectDecoder
            implements OutputObjectDecoder
    {
        private final RowType type;
        private final StructObjectInspector inspector;
        private final List<BlockObjectWriter> fieldWriters;

        public static StructOutputObjectDecoder create(Type type, Object inspector)
        {
            checkArgument((type instanceof RowType) && (inspector instanceof StructObjectInspector));
            return new StructOutputObjectDecoder((RowType) type, (StructObjectInspector) inspector);
        }

        private static BlockObjectWriter createFieldBlockObjectWriter(Type type, StructField field)
        {
            OutputObjectDecoder encoder = HiveObjectDecoders.create(type, field.getFieldObjectInspector());
            return createBlockObjectWriter(encoder, type);
        }

        public StructOutputObjectDecoder(RowType type, StructObjectInspector inspector)
        {
            this.type = type;
            this.inspector = inspector;
            this.fieldWriters = Streams.zip(
                    type.getFields().stream().map(RowType.Field::getType),
                    inspector.getAllStructFieldRefs().stream(),
                    StructOutputObjectDecoder::createFieldBlockObjectWriter)
                    .collect(Collectors.toList());
        }

        @Override
        public Object decode(Object object)
        {
            if (object == null) {
                return null;
            }

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
            RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) pageBuilder.getBlockBuilder(0);
            SingleRowBlockWriter blockBuilder = rowBlockBuilder.beginBlockEntry();
            List<Object> fieldObjects = inspector.getStructFieldsDataAsList(object);
            final int totalNumField = fieldWriters.size();
            final int numField = fieldObjects.size();
            for (int i = 0; i < totalNumField; i++) {
                fieldWriters.get(i).write(blockBuilder, i < numField ? fieldObjects.get(i) : null);
            }
            rowBlockBuilder.closeEntry();
            pageBuilder.declarePosition();
            return type.getObject(rowBlockBuilder, rowBlockBuilder.getPositionCount() - 1);
        }
    }

    private static BlockObjectWriter createBlockObjectWriter(OutputObjectDecoder encoder, Type type)
    {
        return new SimpleBlockObjectWriter(encoder, type);
    }

    private interface BlockObjectWriter
    {
        void write(BlockBuilder out, Object object);
    }

    private static class SimpleBlockObjectWriter
            implements BlockObjectWriter
    {
        private final OutputObjectDecoder outputObjectDecoder;
        private final Type objectType;

        private SimpleBlockObjectWriter(OutputObjectDecoder outputObjectDecoder, Type objectType)
        {
            this.outputObjectDecoder = outputObjectDecoder;
            this.objectType = objectType;
        }

        @Override
        public void write(BlockBuilder out, Object object)
        {
            if (object != null) {
                Object encoded = outputObjectDecoder.decode(object);
                if (encoded != null) {
                    writeNativeValue(objectType, out, encoded);
                    return;
                }
            }
            out.appendNull();
        }
    }
}
