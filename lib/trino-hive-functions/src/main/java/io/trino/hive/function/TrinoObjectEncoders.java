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

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.lang.reflect.Constructor;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.hive.function.HiveFunctionErrorCode.HIVE_FUNCTION_UNSUPPORTED_TRINO_TYPE;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.CHAR;
import static io.trino.spi.type.StandardTypes.DATE;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.MAP;
import static io.trino.spi.type.StandardTypes.REAL;
import static io.trino.spi.type.StandardTypes.ROW;
import static io.trino.spi.type.StandardTypes.SMALLINT;
import static io.trino.spi.type.StandardTypes.TIMESTAMP;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.StandardTypes.VARBINARY;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.Float.intBitsToFloat;

/**
 * Encode Hive type value to Trino type value.
 */
public final class TrinoObjectEncoders
{
    private TrinoObjectEncoders()
    {
    }

    public static InputObjectEncoder create(Type type, TypeManager typeManager)
    {
        String base = type.getTypeSignature().getBase();
        switch (base) {
            case "unknown":
                return o -> o;
            case BIGINT:
                return o -> (Long) o;
            case INTEGER:
                return o -> ((Long) o).intValue();
            case SMALLINT:
                return o -> ((Long) o).shortValue();
            case TINYINT:
                return o -> ((Long) o).byteValue();
            case BOOLEAN:
                return o -> (Boolean) o;
            case DATE:
                return DateTimeUtils::createDate;
            case REAL:
                return o -> intBitsToFloat(((Number) o).intValue());
            case DOUBLE:
                return o -> ((Double) o);
            case TIMESTAMP:
                return o -> {
                    try {
                        LocalDateTime localDateTime = new Timestamp((Long.parseLong(o.toString().substring(0, 13)))).toLocalDateTime();
                        Constructor<?> hiveTimestampConstructor =
                                Class.forName("org.apache.hadoop.hive.common.type.Timestamp").getDeclaredConstructor(LocalDateTime.class);
                        hiveTimestampConstructor.setAccessible(true);
                        return hiveTimestampConstructor.newInstance(localDateTime);
                    }
                    catch (Exception e) {
                        throw new RuntimeException("Failed to convert to Hive timestamp", e);
                    }
                };
            case VARBINARY:
                return o -> ((Slice) o).getBytes();
            case VARCHAR:
                return o -> ((Slice) o).toStringUtf8();
            case CHAR:
                return o -> ((Slice) o).toStringUtf8();
            case ROW:
                return RowInputObjectEncoder.create(((RowType) type), typeManager);
            case ARRAY:
                return ArrayInputObjectEncoder.create(((ArrayType) type), typeManager);
            case MAP:
                return MapInputObjectEncoder.create(((MapType) type), typeManager);
        }

        throw new TrinoException(HIVE_FUNCTION_UNSUPPORTED_TRINO_TYPE, "Unsupported Trino type " + type);
    }

    public static InputBlockDecoder createBlockInputDecoder(Type type, TypeManager typeManager)
    {
        return InputBlockDecoders.create(ObjectInspectors.create(type, typeManager), type);
    }

    private static class RowInputObjectEncoder
            implements InputObjectEncoder
    {
        private final List<InputBlockDecoder> fieldDecoders;

        private static RowInputObjectEncoder create(RowType type, TypeManager typeManager)
        {
            List<InputBlockDecoder> fieldDecoders = type
                    .getFields()
                    .stream()
                    .map(f -> createBlockInputDecoder(f.getType(), typeManager))
                    .collect(Collectors.toList());
            return new RowInputObjectEncoder(fieldDecoders);
        }

        private RowInputObjectEncoder(List<InputBlockDecoder> fieldDecoders)
        {
            this.fieldDecoders = fieldDecoders;
        }

        @Override
        public Object encode(Object object)
        {
            if (object == null) {
                return null;
            }
            Block block = (Block) object;
            List<Object> list = new ArrayList<>(fieldDecoders.size());
            int numField = fieldDecoders.size();
            for (int i = 0; i < numField; i++) {
                list.add(fieldDecoders.get(i).decode(block, i));
            }
            return list;
        }
    }

    private static class ArrayInputObjectEncoder
            implements InputObjectEncoder
    {
        private final InputBlockDecoder elementDecoder;

        private static ArrayInputObjectEncoder create(ArrayType type, TypeManager typeManager)
        {
            return new ArrayInputObjectEncoder(createBlockInputDecoder(type.getElementType(), typeManager));
        }

        private ArrayInputObjectEncoder(InputBlockDecoder elementDecoder)
        {
            this.elementDecoder = elementDecoder;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public Object encode(Object object)
        {
            if (object == null) {
                return null;
            }
            Block block = (Block) object;
            int count = block.getPositionCount();
            List list = new ArrayList(count);
            for (int i = 0; i < count; i++) {
                list.add(elementDecoder.decode(block, i));
            }
            return list;
        }
    }

    private static class MapInputObjectEncoder
            implements InputObjectEncoder
    {
        private final InputBlockDecoder keyDecoder;
        private final InputBlockDecoder valueDecoder;

        private static MapInputObjectEncoder create(MapType type, TypeManager typeManager)
        {
            return new MapInputObjectEncoder(
                    createBlockInputDecoder(type.getKeyType(), typeManager),
                    createBlockInputDecoder(type.getValueType(), typeManager));
        }

        private MapInputObjectEncoder(InputBlockDecoder keyDecoder, InputBlockDecoder valueDecoder)
        {
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public Object encode(Object object)
        {
            if (object == null) {
                return null;
            }
            Block block = (Block) object;
            Map map = new HashMap();
            for (int i = 0; i < block.getPositionCount(); i += 2) {
                Object key = keyDecoder.decode(block, i);
                if (key != null) {
                    Object value = valueDecoder.decode(block, i + 1);
                    map.put(key, value);
                }
            }
            return map;
        }
    }
}
