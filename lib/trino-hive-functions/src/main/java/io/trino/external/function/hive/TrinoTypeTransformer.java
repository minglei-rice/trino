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

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static io.trino.external.function.hive.HiveFunctionErrorCode.unsupportedType;
import static java.util.Objects.requireNonNull;

/**
 * Cast Hive type to Trino type.
 */
public final class TrinoTypeTransformer
{
    private TrinoTypeTransformer()
    {
    }

    public static DecimalType createDecimalType(TypeSignature type)
    {
        requireNonNull(type);
        verify(StandardTypes.DECIMAL.equals(type.getBase()) && type.getParameters().size() == 2,
                "Invalid decimal type " + type);
        int precision = type.getParameters().get(0).getLongLiteral().intValue();
        int scale = type.getParameters().get(1).getLongLiteral().intValue();
        return DecimalType.createDecimalType(precision, scale);
    }

    public static Type fromObjectInspector(ObjectInspector inspector, TypeManager typeManager)
    {
        switch (inspector.getCategory()) {
            case PRIMITIVE:
                verify(inspector instanceof PrimitiveObjectInspector);
                return fromPrimitive((PrimitiveObjectInspector) inspector);
            case LIST:
                verify(inspector instanceof ListObjectInspector);
                return fromList(((ListObjectInspector) inspector), typeManager);
            case MAP:
                verify(inspector instanceof MapObjectInspector);
                return fromMap(((MapObjectInspector) inspector), typeManager);
            case STRUCT:
                verify(inspector instanceof StructObjectInspector);
                return fromStruct(((StructObjectInspector) inspector), typeManager);
            default:
                throw unsupportedType(inspector);
        }
    }

    private static Type fromPrimitive(PrimitiveObjectInspector inspector)
    {
        switch (inspector.getPrimitiveCategory()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case BYTE:
                return TinyintType.TINYINT;
            case SHORT:
                return SmallintType.SMALLINT;
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case STRING:
                return VarcharType.VARCHAR;
            case DATE:
                return DateType.DATE;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            case BINARY:
                return VarbinaryType.VARBINARY;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) inspector.getTypeInfo();
                return DecimalType.createDecimalType(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
            case VARCHAR:
                return VarcharType.createVarcharType(((VarcharTypeInfo) inspector.getTypeInfo()).getLength());
            case CHAR:
                return CharType.createCharType(((VarcharTypeInfo) inspector.getTypeInfo()).getLength());
            case VOID:
            case TIMESTAMPLOCALTZ:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case UNKNOWN:
            default:
                throw unsupportedType(inspector);
        }
    }

    private static Type fromList(ListObjectInspector inspector, TypeManager typeManager)
    {
        ObjectInspector elementInspector = inspector.getListElementObjectInspector();
        return new ArrayType(fromObjectInspector(elementInspector, typeManager));
    }

    private static Type fromMap(MapObjectInspector inspector, TypeManager typeManager)
    {
        Type keyType = fromObjectInspector(inspector.getMapKeyObjectInspector(), typeManager);
        Type valueType = fromObjectInspector(inspector.getMapValueObjectInspector(), typeManager);
        return typeManager.getType(new TypeSignature(
                StandardTypes.MAP,
                TypeSignatureParameter.typeParameter(keyType.getTypeSignature()),
                TypeSignatureParameter.typeParameter(valueType.getTypeSignature())));
    }

    private static Type fromStruct(StructObjectInspector inspector, TypeManager typeManager)
    {
        List<RowType.Field> fields = ((StructObjectInspector) inspector)
                .getAllStructFieldRefs()
                .stream()
                .map(sf -> RowType.field(sf.getFieldName(), fromObjectInspector(sf.getFieldObjectInspector(), typeManager)))
                .collect(Collectors.toList());
        return RowType.from(fields);
    }
}
