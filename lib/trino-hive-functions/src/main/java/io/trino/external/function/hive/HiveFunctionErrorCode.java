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

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static java.lang.String.format;

public enum HiveFunctionErrorCode
        implements ErrorCodeSupplier
{
    HIVE_FUNCTION_UNSUPPORTED_HIVE_TYPE(1, EXTERNAL),
    HIVE_FUNCTION_UNSUPPORTED_PRESTO_TYPE(2, EXTERNAL),
    HIVE_FUNCTION_UNSUPPORTED_SIGNATURE(3, EXTERNAL),
    HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE(4, EXTERNAL),
    HIVE_FUNCTION_IMPLEMENTATION_MISSING(17, EXTERNAL),
    HIVE_FUNCTION_INITIALIZATION_ERROR(18, EXTERNAL),
    HIVE_FUNCTION_EXECUTION_ERROR(19, EXTERNAL),
    /**/;

    private static final int ERROR_CODE_MASK = 0x0110_0000;

    private final ErrorCode errorCode;

    HiveFunctionErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + ERROR_CODE_MASK, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }

    public static TrinoException unsupportedType(Type type)
    {
        return new TrinoException(HIVE_FUNCTION_UNSUPPORTED_PRESTO_TYPE, "Unsupported Presto type " + type);
    }

    public static TrinoException unsupportedType(TypeSignature type)
    {
        return new TrinoException(HIVE_FUNCTION_UNSUPPORTED_PRESTO_TYPE, "Unsupported Presto type " + type);
    }

    public static TrinoException unsupportedType(ObjectInspector inspector)
    {
        return new TrinoException(HIVE_FUNCTION_UNSUPPORTED_HIVE_TYPE, "Unsupported Hive type " + inspector);
    }

    public static TrinoException unsupportedFunctionType(Class<?> cls)
    {
        return new TrinoException(HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE, "Unsupported function type "
                + cls.getName() + " / " + cls.getSuperclass().getName());
    }

    public static TrinoException unsupportedFunctionType(Class<?> cls, Throwable t)
    {
        return new TrinoException(HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE, "Unsupported function type "
                + cls.getName() + " / " + cls.getSuperclass().getName(), t);
    }

    public static TrinoException functionNotFound(String name)
    {
        return functionNotFound(name, "");
    }

    public static TrinoException functionNotFound(String name, String reason)
    {
        return new TrinoException(FUNCTION_NOT_FOUND, format("Function %s not registered. Reason: %s", name, reason));
    }

    public static TrinoException functionNotFound(String name, String reason, Throwable throwable)
    {
        return new TrinoException(FUNCTION_NOT_FOUND, format("Function %s not registered. Reason: %s", name, reason), throwable);
    }

    public static TrinoException functionNotFound(String name, String formatStr, Object... args)
    {
        return functionNotFound(name, format(formatStr, args));
    }

    public static TrinoException initializationError(Throwable t)
    {
        return new TrinoException(HIVE_FUNCTION_INITIALIZATION_ERROR, t);
    }

    public static TrinoException initializationError(Throwable t, String msg)
    {
        return new TrinoException(HIVE_FUNCTION_INITIALIZATION_ERROR, msg, t);
    }

    public static TrinoException executionError(Throwable t)
    {
        return new TrinoException(HIVE_FUNCTION_EXECUTION_ERROR, t);
    }
}
