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

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static java.lang.String.format;

public enum HiveFunctionErrorCode
        implements ErrorCodeSupplier
{
    UNSUPPORTED_HIVE_OBJECT_INSPECTOR(1, EXTERNAL),
    UNSUPPORTED_TRINO_TYPE(2, EXTERNAL),
    UNSUPPORTED_FUNCTION(4, EXTERNAL),
    INITIALIZATION_ERROR(18, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    HiveFunctionErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0110_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }

    public static TrinoException initializationError(Throwable t, String msg)
    {
        return new TrinoException(INITIALIZATION_ERROR, msg, t);
    }

    public static TrinoException functionNotFound(String funcName, String msg)
    {
        return new TrinoException(FUNCTION_NOT_FOUND, format("Function %s is not registered, msg: %s", funcName, msg));
    }

    public static TrinoException unsupportedType(Type type)
    {
        return new TrinoException(UNSUPPORTED_TRINO_TYPE, "Unsupported Trino type " + type);
    }

    public static TrinoException unsupportedFunction(Class<?> cls)
    {
        return new TrinoException(UNSUPPORTED_FUNCTION, "Unsupported function " + cls.getName() + " / " + cls.getSuperclass().getName());
    }
}
