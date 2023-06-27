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

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum HiveFunctionErrorCode
        implements ErrorCodeSupplier
{
    HIVE_FUNCTION_UNSUPPORTED_HIVE_TYPE(1, EXTERNAL),
    HIVE_FUNCTION_UNSUPPORTED_TRINO_TYPE(2, EXTERNAL),
    HIVE_FUNCTION_UNSUPPORTED_SIGNATURE(3, EXTERNAL),
    HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE(4, EXTERNAL),
    HIVE_FUNCTION_IMPLEMENTATION_MISSING(17, EXTERNAL),
    HIVE_FUNCTION_INITIALIZATION_ERROR(18, EXTERNAL),
    HIVE_FUNCTION_EXECUTION_ERROR(19, EXTERNAL),
    HIVE_FUNCTION_NOT_FOUND(20, EXTERNAL),
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
}
