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
package io.trino.spi.function;

import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.List;

public interface FunctionDescriptor
{
    String getFunctionKey();

    Type getReturnType();

    List<TypeSignature> getArgumentSignatures();

    List<Type> getArgumentTypes();

    MethodHandle getMethodHandle();

    InvocationConvention getInvocationConvention();

    boolean isDeterministic();

    default String description()
    {
        return "";
    }

    String example();

    FunctionDescriptor getInternalFunctionDescriptor(TypeManager typeManager);
}
