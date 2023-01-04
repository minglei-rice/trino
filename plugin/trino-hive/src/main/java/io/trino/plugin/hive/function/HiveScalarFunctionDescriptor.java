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
package io.trino.plugin.hive.function;

import io.trino.external.function.hive.HiveScalarFunctionInvoker;
import io.trino.spi.function.ExternalFunctionKey;
import io.trino.spi.function.FunctionDescriptor;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;

/**
 * Contains all the info to describe a hive function.
 */
public class HiveScalarFunctionDescriptor
        implements FunctionDescriptor
{
    private final ExternalFunctionKey functionKey;
    private final List<TypeSignature> argumentSignatures;
    private final List<Type> argumentTypes;
    private final Type returnType;
    private final boolean deterministic;
    private final String description;
    private final String example;

    private final MethodHandle methodHandle;
    private final InvocationConvention invocationConvention;

    private HiveScalarFunctionDescriptor(
            ExternalFunctionKey functionKey,
            List<TypeSignature> argumentSignatures,
            List<Type> argumentTypes,
            Type returnType,
            boolean deterministic,
            String description,
            String example,
            MethodHandle methodHandle)
    {
        this.functionKey = functionKey;
        this.argumentSignatures = argumentSignatures;
        this.argumentTypes = argumentTypes;
        this.returnType = returnType;
        this.deterministic = deterministic;
        this.description = description;
        this.example = example;
        this.methodHandle = methodHandle;
        List<InvocationConvention.InvocationArgumentConvention> argumentsConvention = getNullableArgumentProperties(argumentTypes);
        // TODO: Always nullable return?
        InvocationConvention.InvocationReturnConvention returnConvention = InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
        invocationConvention = new InvocationConvention(argumentsConvention, returnConvention, false, false);
    }

    public static HiveScalarFunctionDescriptor makeFunctionDescriptor(
            ClassLoader classLoader,
            ExternalFunctionKey functionKey,
            boolean deterministic,
            String description,
            String example,
            HiveScalarFunctionInvoker invoker,
            TypeManager typeManager)
    {
        MethodHandle methodHandle =
                ScalarMethodHandles.generateUnbound(classLoader, invoker.getResultType(), invoker.getArgumentTypes(), typeManager)
                        .bindTo(invoker);
        return new HiveScalarFunctionDescriptor(
                functionKey,
                invoker.getArguments(),
                invoker.getArgumentTypes(),
                invoker.getResultType(),
                deterministic,
                description,
                example,
                methodHandle);
    }

    private static List<InvocationConvention.InvocationArgumentConvention> getNullableArgumentProperties(List<Type> evalParamTypes)
    {
        List<InvocationConvention.InvocationArgumentConvention> nullableArgProps = new ArrayList<>(evalParamTypes.size());
        // TODO: Always nullable?
        for (Type type : evalParamTypes) {
            // nullableArgProps.add(type.getJavaType().isPrimitive() ? BOXED_NULLABLE : RETURN_NULL_ON_NULL);
            nullableArgProps.add(BOXED_NULLABLE);
        }
        return nullableArgProps;
    }

    @Override
    public String getFunctionKey()
    {
        return functionKey.toString();
    }

    @Override
    public Type getReturnType()
    {
        return returnType;
    }

    @Override
    public List<TypeSignature> getArgumentSignatures()
    {
        return argumentSignatures;
    }

    @Override
    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    @Override
    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }

    @Override
    public InvocationConvention getInvocationConvention()
    {
        return invocationConvention;
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public String description()
    {
        return description;
    }

    @Override
    public String example()
    {
        return example;
    }

    @Override
    public FunctionDescriptor getInternalFunctionDescriptor(TypeManager typeManager)
    {
        return new InternalHiveFunctionDescriptor(this, typeManager);
    }
}
