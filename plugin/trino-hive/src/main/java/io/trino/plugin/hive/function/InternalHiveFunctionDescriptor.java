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

import io.trino.spi.function.FunctionDescriptor;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.TypeSignatureParameter.namedTypeParameter;
import static io.trino.spi.type.TypeSignatureParameter.typeParameter;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static java.lang.String.format;

/**
 * This class is to transform the real argument types of the Hive function into the parametric type signatures,
 * for example, varchar(10) will be rewritten to varchar(x), in which x is a variable symbol representing an
 * arbitrary integer in [0, Integer.MAX_VALUE].
 *
 * But now the implementation is not good at transforming the function which could accept multiple elements,
 * e.g. map, because it's arguments are arbitrary types and have any number of elements, so it's not easy to
 * recognise the concrete semantic of each argument.
 * For example,
 * {{
 *     select map('k1','v1','k2','v2')
 * }}
 * will return the signature
 * {{
 *   map(varchar(x1), varchar(x2), varchar(x3), varchar(x4))
 * }}
 * by the internal parser, of which argument types are same, however the arguments have different definitions,
 * in which the first is a map key, the second is the value bind to the first key, the third is a map key, ...,
 * and so on.
 *
 * Note: If wrote the same function with different argument types, each function will be treated as a new
 * function definition, so this will cause the memory pressure.
 *
 * TODO: Fold argument types and coerce types if possible.
 */
public class InternalHiveFunctionDescriptor
        implements FunctionDescriptor
{
    private final FunctionDescriptor functionDescriptor;
    private final List<TypeSignature> argumentSignatures;
    int counter;

    public InternalHiveFunctionDescriptor(FunctionDescriptor functionDescriptor, TypeManager typeManager)
    {
        this.functionDescriptor = functionDescriptor;
        this.argumentSignatures = transform(typeManager, functionDescriptor.getArgumentSignatures());
    }

    private List<TypeSignature> transform(TypeManager typeManager, List<TypeSignature> arguments)
    {
        List<TypeSignature> formattedArguments = new ArrayList<>();
        for (TypeSignature signature : arguments) {
            List<TypeSignatureParameter> parameters = signature.getParameters().stream().map(p -> lookupTypeParameter(p, typeManager)).collect(toImmutableList());
            formattedArguments.add(new TypeSignature(signature.getBase(), parameters));
        }
        return formattedArguments;
    }

    private TypeSignatureParameter lookupTypeParameter(TypeSignatureParameter parameter, TypeManager typeManager)
    {
        switch (parameter.getKind()) {
            case TYPE: {
                List<TypeSignatureParameter> newParameters = parameter.getTypeSignature().getParameters().stream().map(p -> lookupTypeParameter(p, typeManager)).collect(toImmutableList());
                return typeParameter(new TypeSignature(parameter.getTypeSignature().getBase(), newParameters));
            }
            case LONG:
                return typeVariable("x" + (++counter));
            case NAMED_TYPE: {
                return namedTypeParameter(lookupNamedTypeSignature(parameter.getNamedTypeSignature(), typeManager));
            }
            case VARIABLE:
                return parameter;
        }
        throw new UnsupportedOperationException(format("Unsupported parameter [%s]", parameter));
    }

    private NamedTypeSignature lookupNamedTypeSignature(NamedTypeSignature namedTypeSignature, TypeManager typeManager)
    {
        List<TypeSignatureParameter> parameters = namedTypeSignature.getTypeSignature().getParameters().stream().map(p -> lookupTypeParameter(p, typeManager)).collect(toImmutableList());
        return new NamedTypeSignature(namedTypeSignature.getFieldName(), new TypeSignature(namedTypeSignature.getTypeSignature().getBase(), parameters));
    }

    @Override
    public String getFunctionKey()
    {
        return functionDescriptor.getFunctionKey();
    }

    @Override
    public Type getReturnType()
    {
        return functionDescriptor.getReturnType();
    }

    @Override
    public List<TypeSignature> getArgumentSignatures()
    {
        return argumentSignatures;
    }

    @Override
    public List<Type> getArgumentTypes()
    {
        return functionDescriptor.getArgumentTypes();
    }

    @Override
    public MethodHandle getMethodHandle()
    {
        return functionDescriptor.getMethodHandle();
    }

    @Override
    public InvocationConvention getInvocationConvention()
    {
        return functionDescriptor.getInvocationConvention();
    }

    @Override
    public boolean isDeterministic()
    {
        return functionDescriptor.isDeterministic();
    }

    @Override
    public String description()
    {
        return functionDescriptor.description();
    }

    @Override
    public String example()
    {
        return functionDescriptor.example();
    }

    @Override
    public FunctionDescriptor getInternalFunctionDescriptor(TypeManager typeManager)
    {
        return this;
    }
}
