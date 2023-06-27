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
package io.trino.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDescriptor;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.QualifiedFunctionName;
import io.trino.spi.function.Signature;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.analyzer.TypeSignatureProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.FunctionKind.SCALAR;

public class HiveFunctionResolver
{
    private static final String DEFAULT_FUNCTION_RESOLVER_NAME = "hive";

    private final Map<String, FunctionResolverManager> externalFunctionManagers = new ConcurrentHashMap<>(1);

    private HiveFunctionResolver() {}

    public static HiveFunctionResolver getInstance()
    {
        return new HiveFunctionResolver();
    }

    public FunctionResolverManager putIfAbsent(String key, FunctionResolverManager value)
    {
        return externalFunctionManagers.putIfAbsent(key, value);
    }

    public boolean contains(String key)
    {
        return externalFunctionManagers.containsKey(key);
    }

    public FunctionResolverManager get(String key)
    {
        return externalFunctionManagers.get(key);
    }

    public ResolvedFunction resolveFunction(
            GlobalFunctionCatalog functions,
            TypeManager typeManager,
            Session session,
            QualifiedFunctionName name,
            List<TypeSignatureProvider> parameterTypes)
    {
        FunctionResolverManager functionResolverManager = get(HiveFunctionResolver.DEFAULT_FUNCTION_RESOLVER_NAME);
        final List<TypeSignature> pTypes = parameterTypes
                .stream()
                .map(TypeSignatureProvider::getTypeSignature)
                .collect(Collectors.toList());
        // first search in hive built-in function, TODO if not found, find in hive metastore.
        FunctionDescriptor functionDescriptor = functionResolverManager.getScalarFunctionDescriptor(
                session,
                HiveFunctionResolver.DEFAULT_FUNCTION_RESOLVER_NAME,
                resolveFunctionName(name),
                pTypes);
        FunctionDescriptor internalFunctionDescriptor = functionDescriptor.getInternalFunctionDescriptor(typeManager);

        Signature signature = Signature.builder()
                .name(internalFunctionDescriptor.getFunctionKey())
                .returnType(internalFunctionDescriptor.getReturnType().getTypeSignature())
                .argumentTypes(internalFunctionDescriptor.getArgumentSignatures()).build();

        List<Boolean> argumentsNullable = internalFunctionDescriptor.getInvocationConvention()
                .getArgumentConventions()
                .stream()
                .map(InvocationConvention.InvocationArgumentConvention::isNullable)
                .collect(toImmutableList());
        FunctionNullability nullability = new FunctionNullability(
                internalFunctionDescriptor.getInvocationConvention().getReturnConvention().isNullable(),
                argumentsNullable);

        ResolvedFunction resolvedFunction = new ResolvedFunction(
                new BoundSignature(signature.getName(), internalFunctionDescriptor.getReturnType(), internalFunctionDescriptor.getArgumentTypes()),
                GlobalSystemConnector.CATALOG_HANDLE,
                FunctionId.toFunctionId(signature),
                SCALAR,
                internalFunctionDescriptor.isDeterministic(),
                nullability,
                ImmutableMap.of(),
                ImmutableSet.of());

        FunctionMetadata.Builder builder = FunctionMetadata.builder(resolvedFunction.getFunctionKind())
                .functionId(resolvedFunction.getFunctionId())
                .signature(signature)
                .canonicalName(signature.getName())
                .argumentNullability(argumentsNullable)
                .description(internalFunctionDescriptor.description());

        if (!internalFunctionDescriptor.isDeterministic()) {
            builder.nondeterministic();
        }

        if (internalFunctionDescriptor.getInvocationConvention().getReturnConvention().isNullable()) {
            builder.nullable();
        }
        FunctionMetadata functionMetadata = builder.build();
        SpecializedSqlScalarFunction scalarFunction = functionResolverManager.getScalarFunctionImplementation(internalFunctionDescriptor);
        HiveSqlScalarFunction hiveSqlScalarFunction = new HiveSqlScalarFunction(functionMetadata, scalarFunction);
        InternalFunctionBundle internalFunctionBundle = new InternalFunctionBundle(hiveSqlScalarFunction);
        for (FunctionMetadata metadata : internalFunctionBundle.getFunctions()) {
            for (FunctionMetadata existingFunction : functions.listFunctions()) {
                if (existingFunction.getFunctionId().equals(metadata.getFunctionId())) {
                    return resolvedFunction;
                }
            }
        }
        functions.addFunctions(internalFunctionBundle);
        return resolvedFunction;
    }

    private static String resolveFunctionName(QualifiedFunctionName name)
    {
        return name.getFunctionName();
    }
}
