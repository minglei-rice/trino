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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.operator.scalar.ScalarFunctionImplementation;
import io.trino.spi.function.ExternalFunctionKey;
import io.trino.spi.function.FunctionDescriptor;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.MetadataManager.verifyMethodHandleSignature;

/**
 * Supports resolving function name with the format catalogName.resolverName.functionName, for examples:
 *   hive.hive.max -- hive catalog, hive resolver, max function
 *   hive.max      -- hive resolver, max function
 *   max           -- max function
 */
public class HiveFunctionManagerResolver
{
    private static final String DEFAULT_CATALOG_NAME_TO_RESOLVE = "hive";
    private static final String DEFAULT_FUNCTION_RESOLVER_NAME = "hive";

    private final Map<String, HiveFunctionManager> hiveFunctionManagers = new ConcurrentHashMap<>();

    private HiveFunctionManagerResolver() {}

    public static HiveFunctionManagerResolver getInstance()
    {
        return new HiveFunctionManagerResolver();
    }

    public HiveFunctionManager putIfAbsent(String key, HiveFunctionManager value)
    {
        return hiveFunctionManagers.putIfAbsent(key, value);
    }

    public boolean contains(String key)
    {
        return hiveFunctionManagers.containsKey(key);
    }

    public HiveFunctionManager get(String key)
    {
        return hiveFunctionManagers.get(key);
    }

    /**
     * Resolve the function name {@link QualifiedName} and try to register the resolved function
     * into {@link FunctionRegistry}.
     */
    public ResolvedFunction resolvedFunction(
            FunctionRegistry functionRegistry,
            TypeManager typeManager,
            Session session,
            QualifiedName name,
            List<TypeSignatureProvider> parameterTypes)
    {
        String catalogName = resolveCatalogName(session, name);
        HiveFunctionManager hiveFunctionManager = get(catalogName);
        final List<TypeSignature> pTypes = parameterTypes
                .stream()
                .map(TypeSignatureProvider::getTypeSignature)
                .collect(Collectors.toList());

        FunctionDescriptor functionDescriptor = hiveFunctionManager.getScalarFunctionDescriptor(
                session,
                catalogName,
                resolveResolverName(name),
                resolveFunctionName(name),
                pTypes);
        FunctionDescriptor internalFunctionDescriptor = functionDescriptor.getInternalFunctionDescriptor(typeManager);
        Signature signature = new Signature(
                internalFunctionDescriptor.getFunctionKey(),
                internalFunctionDescriptor.getReturnType().getTypeSignature(),
                internalFunctionDescriptor.getArgumentSignatures());
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
                FunctionId.toFunctionId(signature),
                SCALAR,
                internalFunctionDescriptor.isDeterministic(),
                nullability,
                ImmutableMap.of(),
                ImmutableSet.of());
        FunctionMetadata functionMetadata = new FunctionMetadata(
                signature,
                nullability,
                false,
                internalFunctionDescriptor.isDeterministic(),
                internalFunctionDescriptor.description(),
                SCALAR);

        ScalarFunctionImplementation scalarFunctionImplementation = hiveFunctionManager.getScalarFunctionImplementation(internalFunctionDescriptor);
        FunctionInvoker functionInvoker = functionRegistry.getScalarFunctionInvoker(
                resolvedFunction.getFunctionId(),
                resolvedFunction.getSignature(),
                scalarFunctionImplementation,
                internalFunctionDescriptor.getInvocationConvention());
        verifyMethodHandleSignature(resolvedFunction.getSignature(), functionInvoker, internalFunctionDescriptor.getInvocationConvention());
        functionRegistry.addFunctions(ImmutableList.of(new HiveSqlScalarFunction(functionMetadata, scalarFunctionImplementation)));
        return resolvedFunction;
    }

    public static String resolveCatalogName(Session session, QualifiedName name)
    {
        return resolveCatalogName(name).orElse(session.getCatalog().orElse(DEFAULT_CATALOG_NAME_TO_RESOLVE));
    }

    public static ExternalFunctionKey resolveFunctionKey(QualifiedName name)
    {
        String resolverName = resolveResolverName(name).orElse(DEFAULT_FUNCTION_RESOLVER_NAME);
        return ExternalFunctionKey.of(resolverName, name.getSuffix());
    }

    private static Optional<String> resolveCatalogName(QualifiedName name)
    {
        if (name.getParts().size() > 2) {
            return Optional.of(name.getParts().get(0));
        }
        return Optional.empty();
    }

    private static Optional<String> resolveResolverName(QualifiedName name)
    {
        if (name.getParts().size() > 1) {
            return name.getPrefix().map(QualifiedName::getSuffix);
        }
        return Optional.empty();
    }

    private static String resolveFunctionName(QualifiedName name)
    {
        return name.getSuffix();
    }
}
