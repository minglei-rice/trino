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
import io.trino.Session;
import io.trino.operator.scalar.ChoicesScalarFunctionImplementation;
import io.trino.operator.scalar.ScalarFunctionImplementation;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.ExternalFunctionResolver;
import io.trino.spi.function.FunctionDescriptor;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.TypeSignature;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;

public class DefaultHiveFunctionManager
        implements HiveFunctionManager
{
    private final String identifier;
    private final Map<String, ExternalFunctionResolver> resolvers;

    public DefaultHiveFunctionManager(String identifier, List<ExternalFunctionResolver> resolvers)
    {
        this.identifier = identifier;
        ImmutableMap.Builder<String, ExternalFunctionResolver> builder = ImmutableMap.builder();
        resolvers.forEach(resolver -> builder.put(resolver.getName(), resolver));
        this.resolvers = builder.build();
    }

    @Override
    public String getIdentifier()
    {
        return identifier;
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionDescriptor descriptor)
    {
        return getImplementation(descriptor);
    }

    @Override
    public FunctionDescriptor getScalarFunctionDescriptor(Session session, String catalogName, Optional<String> resolverName, String functionName, List<TypeSignature> parameters)
    {
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        if (resolverName.isPresent()) {
            if (resolvers.containsKey(resolverName.get())) {
                return resolvers.get(resolverName.get()).getFunctionDescriptor(connectorSession, functionName, parameters);
            }
            throw new TrinoException(FUNCTION_NOT_FOUND, String.format("Expected resolving function %s from external functions resolver %s, but no found!", functionName, resolverName.get()));
        }

        // return the first matched descriptor
        TrinoException lastException = null;
        for (ExternalFunctionResolver resolver : resolvers.values()) {
            try {
                return resolver.getFunctionDescriptor(connectorSession, functionName, parameters);
            }
            catch (TrinoException e) {
                lastException = e;
                // resolving again when the exception type is FUNCTION_NOT_FOUND
                if (!FUNCTION_NOT_FOUND.toErrorCode().equals(e.getErrorCode())) {
                    throw e;
                }
            }
        }
        String resolverNames = String.join(",", resolvers.keySet());
        throw new TrinoException(
                FUNCTION_NOT_FOUND,
                String.format("Could not resolve function %s from the external function resolvers [%s]!", functionName, resolverNames),
                lastException);
    }

    private ScalarFunctionImplementation getImplementation(FunctionDescriptor descriptor)
    {
        InvocationConvention convention = descriptor.getInvocationConvention();
        return new ChoicesScalarFunctionImplementation(
                new BoundSignature(descriptor.getFunctionKey(), descriptor.getReturnType(), descriptor.getArgumentTypes()),
                convention.getReturnConvention(),
                convention.getArgumentConventions(),
                descriptor.getMethodHandle());
    }
}
