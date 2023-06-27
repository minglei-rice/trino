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
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDescriptor;
import io.trino.spi.function.FunctionMetadataResolver;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.TypeSignature;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;

public class FunctionResolverManager
{
    private final String catalogName;
    private final Map<String, FunctionMetadataResolver> resolvers;

    public FunctionResolverManager(String catalogName, FunctionMetadataResolver resolver)
    {
        this.catalogName = catalogName;
        ImmutableMap.Builder<String, FunctionMetadataResolver> builder = ImmutableMap.builder();
        builder.put(resolver.getName(), resolver);
        this.resolvers = builder.buildOrThrow();
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public SpecializedSqlScalarFunction getScalarFunctionImplementation(FunctionDescriptor descriptor)
    {
        return getImplementation(descriptor);
    }

    public SpecializedSqlScalarFunction getImplementation(FunctionDescriptor descriptor)
    {
        InvocationConvention convention = descriptor.getInvocationConvention();
        BoundSignature boundSignature = new BoundSignature(descriptor.getFunctionKey(), descriptor.getReturnType(), descriptor.getArgumentTypes());
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                convention.getReturnConvention(),
                convention.getArgumentConventions(),
                descriptor.getMethodHandle());
    }

    public FunctionDescriptor getScalarFunctionDescriptor(Session session, String catalogName, String functionName, List<TypeSignature> parameters)
    {
        ConnectorSession connectorSession = session.toConnectorSession();
        if (resolvers.containsKey(catalogName)) {
            return resolvers.get(catalogName).getFunctionDescriptor(connectorSession, functionName, parameters);
        }
        throw new TrinoException(FUNCTION_NOT_FOUND, String.format("Expected resolving function %s from external functions resolver %s, but no found!", functionName, catalogName));
    }
}
