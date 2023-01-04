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

import com.google.common.base.Joiner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.ExternalFunctionKey;
import io.trino.spi.function.ExternalFunctionResolver;
import io.trino.spi.function.FunctionDescriptor;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Throwables.throwIfInstanceOf;

public class HiveFunctionResolver
        implements ExternalFunctionResolver
{
    private final HiveFunctionCreator creator;
    private final Cache<String, FunctionDescriptor> cache;

    private TypeManager typeManager;

    @Inject
    public HiveFunctionResolver(HiveFunctionCreator creator)
    {
        this.creator = creator;
        cache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(1000)
                .softValues()
                .build();
    }

    @Override
    public String getName()
    {
        return "hive";
    }

    @Override
    public Collection<FunctionDescriptor> getFunctionDescriptors(Identity identity, String functionName, List<TypeSignature> parameters)
    {
        return null;
    }

    @Override
    public FunctionDescriptor getFunctionDescriptor(ConnectorSession session, String functionName, List<TypeSignature> parameters)
    {
        ExternalFunctionKey externalFunctionKey = ExternalFunctionKey.of(getName(), functionName);
        String cacheKey = externalFunctionKey + "(" + Joiner.on(",").join(parameters) + ")";
        try {
            return cache.get(cacheKey, () -> creator.create(session, externalFunctionKey, parameters, typeManager));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }
}
