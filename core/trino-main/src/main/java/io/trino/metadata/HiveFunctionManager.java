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

import io.trino.Session;
import io.trino.operator.scalar.ScalarFunctionImplementation;
import io.trino.spi.function.FunctionDescriptor;
import io.trino.spi.type.TypeSignature;

import java.util.List;
import java.util.Optional;

public interface HiveFunctionManager
{
    String getIdentifier();

    ScalarFunctionImplementation getScalarFunctionImplementation(FunctionDescriptor descriptor);

    FunctionDescriptor getScalarFunctionDescriptor(Session session, String catalogName, Optional<String> resolverName, String functionName, List<TypeSignature> parameters);
}
