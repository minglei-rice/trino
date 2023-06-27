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

import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ExternalFunctionKey
        implements Serializable
{
    private static final String DELIMITER = "$$";

    // TODO Used to display the specified parser being used.
    private final String resolver;
    private final String funcName;

    private ExternalFunctionKey(String resolver, String funcName)
    {
        this.resolver = resolver;
        this.funcName = funcName;
    }

    public static ExternalFunctionKey of(String resolver, String funcName)
    {
        requireNonNull(resolver, "function resolver name is null");
        requireNonNull(funcName, "function name is null");
        return new ExternalFunctionKey(resolver, funcName);
    }

    public static ExternalFunctionKey of(String funcName)
    {
        requireNonNull(funcName, "function name is null");
        String[] parts = funcName.split("\\$\\$");
        if (parts.length != 2) {
            throw new IllegalArgumentException(String.format("funcName %s is not legal", funcName));
        }
        return ExternalFunctionKey.of(parts[0], parts[1]);
    }

    public String getResolver()
    {
        return resolver;
    }

    public String getFuncName()
    {
        return funcName;
    }

    @Override
    public String toString()
    {
        return resolver + DELIMITER + funcName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExternalFunctionKey that = (ExternalFunctionKey) o;
        return Objects.equals(resolver, that.resolver) && Objects.equals(funcName, that.funcName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(resolver, funcName);
    }
}
