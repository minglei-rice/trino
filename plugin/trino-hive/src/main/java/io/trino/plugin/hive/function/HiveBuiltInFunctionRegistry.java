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

import com.google.inject.Inject;
import io.trino.hive.function.HiveFunctionRegistry;
import io.trino.hive.function.StaticFunctionRegistry;
import io.trino.spi.classloader.ThreadContextClassLoader;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HiveBuiltInFunctionRegistry
        implements HiveFunctionRegistry
{
    private final ClassLoader classLoader;

    @Inject
    public HiveBuiltInFunctionRegistry(@ForHiveFunction ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    @Override
    public Class<?> getClass(String name)
            throws ClassNotFoundException
    {
        return getClass(name, classLoader);
    }

    @Override
    public Class<?> getClass(String name, ClassLoader classLoader)
            throws ClassNotFoundException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return StaticFunctionRegistry.getFunctionInfo(name).getFunctionClass();
        }
        catch (SemanticException | NullPointerException e) {
            throw new ClassNotFoundException("Class of built in function [" + name + "] not found", e);
        }
    }
}
