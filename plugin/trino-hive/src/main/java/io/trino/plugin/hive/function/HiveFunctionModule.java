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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.hive.function.HiveFunctionRegistry;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class HiveFunctionModule
        extends AbstractConfigurationAwareModule
{
    private final ClassLoader classLoader;
    private final Map<String, String> config;

    public HiveFunctionModule(ClassLoader classLoader, Map<String, String> config)
    {
        this.classLoader = classLoader;
        this.config = config;
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(HiveFunctionRegistry.class).to(HiveBuiltInFunctionRegistry.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ExternalFunctionManagerConfig.class);
        binder.bind(HiveFunctionCreator.class).in(Scopes.SINGLETON);
        binder.bind(HiveFunctionResolver.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForHiveFunction
    public ClassLoader getClassLoader()
    {
        return classLoader;
    }

    @Provides
    @Singleton
    @ForHiveFunction
    public Map<String, String> getConf()
    {
        return config;
    }
}
