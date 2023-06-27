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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hive.function.HiveFunctionMetadata;
import io.trino.hive.function.HiveFunctionRegistry;
import io.trino.hive.function.HiveScalarFunctionInvoker;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.ExternalFunctionKey;
import io.trino.spi.function.FunctionDescriptor;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.hive.function.HiveFunctionErrorCode.HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE;

public class HiveFunctionCreator
{
    private final Logger log = Logger.get(HiveFunctionCreator.class);
    private final HiveFunctionRegistry hiveFunctionRegistry;
    private final HDFSClassLoader classLoader;

    // TODO: Support writing hive complex types in sql, such as map/array/struct?
    private static final Set<String> FUNCTIONS_TO_EXCLUDE = ImmutableSet.of(
            "map",
            "array",
            "struct",
            "current_database",
            "current_date",
            "current_groups",
            "current_timestamp",
            "current_user",
            "logged_in_user",
            "unix_timestamp",
            "create_union",
            "extract_union");

    @Inject
    public HiveFunctionCreator(
            HiveFunctionRegistry externalFunctionRegistry,
            @ForHiveFunction ClassLoader parentClassLoader)
    {
        this.hiveFunctionRegistry = externalFunctionRegistry;
        this.classLoader = new HDFSClassLoader(new URL[]{}, parentClassLoader);
    }

    public FunctionDescriptor create(ConnectorSession session, ExternalFunctionKey functionKey, List<TypeSignature> argumentTypes, TypeManager typeManager)
    {
        try {
            return createBuiltInFunctionDescriptor(functionKey, argumentTypes, typeManager);
        }
        catch (Throwable e) {
              // TODO from hms to get function.
            throw new RuntimeException();
        }
    }

    private HiveScalarFunctionDescriptor createBuiltInFunctionDescriptor(ExternalFunctionKey functionKey, List<TypeSignature> argumentTypes, TypeManager typeManager)
            throws Exception
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Class<?> functionClass = hiveFunctionRegistry.getClass(functionKey.getFuncName());
            return makeScalarFunctionDescriptor(functionClass, functionKey, argumentTypes, typeManager);
        }
    }

    private HiveScalarFunctionDescriptor makeScalarFunctionDescriptor(
            Class<?> cls,
            ExternalFunctionKey functionKey,
            List<TypeSignature> arguments,
            TypeManager typeManager)
    {
        if (anyAssignableFrom(cls, GenericUDF.class, UDF.class)) {
            HiveFunctionMetadata functionMetadata = HiveFunctionMetadata.builder(cls).build();
            // if a function was runtime constant or excluded, it implies the function will utilize Hive SessionState
            // to evaluate the arguments in Hive 3+, which is not compatible with Trino.
            // TODO: Support more hive3+ UDFs.
            if (functionMetadata.isRuntimeConstant() || FUNCTIONS_TO_EXCLUDE.contains(functionKey.getFuncName())) {
                throw new TrinoException(HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE, String.format("Unsupported function type %s", cls.getName()));
            }

            HiveScalarFunctionInvoker invoker = HiveScalarFunctionInvoker.create(cls, functionKey.getFuncName(), arguments, typeManager);
            return HiveScalarFunctionDescriptor.makeFunctionDescriptor(
                    classLoader,
                    functionKey,
                    functionMetadata.isDeterministic(),
                    functionMetadata.getDescription(),
                    functionMetadata.getExample(),
                    invoker,
                    typeManager);
        }
        throw new TrinoException(HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE, String.format("Unsupported function type " + cls.getName() + " / " + cls.getSuperclass().getName()));
    }

    private static boolean anyAssignableFrom(Class<?> cls, Class<?>... supers)
    {
        return Stream.of(supers).anyMatch(s -> s.isAssignableFrom(cls));
    }

    static class HDFSClassLoader
            extends URLClassLoader
    {
        private static final Set<String> PROTOCOLS = ImmutableSet.of("hdfs", "viewfs", "file", "jar", "ftp", "http", "https");

        private final Logger log = Logger.get(HDFSClassLoader.class);

        private final Object downloadLocker = new Object();

        public HDFSClassLoader(URL[] urls, ClassLoader parent)
        {
            this(HDFSClassLoader.class.getSimpleName(), urls, parent);
        }

        public HDFSClassLoader(String workDir, HdfsEnvironment environment, URL[] urls)
        {
            super(urls);
        }

        public HDFSClassLoader(String name, URL[] urls, ClassLoader parent)
        {
            super(name, urls, parent);
        }
    }
}
