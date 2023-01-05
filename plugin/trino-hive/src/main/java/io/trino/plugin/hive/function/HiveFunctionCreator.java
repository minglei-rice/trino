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
import io.trino.hive.function.HiveFunctionRegistry;
import io.trino.hive.function.HiveFunctionMetadata;
import io.trino.hive.function.HiveScalarFunctionInvoker;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.metastore.thrift.MetastoreLocator;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.ExternalFunctionCreator;
import io.trino.spi.function.ExternalFunctionKey;
import io.trino.spi.function.FunctionDescriptor;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static io.trino.hive.function.HiveFunctionErrorCode.functionNotFound;
import static io.trino.hive.function.HiveFunctionErrorCode.initializationError;
import static io.trino.hive.function.HiveFunctionErrorCode.unsupportedFunction;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class HiveFunctionCreator
        implements ExternalFunctionCreator
{
    private final Logger log = Logger.get(HiveFunctionCreator.class);
    private final ExternalFunctionManagerConfig config;
    private final HiveFunctionRegistry hiveFunctionRegistry;
    private final HDFSClassLoader classLoader;
    private MetastoreLocator clientProvider;

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
            ExternalFunctionManagerConfig config,
            HiveFunctionRegistry hiveFunctionRegistry,
            HdfsEnvironment environment,
            @ForHiveFunction ClassLoader parentClassLoader)
    {
        this.config = config;
        this.hiveFunctionRegistry = hiveFunctionRegistry;
        this.classLoader = new HDFSClassLoader(config.getExternalFunctionsStorageDir(), environment, new URL[]{}, parentClassLoader);
    }

    public void setClientProvider(MetastoreLocator clientProvider)
    {
        this.clientProvider = clientProvider;
    }

    /**
     * TODO: Move getting class operations into {@link HiveFunctionRegistry}.
     */
    @Override
    public FunctionDescriptor create(ConnectorSession session, ExternalFunctionKey functionKey, List<TypeSignature> argumentTypes, TypeManager typeManager)
    {
        try {
            return createBuiltInFunctionDescriptor(functionKey, argumentTypes, typeManager);
        }
        catch (ClassNotFoundException e) {
            log.warn(String.format("Hive builtin UDF [%s] not found! Exception: %s", functionKey, e.getMessage()));
            try {
                return createHiveMetaStoreFunctionDescriptor(
                        Identity.forUser(session.getUser()).build(),
                        functionKey,
                        argumentTypes,
                        typeManager);
            }
            catch (RuntimeException te) {
                throw functionNotFound(functionKey.getFuncName(), "Could not resolve it from hive metastore.");
            }
        }
        catch (TrinoException te) {
            throw functionNotFound(functionKey.getFuncName(), "Could not resolve it from hive built-in functions.");
        }
    }

    private HiveScalarFunctionDescriptor createBuiltInFunctionDescriptor(ExternalFunctionKey functionKey, List<TypeSignature> argumentTypes, TypeManager typeManager)
            throws TrinoException, ClassNotFoundException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            try {
                Class<?> functionClass = hiveFunctionRegistry.getClass(functionKey.getFuncName());
                return makeScalarFunctionDescriptor(functionClass, functionKey, argumentTypes, typeManager);
            }
            catch (ClassNotFoundException e) {
                throw e;
            }
            catch (Throwable t) {
                throw initializationError(t, "Failed to create built in function descriptor.");
            }
        }
    }

    /**
     * The class will not be tried to be loaded from the local jars because the function may have been unregistered in Hive.
     */
    private HiveScalarFunctionDescriptor createHiveMetaStoreFunctionDescriptor(
            Identity identity,
            ExternalFunctionKey functionKey,
            List<TypeSignature> argumentTypes,
            TypeManager typeManager)
    {
        long startTime = System.currentTimeMillis();
        try (ThreadContextClassLoader ignore = new ThreadContextClassLoader(classLoader);
                ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient(Optional.empty())) {
            String functionName = functionKey.getFuncName();
            // TODO: Support parsing schema from the external function key.
            String schema = config.getHiveExternalFunctionSchemaDefault();
            Function function = metastoreClient.getFunction(schema, functionName);
            if (function != null) {
                Class<?> functionClass = getClass(function.getClassName(), function, identity);
                return makeScalarFunctionDescriptor(functionClass, functionKey, argumentTypes, typeManager);
            }
            throw functionNotFound(functionKey.getFuncName(), "Function not found in Hive metastore");
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Throwable e) {
            throw initializationError(e, "Error loading function from Hive metastore.");
        }
        finally {
            log.info("Creating function descriptor from Hive metastore took {} ms", System.currentTimeMillis() - startTime);
        }
    }

    private Class<?> getClass(String className, Function function, Identity identity)
    {
        try {
            return classLoader.loadClass(className);
        }
        catch (ClassNotFoundException e) {
            registerClassesFromJar(function, identity);
            try {
                return classLoader.loadClass(className);
            }
            catch (ClassNotFoundException ex) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not find class " + className + " from classloader!");
            }
        }
    }

    /**
     * This is a behavior of batch loading udf, which will load all udf belonging to this jar.
     */
    private void registerClassesFromJar(Function function, Identity identity)
    {
        function.getResourceUris()
                .stream()
                .filter(uri -> uri.getResourceType() == ResourceType.JAR) // only match jar
                .map(ResourceUri::getUri)
                .findFirst()
                .ifPresentOrElse(
                        jarUri -> classLoader.addHdfsJar(jarUri, identity), // if found jar, then download it.
                        () -> { throw new TrinoException(GENERIC_USER_ERROR, String.format("Could not determine the resource path bound to the function %s(%s)", function.getFunctionName(), function.getClassName()));
                    });
    }

    private HiveScalarFunctionDescriptor makeScalarFunctionDescriptor(
            Class<?> cls,
            ExternalFunctionKey functionKey,
            List<TypeSignature> arguments,
            TypeManager typeManager)
    {
        if (anyAssignableFrom(cls, GenericUDF.class, UDF.class)) {
            HiveFunctionMetadata functionMetadata = HiveFunctionMetadata.parseFunction(cls);
            // if a function was runtime constant or excluded, it implies the function will utilize Hive SessionState
            // to evaluate the arguments in Hive 3+, which is not compatible with Trino.
            // TODO: Support more hive3+ UDFs.
            if (functionMetadata.isRuntimeConstant() || FUNCTIONS_TO_EXCLUDE.contains(functionKey.getFuncName())) {
                throw unsupportedFunction(cls);
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
        else if (anyAssignableFrom(cls, AbstractGenericUDAFResolver.class, UDAF.class)) {
            throw unsupportedFunction(cls);
        }
        throw unsupportedFunction(cls);
    }

    private static boolean anyAssignableFrom(Class<?> cls, Class<?>... supers)
    {
        return Stream.of(supers).anyMatch(s -> s.isAssignableFrom(cls));
    }

    static class HDFSClassLoader
            extends URLClassLoader
    {
        private final Logger log = Logger.get(HDFSClassLoader.class);

        private static final Set<String> PROTOCOLS = ImmutableSet.of("hdfs", "viewfs", "file", "jar", "ftp", "http", "https");
        private final Lock downloadLock = new ReentrantLock();
        private final String workDir;
        private final HdfsEnvironment environment;

        public HDFSClassLoader(String workDir, HdfsEnvironment environment, URL[] urls, ClassLoader parent)
        {
            this(HDFSClassLoader.class.getSimpleName(), workDir, environment, urls, parent);
        }

        public HDFSClassLoader(String name, String workDir, HdfsEnvironment environment, URL[] urls, ClassLoader parent)
        {
            super(name, urls, parent);
            this.workDir = workDir;
            this.environment = environment;
        }

        /**
         * Download the target jar file from HDFS to work dir, and then to add the local path to the URL
         * classloader.
         */
        public void addHdfsJar(String path, Identity identity)
        {
            requireNonNull(path);
            try {
                org.apache.hadoop.fs.Path remotePath = new org.apache.hadoop.fs.Path(path);
                if (!PROTOCOLS.contains(remotePath.toUri().getScheme())) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported protocol for resource path: " + path);
                }

                Path localPath = Paths.get(workDir, HDFSClassLoader.class.getName(), remotePath.getName());
                long startTime = System.currentTimeMillis();
                downloadHdfsResource(remotePath, localPath, identity);
                log.info("Download resource {} cost {} ms.", remotePath, System.currentTimeMillis() - startTime);

                if (!Files.exists(localPath)) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Jar file " + remotePath + " has been downloaded, but could not find it at " + localPath);
                }

                addURL(localPath.toUri().toURL());
            }
            catch (IOException e) {
                log.error("Failed to download the jar file {}.", path, e);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }
        }

        private void downloadHdfsResource(org.apache.hadoop.fs.Path path, Path localPath, Identity identity) throws IOException
        {
            org.apache.hadoop.fs.Path localHdfsPath = new org.apache.hadoop.fs.Path("file://", localPath.toString());
            ConnectorIdentity connectorIdentity = ConnectorIdentity.forUser(identity.getUser()).build();
            HdfsEnvironment.HdfsContext hdfsContext = new HdfsEnvironment.HdfsContext(connectorIdentity);
            try (FileSystem remoteFileSystem = environment.getFileSystem(hdfsContext, path);
                    FileSystem localFs = environment.getFileSystem(hdfsContext, localHdfsPath)) {
                downloadLock.lock();
                if (!localFs.exists(localHdfsPath)) {
                    // if the hdfs file is not existed, FileNotFoundException should be thrown
                    FileUtil.copy(remoteFileSystem, path, localFs, localHdfsPath, false, environment.getConfiguration(hdfsContext, path));
                }
            }
            finally {
                downloadLock.unlock();
            }
        }
    }
}
