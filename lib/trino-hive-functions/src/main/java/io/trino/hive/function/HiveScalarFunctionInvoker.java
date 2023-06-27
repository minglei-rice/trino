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
package io.trino.hive.function;

import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.trino.hive.function.HiveFunctionErrorCode.HIVE_FUNCTION_EXECUTION_ERROR;
import static io.trino.hive.function.HiveFunctionErrorCode.HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE;
import static java.util.Objects.requireNonNull;

/**
 * Generate the compatible class with Trino from the Hive function info, and supply the method to invoke the function.
 */
public class HiveScalarFunctionInvoker
        implements ScalarFunctionInvoker
{
    private final List<TypeSignature> arguments;
    private final Supplier<GenericUDF> udfSupplier;
    private final InputObjectEncoder[] argumentEncoders;
    private final OutputObjectDecoder outputObjectDecoder;
    private final List<Type> argumentTypes;
    private final Type resultType;

    public static HiveScalarFunctionInvoker create(Class<?> cls, String name, List<TypeSignature> arguments, TypeManager typeManager)
    {
        final List<Type> argumentTypes = arguments.stream()
                .map(typeManager::getType)
                .collect(Collectors.toList());

        // Step 1: transform function arguments
        ObjectInspector[] inputInspectors = argumentTypes.stream()
                .map(argumentType -> ObjectInspectors.create(argumentType, typeManager))
                .toArray(ObjectInspector[]::new);

        // Step 2: initialize a ThreadLocal GenericUDF instance
        AtomicReference<ObjectInspector> resultInspector = new AtomicReference<>();
        final ThreadLocal<GenericUDF> genericUDFSupplier = ThreadLocal.withInitial(() -> {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(cls.getClassLoader())) {
                // Step 2.1: construct an instance
                GenericUDF ret = createGenericUDF(name, cls);
                // Step 2.2: initialize the instance
                resultInspector.set(ret.initialize(inputInspectors));
                return ret;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Step 3: get the threadlocal instance
        genericUDFSupplier.get();

        // Step 4: Create invoker
        // TODO: How to determine the return type length, such as varchar(x), in which case x
        //  is inferred as Integer.MAX_VALUE from hive inspector by default?
        Type resultType = TrinoTypeTransformer.fromObjectInspector(resultInspector.get(), typeManager);
        InputObjectEncoder[] argumentEncoders = argumentTypes.stream()
                .map(argumentsType -> TrinoObjectEncoders.create(argumentsType, typeManager))
                .toArray(InputObjectEncoder[]::new);
        OutputObjectDecoder outputDecoder = HiveObjectDecoders.create(resultType, resultInspector.get());

        return new HiveScalarFunctionInvoker(
                arguments,
                genericUDFSupplier::get,
                argumentEncoders,
                outputDecoder,
                argumentTypes,
                resultType);
    }

    private static GenericUDF createGenericUDF(String name, Class<?> cls)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
    {
        if (GenericUDF.class.isAssignableFrom(cls)) {
            Constructor<?> constructor = cls.getConstructor();
            return (GenericUDF) constructor.newInstance();
        }
        else if (UDF.class.isAssignableFrom(cls)) {
            return new CustomizedGenericUDFBridge(name, false, cls.getName());
        }
        throw new TrinoException(HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE, cls.getName());
    }

    public HiveScalarFunctionInvoker(List<TypeSignature> arguments,
                                     Supplier<GenericUDF> udfSupplier,
                                     InputObjectEncoder[] argumentEncoders,
                                     OutputObjectDecoder outputObjectDecoder,
                                     List<Type> argumentTypes,
                                     Type resultType)
    {
        this.arguments = requireNonNull(arguments, "Arguments should not be null");
        this.udfSupplier = udfSupplier;
        this.argumentEncoders = argumentEncoders;
        this.outputObjectDecoder = outputObjectDecoder;
        this.argumentTypes = requireNonNull(argumentTypes, "Argument types should not be null");
        this.resultType = requireNonNull(resultType, "Result type should not be null");
    }

    @Override
    public Type getResultType()
    {
        return resultType;
    }

    @Override
    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    @Override
    public List<TypeSignature> getArguments()
    {
        return arguments;
    }

    @Override
    public Object evaluate(Object... inputs)
    {
        try {
            DeferredObject[] objects = new DeferredObject[inputs.length];
            for (int i = 0; i < inputs.length; i++) {
                objects[i] = inputs[i] == null ? new DeferredJavaObject(null) :
                        new DeferredJavaObject(argumentEncoders[i].encode(inputs[i]));
            }
            Object evaluated = udfSupplier.get().evaluate(objects);
            return outputObjectDecoder.decode(evaluated);
        }
        catch (HiveException e) {
            throw new TrinoException(HIVE_FUNCTION_EXECUTION_ERROR, e);
        }
    }
}
