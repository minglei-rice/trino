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
package io.trino.external.function.hive;

import io.trino.external.function.InputObjectEncoder;
import io.trino.external.function.OutputObjectDecoder;
import io.trino.external.function.ScalarFunctionInvoker;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.StandardErrorCode;
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

import static io.trino.external.function.hive.HiveFunctionErrorCode.executionError;
import static io.trino.external.function.hive.HiveFunctionErrorCode.initializationError;
import static io.trino.external.function.hive.HiveFunctionErrorCode.unsupportedFunctionType;
import static java.util.Objects.requireNonNull;

/**
 * Generate a compatible class for Trino using Hive function information and provide a method for invoking the function.
 */
public class HiveScalarFunctionInvoker
        implements ScalarFunctionInvoker
{
    /**
     * A list of the argument types of the function, represented as TypeSignature objects.
     */
    private final List<TypeSignature> arguments;

    /**
     * This is a supplier object that provides a GenericUDF (a Hive function class).
     */
    private final GenericUDF udfSupplier;

    /**
     * This is an array of InputObjectEncoder objects, which are used to encode the input arguments of the function.
     */
    private final InputObjectEncoder[] argumentEncoders;

    /**
     * This is an OutputObjectDecoder object, which is used to decode the output of the function.
     */
    private final OutputObjectDecoder outputObjectDecoder;

    /**
     * This is a list of the argument types of the function, represented as Type objects.
     */
    private final List<Type> argumentTypes;

    /**
     * This is the return type of the function, represented as a Type object.
     */
    private final Type resultType;

    public HiveScalarFunctionInvoker(List<TypeSignature> arguments,
                                     GenericUDF udfSupplier,
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

    public static HiveScalarFunctionInvoker create(Class<?> cls, String name, List<TypeSignature> arguments, TypeManager typeManager)
    {
        final List<Type> argumentTypes = arguments.stream()
                .map(typeManager::getType)
                .collect(Collectors.toList());

        try {
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
                    throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                            "Error occurred while creating and initializing GenericUDF instance.", e);
                }
            });

            // Step 3: invoke initial
            genericUDFSupplier.get();

            // Step 4: Create invoker
            // TODO: How can we determine the length of the return type, such as for a varchar(x), where x
            //  is inferred as the maximum value of Integer by default using the hive inspector?
            Type resultType = TrinoTypeTransformer.fromObjectInspector(resultInspector.get(), typeManager);
            InputObjectEncoder[] argumentEncoders = argumentTypes.stream()
                    .map(argumentsType -> TrinoObjectEncoders.create(argumentsType, typeManager))
                    .toArray(InputObjectEncoder[]::new);
            OutputObjectDecoder outputDecoder = HiveObjectDecoders.create(resultType, resultInspector.get());

            return new HiveScalarFunctionInvoker(
                    arguments,
                    genericUDFSupplier.get(),
                    argumentEncoders,
                    outputDecoder,
                    argumentTypes,
                    resultType);
        }
        catch (Throwable e) {
            throw initializationError(e, String.format("Failed to initialize function %s with class %s.", name, cls));
        }
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
        throw unsupportedFunctionType(cls);
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

    /**
     * Note that the argumentEncoders and outputObjectDecoder are used to bridge this gap between the Trino and Hive
     * representations of the function's inputs and outputs, allowing the HiveScalarFunctionInvoker to invoke
     * the Hive UDF with the correct arguments and interpret the result of the UDF correctly.
     *
     * Without these encoders and decoders, it would not be possible to use the HiveScalarFunctionInvoker
     * to invoke a Hive UDF from Trino.
     *
     * the input arguments are first passed through the argumentEncoders to convert them to a format that
     * can be passed to the Hive UDF. The resulting array of encoded arguments is then passed to the Hive UDF,
     * which returns its output. This output is then passed to the outputObjectDecoder to convert it to a
     * format that is compatible with Trino.
     */
    @Override
    public Object evaluate(Object... inputs)
    {
        try {
            DeferredObject[] objects = new DeferredObject[inputs.length];
            for (int i = 0; i < inputs.length; i++) {
                objects[i] = (inputs[i] == null ? new DeferredJavaObject(null) : new DeferredJavaObject(argumentEncoders[i].encode(inputs[i])));
            }
            Object evaluated = udfSupplier.evaluate(objects);
            return outputObjectDecoder.decode(evaluated);
        }
        catch (HiveException e) {
            throw executionError(e);
        }
    }
}
