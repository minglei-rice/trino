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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFMethodResolver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Overwrite the default behaviours of {@link GenericUDFBridge} to avoid the incompatible code mostly
 * related to {@link org.apache.hadoop.hive.ql.session.SessionState}.
 */
public class CustomizedGenericUDFBridge extends GenericUDFBridge {
    private static final long serialVersionUID = 4994861742809501113L;

    /**
     * The name of the UDF.
     */
    private String udfName;

    /**
     * Whether the UDF is an operator or not. This controls how the display string
     * is generated.
     */
    private boolean isOperator;

    /**
     * The underlying UDF class Name.
     */
    private String udfClassName;

    /**
     * The underlying method of the UDF class.
     */
    private transient Method udfMethod;

    /**
     * Helper to convert the parameters before passing to udfMethod.
     */
    private transient GenericUDFUtils.ConversionHelper conversionHelper;
    /**
     * The actual udf object.
     */
    private transient UDF udf;
    /**
     * The non-deferred real arguments for method invocation.
     */
    private transient Object[] realArguments;

    private transient UDFMethodResolver resolver;

    public CustomizedGenericUDFBridge(String udfName, boolean isOperator, String udfClassName) {
        this.udfName = udfName;
        this.isOperator = isOperator;
        this.udfClassName = udfClassName;
    }

    public void setUdfName(String udfName) {
        this.udfName = udfName;
    }

    @Override
    public String getUdfName() {
        return udfName;
    }

    public String getUdfClassName() {
        return udfClassName;
    }

    public void setUdfClassName(String udfClassName) {
        this.udfClassName = udfClassName;
    }

    public boolean isOperator() {
        return isOperator;
    }

    public void setOperator(boolean isOperator) {
        this.isOperator = isOperator;
    }

    public void setUdfMethodResolver(UDFMethodResolver resolver) {
        this.resolver = resolver;
    }

    /** Gets the UDF class and checks it against the whitelist, if any. */
    private Class<? extends UDF> getUdfClassInternal()
            throws ClassNotFoundException {
        /**
         * Could not use Hive SessionState classloader.
         */
        @SuppressWarnings("unchecked")
        Class<? extends UDF> clazz = (Class<? extends UDF>) Class.forName(
                udfClassName, true, Thread.currentThread().getContextClassLoader());
        return clazz;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        Class<? extends UDF> cls;
        try {
            cls = getUdfClassInternal();
            udf = cls.getConstructor().newInstance();
        } catch (Exception e) {
            throw new UDFArgumentException(
                    "Unable to instantiate UDF implementation class " + udfClassName + ": " + e);
        }

        // Resolve for the method based on argument types
        ArrayList<TypeInfo> argumentTypeInfos = new ArrayList<TypeInfo>(
                arguments.length);
        for (ObjectInspector argument : arguments) {
            argumentTypeInfos.add(TypeInfoUtils
                    .getTypeInfoFromObjectInspector(argument));
        }
        if (resolver != null) {
            udf.setResolver(resolver);
        } else {
            udf.setResolver(new CustomizedUDFMethodResolver(cls));
        }
        udfMethod = udf.getResolver().getEvalMethod(argumentTypeInfos);
        udfMethod.setAccessible(true);

        // Create parameter converters
        conversionHelper = new GenericUDFUtils.ConversionHelper(udfMethod, arguments);

        // Create the non-deferred realArgument
        realArguments = new Object[arguments.length];

        // Get the return ObjectInspector.
        return ObjectInspectorFactory
                .getReflectionObjectInspector(udfMethod.getGenericReturnType(),
                        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == realArguments.length);

        // Calculate all the arguments
        for (int i = 0; i < realArguments.length; i++) {
            realArguments[i] = arguments[i].get();
        }

        // Call the function
        Object result = StaticFunctionRegistry.invoke(udfMethod, udf, conversionHelper
                .convertIfNecessary(realArguments));

        // For non-generic UDF, type info isn't available. This poses a problem for Hive Decimal.
        // If the returned value is HiveDecimal, we assume maximum precision/scale.
        if (result != null && result instanceof HiveDecimalWritable) {
            result = HiveDecimalWritable.enforcePrecisionScale
                    ((HiveDecimalWritable) result,
                            HiveDecimal.SYSTEM_DEFAULT_PRECISION,
                            HiveDecimal.SYSTEM_DEFAULT_SCALE);
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        if (isOperator) {
            if (children.length == 1) {
                // Prefix operator
                return "(" + udfName + " " + children[0] + ")";
            } else {
                // Infix operator
                assert children.length == 2;
                return "(" + children[0] + " " + udfName + " " + children[1] + ")";
            }
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(udfName);
            sb.append("(");
            for (int i = 0; i < children.length; i++) {
                sb.append(children[i]);
                if (i + 1 < children.length) {
                    sb.append(", ");
                }
            }
            sb.append(")");
            return sb.toString();
        }
    }

    @Override
    public String[] getRequiredJars() {
        return udf.getRequiredJars();
    }

    @Override
    public String[] getRequiredFiles() {
        return udf.getRequiredFiles();
    }

    public static class CustomizedUDFMethodResolver implements UDFMethodResolver
    {
        /**
         * The class of the UDF.
         */
        private final Class<? extends UDF> udfClass;

        /**
         * Constructor. This constructor sets the resolver to be used for comparison
         * operators. See {@link UDFMethodResolver}
         */
        public CustomizedUDFMethodResolver(Class<? extends UDF> udfClass) {
            this.udfClass = udfClass;
        }

        @Override
        public Method getEvalMethod(List<TypeInfo> argClasses) throws UDFArgumentException {
            return StaticFunctionRegistry.getMethodInternal(udfClass, "evaluate", false, argClasses);
        }
    }
}
