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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;

import static java.util.Objects.requireNonNull;

public class HiveFunctionMetadata
{
    private final String name;
    private final boolean deterministic;
    private final boolean runtimeConstant;
    private final String description;
    private final String example;

    public HiveFunctionMetadata(String name, boolean deterministic, boolean runtimeConstant, String description, String example)
    {
        requireNonNull(name, "function name should not be null");

        this.name = name;
        this.deterministic = deterministic;
        this.runtimeConstant = runtimeConstant;
        this.description = description;
        this.example = example;
    }

    public String getName()
    {
        return name;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public boolean isRuntimeConstant()
    {
        return runtimeConstant;
    }

    public String getDescription()
    {
        return description;
    }

    public String getExample()
    {
        return example;
    }

    public static HiveFunctionMetadata parseFunction(Class<?> definedClass)
    {
        requireNonNull(definedClass, "parsed function class should not be null");
        FunctionMetadataBuilder builder = FunctionMetadataBuilder.newBuilder(definedClass);
        builder.fillMetadata(definedClass, builder);
        return builder.build();
    }

    public static class FunctionMetadataBuilder
    {
        private final Class<?> funcClass;
        private String name;
        private Boolean deterministic;
        private Boolean runtimeConstant;
        private String description;
        private String example;

        private FunctionMetadataBuilder(Class<?> funcClass)
        {
            this.funcClass = funcClass;
        }

        private void fillMetadata(Class<?> definedClass, FunctionMetadataBuilder builder)
        {
            if (definedClass == null) {
                return;
            }
            Description description = definedClass.getAnnotation(Description.class);
            if (description != null) {
                builder.name(description.name());
                builder.description(description.value());
                builder.example(description.extended());
            }

            UDFType type = definedClass.getAnnotation(UDFType.class);
            if (type != null) {
                builder.deterministic(type.deterministic());
                builder.runtimeConstant(type.runtimeConstant());
            }

            fillMetadata(definedClass.getSuperclass(), builder);
        }

        public static FunctionMetadataBuilder newBuilder(Class<?> funcClass)
        {
            return new FunctionMetadataBuilder(funcClass);
        }

        public FunctionMetadataBuilder name(String name)
        {
            if (this.name == null) {
                this.name = name;
            }
            return this;
        }

        public FunctionMetadataBuilder deterministic(boolean deterministic)
        {
            if (this.deterministic == null) {
                this.deterministic = deterministic;
            }
            return this;
        }

        public FunctionMetadataBuilder runtimeConstant(boolean runtimeConstant)
        {
            if (this.runtimeConstant == null) {
                this.runtimeConstant = runtimeConstant;
            }
            return this;
        }

        public FunctionMetadataBuilder description(String description)
        {
            if (this.description == null) {
                this.description = description;
            }
            return this;
        }

        public FunctionMetadataBuilder example(String example)
        {
            if (this.example == null) {
                this.example = example;
            }
            return this;
        }

        public HiveFunctionMetadata build()
        {
            return new HiveFunctionMetadata(
                    name == null ? funcClass.getSimpleName() : name,
                    deterministic != null && deterministic,
                    runtimeConstant != null && runtimeConstant,
                    description == null ? "" : description,
                    example == null ? "" : example);
        }
    }
}

