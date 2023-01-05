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

import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.util.Collection;
import java.util.List;

/**
 * Represents a class that can be used to invoke a scalar function. A scalar function is a function that
 * takes a set of inputs and returns a single output.
 */
public interface ScalarFunctionInvoker
{
    /**
     * Returns the type of the output returned by the function.
     */
    Type getResultType();

    /**
     * Returns a collection of the types of the inputs accepted by the function.
     */
    Collection<Type> getArgumentTypes();

    /**
     * Returns a list of the type signatures of the inputs accepted by the function.
     */
    List<TypeSignature> getArguments();

    /**
     * Takes a variable number of input arguments and returns the output of the function.
     * The input arguments must match the number and types specified by the getArgumentTypes
     * and getArguments methods.
     */
    Object evaluate(Object... inputs);
}
