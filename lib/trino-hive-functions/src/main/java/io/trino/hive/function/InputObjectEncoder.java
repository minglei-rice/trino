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

/**
 * Interface for encoding objects for input to a Hive function.
 * The encoded object will be converted to the type expected by the Hive function.
 */
public interface InputObjectEncoder
{
    /**
     * Encodes the given object for input to a Hive function.
     * The object should be of a type that is compatible with the Trino function,
     * and the returned object will be of a type that is compatible with the Hive function.
     *
     * @param object the object belongs to trino
     * @return the encoded object belongs to hive
     */
    Object encode(Object object);
}
