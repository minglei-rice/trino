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
package io.trino.spi.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * String related Predicate which may not be able to transform into TupleDomain.
 */
public class StringPredicate
{
    public static enum Operator
    {
        EQUALS, LIKE, STARTS_WITH, HAS_TOKEN
    }

    private final String field;
    private final String variable;
    private final Operator operator;

    @JsonCreator
    public StringPredicate(
            @JsonProperty("field") String field,
            @JsonProperty("variable") String variable,
            @JsonProperty("operator") Operator operator)
    {
        this.field = field;
        this.variable = variable;
        this.operator = operator;
    }

    @JsonProperty
    public String getField()
    {
        return field;
    }

    @JsonProperty
    public String getVariable()
    {
        return variable;
    }

    @JsonProperty
    public Operator getOperator()
    {
        return operator;
    }

    public static StringPredicate of(String field, String variable, Operator operator)
    {
        return new StringPredicate(field, variable, operator);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof StringPredicate)) {
            return false;
        }
        StringPredicate other = (StringPredicate) o;
        return field.equals(other.field) && variable.equals(other.variable) && operator.equals(other.operator);
    }

    @Override
    public int hashCode()
    {
        return field.hashCode() * 17 + variable.hashCode() * 5 + operator.hashCode();
    }
}
