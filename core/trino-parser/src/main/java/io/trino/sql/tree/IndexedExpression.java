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
package io.trino.sql.tree;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class IndexedExpression
        extends Expression
{
    public static final IndexedExpression TRUE_EXPRESSION = new IndexedExpression(TRUE_LITERAL, 0);

    private final Expression origin;
    private final int id;

    public IndexedExpression(Expression origin, int id)
    {
        super(origin.getLocation());
        this.origin = origin;
        this.id = id;
    }

    public Expression getOriginExpression()
    {
        return origin;
    }

    public int getId()
    {
        return id;
    }

    @Override
    public Optional<NodeLocation> getLocation()
    {
        return origin.getLocation();
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIndexedExpression(this, context);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (this == other) {
            return true;
        }
        if (other instanceof IndexedExpression) {
            IndexedExpression otherIndexedExpression = (IndexedExpression) other;
            return origin.shallowEquals(otherIndexedExpression.origin);
        }
        return false;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return origin.getChildren();
    }

    @Override
    public int hashCode()
    {
        return origin.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        IndexedExpression otherIndexedExpression = (IndexedExpression) obj;
        return id == otherIndexedExpression.id && origin.equals(otherIndexedExpression.origin);
    }
}
