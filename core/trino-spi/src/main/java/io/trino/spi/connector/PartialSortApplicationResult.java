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
package io.trino.spi.connector;

public class PartialSortApplicationResult<T>
{
    private final T handle;

    private final boolean asc;

    private final boolean canRewrite;

    private final String sortOrder;

    public PartialSortApplicationResult(T handle, boolean asc, boolean canRewrite, String sortOrder)
    {
        this.handle = handle;
        this.asc = asc;
        this.canRewrite = canRewrite;
        this.sortOrder = sortOrder;
    }

    public String getSortOrder()
    {
        return sortOrder;
    }

    public T getHandle()
    {
        return handle;
    }

    public boolean isAsc()
    {
        return asc;
    }

    public boolean isCanRewrite()
    {
        return canRewrite;
    }
}
