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
package io.trino.orc;

import org.roaringbitmap.RoaringBitmap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OrcRowBitmapSetPredicate
        implements OrcRowSetPredicate
{
    private final RoaringBitmap bitmap;

    public OrcRowBitmapSetPredicate(RoaringBitmap bitmap)
    {
        requireNonNull(bitmap, "bitmap can not be null.");
        this.bitmap = bitmap;
    }

    @Override
    public boolean matches(int start, int length)
    {
        checkArgument(start >= 0, "start offset can not be negative.");
        checkArgument(length > 0, "length must be positive.");
        if (length == 1) {
            return bitmap.contains(start);
        }
        else {
            return bitmap.rangeCardinality(start, start + length) > 0;
        }
    }
}
