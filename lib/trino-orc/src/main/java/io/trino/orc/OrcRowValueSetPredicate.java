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

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OrcRowValueSetPredicate
        implements OrcRowSetPredicate
{
    private final int[] rowNumbers;

    public OrcRowValueSetPredicate(int[] rowNumbers)
    {
        requireNonNull(rowNumbers, "rowNumbers can not be null.");
        this.rowNumbers = rowNumbers;
    }

    @Override
    public boolean matches(int start, int length)
    {
        checkArgument(start >= 0, "start offset can not be negative.");
        checkArgument(length > 0, "length must be positive.");

        int arrayIndex = Arrays.binarySearch(rowNumbers, start);
        if (arrayIndex >= 0) {
            return true;
        }
        else { // Negative arrayIndex means that 'start' is not found in rowNumbers, and return value is (-(insertion point) - 1)
            int insertPoint = -(arrayIndex + 1);
            if (insertPoint >= rowNumbers.length) { // start is larger than max value in rowNumbers.
                return false;
            }
            else {
                // rowNumber values does not exists in [start, start + length).
                return (start + length - 1) >= rowNumbers[insertPoint];
            }
        }
    }
}
