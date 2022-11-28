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
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOrcRowSet
{
    @Test
    public void testOrcRowValueSet()
    {
        OrcRowSetPredicate rowSetPredicate = new OrcRowValueSetPredicate(new int[] {});
        assertFalse(rowSetPredicate.matches(0, 1));

        rowSetPredicate = new OrcRowValueSetPredicate(new int[] {10});
        assertTrue(rowSetPredicate.matches(0, 11));
        assertFalse(rowSetPredicate.matches(0, 10));
        assertFalse(rowSetPredicate.matches(11, 2));

        rowSetPredicate = new OrcRowValueSetPredicate(new int[] {10, 16});
        assertTrue(rowSetPredicate.matches(0, 11));
        assertTrue(rowSetPredicate.matches(10, 1));
        assertTrue(rowSetPredicate.matches(10, 7));
        assertFalse(rowSetPredicate.matches(0, 10));
        assertFalse(rowSetPredicate.matches(11, 2));
        assertFalse(rowSetPredicate.matches(17, 2));
    }

    @Test
    public void testOrcRowBitMapSet()
    {
        OrcRowSetPredicate rowSetPredicate = new OrcRowBitmapSetPredicate(new RoaringBitmap());
        assertFalse(rowSetPredicate.matches(0, 1));

        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(10);
        rowSetPredicate = new OrcRowBitmapSetPredicate(bitmap);
        assertTrue(rowSetPredicate.matches(0, 11));
        assertFalse(rowSetPredicate.matches(0, 10));
        assertFalse(rowSetPredicate.matches(11, 2));

        bitmap = new RoaringBitmap();
        bitmap.add(10);
        bitmap.add(16);
        rowSetPredicate = new OrcRowBitmapSetPredicate(bitmap);
        assertTrue(rowSetPredicate.matches(0, 11));
        assertTrue(rowSetPredicate.matches(10, 1));
        assertTrue(rowSetPredicate.matches(10, 7));
        assertFalse(rowSetPredicate.matches(0, 10));
        assertFalse(rowSetPredicate.matches(11, 2));
        assertFalse(rowSetPredicate.matches(17, 2));
    }
}
