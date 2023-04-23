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
package com.bilibili.olap.functions;

import com.bilibili.olap.functions.aggregation.BitmapIntAgg;
import com.bilibili.olap.functions.aggregation.BitmapLongAgg;
import com.bilibili.olap.functions.aggregation.BitmapShortAgg;
import com.bilibili.olap.functions.aggregation.MergeBitmap;
import com.bilibili.olap.functions.scalar.BitmapFunctions;
import com.bilibili.olap.functions.scalar.BitmapOperators;
import com.bilibili.olap.functions.type.BitmapType;
import com.bilibili.olap.functions.type.ClickHouseBitmapType;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.Plugin;
import io.trino.spi.type.Type;

import java.util.Set;

public class ClickHouseFunctionsPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(BitmapOperators.class)
                .add(BitmapFunctions.class)
                .add(BitmapLongAgg.class)
                .add(BitmapIntAgg.class)
                .add(BitmapShortAgg.class)
                .add(MergeBitmap.class)
                .build();
    }

    @Override
    public Iterable<Type> getTypes()
    {
        return ImmutableSet.<Type>builder()
                .add(BitmapType.BITMAP_TYPE)
                .add(ClickHouseBitmapType.CLICK_HOUSE_BITMAP_TYPE)
                .build();
    }
}
