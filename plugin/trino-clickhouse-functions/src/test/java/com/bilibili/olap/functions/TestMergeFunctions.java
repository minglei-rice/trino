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

import com.bilibili.olap.functions.aggregation.BitmapStateSerializer;
import com.bilibili.olap.functions.aggregation.BitmapWithSmallSet;
import com.bilibili.olap.functions.aggregation.ClickHouseDataTypeMapping;
import com.bilibili.olap.functions.type.BitmapType;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.AbstractTestAggregationFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import io.trino.testing.LocalQueryRunner;

import java.util.Collections;
import java.util.List;

import static io.trino.SessionTestUtils.TEST_SESSION;

public class TestMergeFunctions
        extends AbstractTestAggregationFunction
{
    private static final BitmapStateSerializer SERIALIZER = new BitmapStateSerializer();
    private static final LocalQueryRunner RUNNER = LocalQueryRunner.builder(TEST_SESSION)
            .build();

    static {
        RUNNER.installPlugin(new ClickHouseFunctionsPlugin());
    }

    @Override
    protected TestingFunctionResolution createFunctionResolution()
    {
        return new TestingFunctionResolution(RUNNER.getTransactionManager(), RUNNER.getMetadata());
    }

    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, length, 32);
        for (int i = start; i < start + length; i++) {
            BitmapWithSmallSet bitmap = new BitmapWithSmallSet(ClickHouseDataTypeMapping.INT64);
            bitmap.add((long) i);
            SERIALIZER.serialize(bitmap, blockBuilder, true);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected String getFunctionName()
    {
        return "merge";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return Collections.singletonList(BitmapType.BITMAP_TYPE);
    }

    @Override
    protected Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        BitmapWithSmallSet bitmap = new BitmapWithSmallSet(ClickHouseDataTypeMapping.INT64);
        for (long i = start; i < start + length; i++) {
            bitmap.add(i);
        }

        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 0, 0);
        SERIALIZER.serialize(bitmap, blockBuilder, true);
        Block block = blockBuilder.build();
        return new SqlVarbinary(block.getSlice(0, 0, block.getSliceLength(0)).getBytes());
    }
}
