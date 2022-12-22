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
package io.trino.plugin.iceberg.functions.operator.aggregation.state;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import org.apache.iceberg.aggindex.PercentileAccumulator;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class AggIndexPercentileStateSerializer
        implements AccumulatorStateSerializer<AggIndexPercentileAccumulatorState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(AggIndexPercentileAccumulatorState state, BlockBuilder out)
    {
        if (state.getResult() == null) {
            out.appendNull();
        }
        else {
            byte[] serialized = state.getPercentileAccumulator().toBinary();

            SliceOutput output = Slices.allocate(SIZE_OF_DOUBLE + SIZE_OF_DOUBLE + SIZE_OF_INT + serialized.length).getOutput();
            output.appendDouble(state.getWeight());
            output.appendDouble(state.getPercentile());
            output.appendInt(serialized.length);
            output.appendBytes(serialized);

            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, AggIndexPercentileAccumulatorState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();

        state.setWeight(input.readDouble());
        // read percentile
        state.setPercentile(input.readDouble());

        // read digest
        int length = input.readInt();
        PercentileAccumulator percentileAggAccumulator = state.getPercentileAccumulator();
        percentileAggAccumulator.fromBinary(input.readSlice(length).getBytes());
        state.addMemoryUsage(state.getResult().estimatedInMemorySizeInBytes());
    }
}
