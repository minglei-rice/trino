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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class AggIndexPercentileArrayStateSerializer
        implements AccumulatorStateSerializer<AggIndexPercentileArrayAccumulatorState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(AggIndexPercentileArrayAccumulatorState state, BlockBuilder out)
    {
        if (state.getResult() == null) {
            out.appendNull();
        }
        else {
            byte[] serialized = state.getPercentileAccumulator().toBinary();

            SliceOutput output = Slices.allocate(
                    SIZE_OF_DOUBLE +
                    SIZE_OF_INT + // number of percentiles
                            state.getPercentiles().size() * SIZE_OF_DOUBLE + // percentiles
                            SIZE_OF_INT + // digest length
                            serialized.length) // digest
                    .getOutput();

            // write percentiles
            List<Double> percentiles = state.getPercentiles();
            output.appendDouble(state.getWeight());
            output.appendInt(percentiles.size());
            for (double percentile : percentiles) {
                output.appendDouble(percentile);
            }

            output.appendInt(serialized.length);
            output.appendBytes(serialized);

            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, AggIndexPercentileArrayAccumulatorState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();

        state.setWeight(input.readDouble());
        // read number of percentiles
        int numPercentiles = input.readInt();

        ImmutableList.Builder<Double> percentilesListBuilder = ImmutableList.builder();
        for (int i = 0; i < numPercentiles; i++) {
            percentilesListBuilder.add(input.readDouble());
        }
        state.setPercentiles(percentilesListBuilder.build());

        // read digest
        int length = input.readInt();
        state.getPercentileAccumulator().fromBinary(input.readSlice(length).getBytes());
        state.addMemoryUsage(state.getResult().estimatedInMemorySizeInBytes());
    }
}
