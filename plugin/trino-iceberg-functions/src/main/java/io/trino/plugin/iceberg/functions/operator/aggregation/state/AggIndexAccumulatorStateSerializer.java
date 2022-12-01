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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.spi.aggindex.AggIndex;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import org.apache.iceberg.types.Types;

import java.nio.charset.StandardCharsets;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class AggIndexAccumulatorStateSerializer
        implements AccumulatorStateSerializer<AggIndexAccumulatorState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(AggIndexAccumulatorState state, BlockBuilder out)
    {
        if (state.resultIsNull()) {
            out.appendNull();
        }
        else {
            byte[] serialized = state.getPartialAggAccumulator(state.getType(), state.getAggFunctionType()).toBinary();
            org.apache.iceberg.types.Type type = TypeConverter.toIcebergType(state.getType());
            byte[] typeBytes = type.toString().getBytes(StandardCharsets.UTF_8);
            SliceOutput output = Slices.allocate(SIZE_OF_BYTE + SIZE_OF_BYTE + typeBytes.length + SIZE_OF_INT + serialized.length).getOutput();
            // write agg function ordinal
            output.appendByte(state.getAggFunctionType().ordinal());
            // write type length
            output.appendByte(typeBytes.length);
            // write type string
            output.appendBytes(typeBytes);
            // write slice length
            output.appendInt(serialized.length);
            // write slice
            output.appendBytes(serialized);

            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, AggIndexAccumulatorState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();
        AggIndex.AggFunctionType functionType = AggIndex.AggFunctionType.values()[input.readByte()];
        int typeLength = input.readByte();
        org.apache.iceberg.types.Type icebergType = Types.fromPrimitiveString(input.readSlice(typeLength).toStringUtf8());
        Type type = TypeConverter.toTrinoType(icebergType, null);
        int length = input.readInt();
        Slice slice = input.readSlice(length);
        state.getPartialAggAccumulator(type, functionType).fromBinary(slice.getBytes());
    }
}
