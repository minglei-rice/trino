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
package org.roaringbitmap.longlong;

import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Serialize/Deserialize the keys and values of this bitmap in little endian.
 *
 * {@link Roaring64NavigableMap} is not compatible with ClickHouse CRoaring, because of the incompatible
 * serialize/deserialize methods in which the map keys are write/read in different byte order, so this
 * class is designed to override Roaring64NavigableMap to eliminate the inconsistency.
 */
public class ClickHouseRoaring64NavigableMap
        extends Roaring64NavigableMap
{
    private static final boolean DEFAULT_ORDER_IS_SIGNED = false;

    private final boolean signedLongs;

    public ClickHouseRoaring64NavigableMap()
    {
        this(DEFAULT_ORDER_IS_SIGNED);
    }

    public ClickHouseRoaring64NavigableMap(boolean signedLongs)
    {
        super(signedLongs);
        this.signedLongs = signedLongs;
    }

    public static Roaring64NavigableMap bitmapOf(Roaring64NavigableMap bitmap)
    {
        requireNonNull(bitmap, "bitmap is null");
        final Roaring64NavigableMap ans = new ClickHouseRoaring64NavigableMap();
        ans.or(bitmap);
        return ans;
    }

    @Override
    public void serialize(DataOutput out)
            throws IOException
    {
        // TODO: Should we transport the performance tweak 'doCacheCardinalities'?
        out.writeBoolean(signedLongs);
        out.writeInt(getHighToBitmap().size());

        for (Map.Entry<Integer, BitmapDataProvider> entry : getHighToBitmap().entrySet()) {
            // write entry in little endian
            out.writeInt(Integer.reverseBytes(entry.getKey()));
            entry.getValue().serialize(out);
        }
    }

    @Override
    public void deserialize(DataInput in)
            throws IOException
    {
        this.clear();

        boolean readSignedLongs = in.readBoolean();
        checkArgument(signedLongs == readSignedLongs, "signedLongs flag should be equal to the deserialized");

        int nbHighs = in.readInt();
        for (int i = 0; i < nbHighs; i++) {
            int high = Integer.reverseBytes(in.readInt());
            RoaringBitmap provider = new RoaringBitmap();
            provider.deserialize(in);
            getHighToBitmap().put(high, provider);
        }
    }
}
