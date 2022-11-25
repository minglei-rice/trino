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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BufferUtils
{
    private BufferUtils() {}

    public interface BufferMaker
    {
        ByteBuffer newBuffer(int capacity);
    }

    public static ByteBuffer newBuffer(int capacity)
    {
        return ByteBuffer.allocate(capacity);
    }

    public static ByteBuffer newLittleEndianBuffer(int capacity)
    {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        if (buffer.order() != ByteOrder.LITTLE_ENDIAN) {
            buffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer;
    }
}
