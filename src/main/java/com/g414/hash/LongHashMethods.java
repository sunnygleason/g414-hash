/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.g414.hash;

/**
 * Utility methods for nifty hash implementations.
 */
public class LongHashMethods {
    /** take a bunch of random bytes and turn them into a single long */
    public static final long condenseBytesIntoLong(byte[] representation) {
        long seed = 0L;
        int pos = 0;

        for (byte b : representation) {
            long bLong = ((long) b) << (pos * 8);
            seed ^= bLong;
            pos = (pos + 1) % 8;
        }

        return seed;
    }

    /** gather a long from the specified index into the byte array */
    public static final long gatherLongLE(byte[] data, int index) {
        int i1 = gatherIntLE(data, index);
        long l2 = gatherIntLE(data, index + 4);

        return uintToLong(i1) | (l2 << 32);
    }

    /**
     * gather a partial long from the specified index using the specified number
     * of bytes into the byte array
     */
    public static final long gatherPartialLongLE(byte[] data, int index,
            int available) {
        if (available >= 4) {
            int i = gatherIntLE(data, index);
            long l = uintToLong(i);

            available -= 4;

            if (available == 0) {
                return l;
            }

            int i2 = gatherPartialIntLE(data, index + 4, available);

            l <<= (available << 3);
            l |= (long) i2;

            return l;
        }

        return (long) gatherPartialIntLE(data, index, available);
    }

    /** perform unsigned extension of int to long */
    public static final long uintToLong(int i) {
        long l = (long) i;

        return (l << 32) >>> 32;
    }

    /** gather an int from the specified index into the byte array */
    public static final int gatherIntLE(byte[] data, int index) {
        int i = data[index] & 0xFF;

        i |= (data[++index] & 0xFF) << 8;
        i |= (data[++index] & 0xFF) << 16;
        i |= (data[++index] << 24);

        return i;
    }

    /**
     * gather a partial int from the specified index using the specified number
     * of bytes into the byte array
     */
    public static final int gatherPartialIntLE(byte[] data, int index,
            int available) {
        int i = data[index] & 0xFF;

        if (available > 1) {
            i |= (data[++index] & 0xFF) << 8;
            if (available > 2) {
                i |= (data[++index] & 0xFF) << 16;
            }
        }

        return i;
    }
}
