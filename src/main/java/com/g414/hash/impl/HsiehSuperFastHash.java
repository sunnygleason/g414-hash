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
package com.g414.hash.impl;

import java.io.UnsupportedEncodingException;

import com.g414.hash.LongHash;
import com.g414.hash.LongHashMethods;

/**
 * SuperFastHash implementation based on work of Paul Hsieh. Includes a
 * provisional 64-bit version for experimentation. Warning! With 64-bit version,
 * shift widths are definitely sub-optimal and are subject to change.
 * 
 * http://www.azillionmonkeys.com/qed/hash.html
 */
public class HsiehSuperFastHash implements LongHash {
    private final int[] LEFT_SHIFT_WIDTHS = { 0, 10, 11, 16, 43, 42, 43, 48 };
    private final int[] RIGHT_SHIFT_WIDTHS = { 0, 1, 17, 11, 49, 33, 49, 43 };

    /** @see LongHash#getMagic() */
    @Override
    public byte[] getMagic() {
        return "__HSFH__".getBytes();
    }

    /** @see LongHash#getName() */
    @Override
    public String getName() {
        return this.getClass().getName();
    }

    /** @see LongHash#getLongHashCode(String) */
    @Override
    public long getLongHashCode(String object) {
        try {
            byte[] representation = object.getBytes("UTF-8");
            return computeHsiehLongHash(representation, representation.length);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getLongHashCode(byte[]) */
    @Override
    public long getLongHashCode(byte[] data) {
        if (data == null) {
            return 0;
        }

        return computeHsiehLongHash(data, data.length);
    }

    /** @see LongHash#getIntHashCode(String) */
    @Override
    public int getIntHashCode(String object) {
        try {
            byte[] representation = object.getBytes("UTF-8");
            return computeHsiehIntHash(representation, representation.length);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getIntHashCode(byte[]) */
    @Override
    public int getIntHashCode(byte[] data) {
        if (data == null) {
            return 0;
        }

        return computeHsiehIntHash(data, data.length);
    }

    /** @see LongHash#getLongHashCodes(String, int) */
    @Override
    public long[] getLongHashCodes(String object, int k) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1");
        }

        try {
            long[] hashCodes = new long[k];
            byte[] representation = object.getBytes("UTF-8");

            long lastHash = representation.length;

            for (int i = 0; i < k; i++) {
                long newHash = computeHsiehLongHash(representation, lastHash);
                hashCodes[i] = newHash;
                lastHash = newHash;
            }

            return hashCodes;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getIntHashCodes(String, int) */
    @Override
    public int[] getIntHashCodes(String object, int k) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1");
        }

        int[] hashCodes = new int[k];
        try {
            byte[] representation = object.getBytes("UTF-8");

            int lastHash = representation.length;

            for (int i = 0; i < k; i++) {
                int newHash = computeHsiehIntHash(representation, lastHash);
                hashCodes[i] = newHash;
                lastHash = newHash;
            }

            return hashCodes;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /**
     * Implementation of Fnv1 Hash, ported from 64-bit version.
     * 
     * @param data
     * @param seed
     * @return
     */
    public long computeHsiehLongHash(byte[] data, long seed) {
        if (data == null) {
            return seed;
        }

        final int len = data.length;
        long hVal = seed;

        for (int i = 0; i < len - 8; i += 8) {
            hVal += LongHashMethods.gatherIntLE(data, i);
            long tmp = LongHashMethods.gatherIntLE(data, i + 4) << 27;
            hVal = (hVal << 32) ^ tmp;
            hVal += hVal >> 43;
        }

        final int rem = len & 7;

        if (rem > 0) {
            final int i = len - rem;
            final int t1 = (rem >= 4) ? LongHashMethods.gatherIntLE(data, i)
                    : LongHashMethods.gatherPartialIntLE(data, i, Math.min(rem,
                            3));
            final int t2 = (rem > 4) ? LongHashMethods.gatherPartialIntLE(data,
                    i + 4, rem - 4) : 0;

            hVal += t1;
            hVal ^= hVal << LEFT_SHIFT_WIDTHS[rem];
            hVal ^= t2;
            hVal += hVal >> RIGHT_SHIFT_WIDTHS[rem];
        }

        hVal ^= hVal << 35;
        hVal += hVal >> 37;
        hVal ^= hVal << 36;
        hVal += hVal >> 49;
        hVal ^= hVal << 57;
        hVal += hVal >> 38;
        hVal ^= hVal << 3;
        hVal += hVal >> 5;
        hVal ^= hVal << 4;
        hVal += hVal >> 17;
        hVal ^= hVal << 25;
        hVal += hVal >> 6;

        return hVal;
    }

    /**
     * Implementation of Fnv1 Hash, ported from 32-bit version.
     * 
     * @param data
     * @param seed
     * @return
     */
    public int computeHsiehIntHash(byte[] data, int seed) {
        if (data == null) {
            return seed;
        }

        final int len = data.length;
        int hVal = seed;
        int rem = len & 3;

        for (int i = 0; i < len - 4; i += 4) {
            hVal += LongHashMethods.gatherPartialIntLE(data, i, 2);
            int tmp = LongHashMethods.gatherPartialIntLE(data, i + 2, 2) << 11;
            hVal = (hVal << 16) ^ tmp;
            hVal += hVal >> 11;
        }

        if (rem > 0) {
            final int i = len - rem;

            final int t1 = LongHashMethods.gatherPartialIntLE(data, i, Math
                    .min(rem, 2));
            final int t2 = rem > 2 ? LongHashMethods.gatherPartialIntLE(data,
                    i + 2, rem - 2) : 0;

            hVal += t1;
            hVal ^= hVal << LEFT_SHIFT_WIDTHS[rem];
            hVal ^= t2;
            hVal += hVal >> RIGHT_SHIFT_WIDTHS[rem];
        }

        hVal ^= hVal << 3;
        hVal += hVal >> 5;
        hVal ^= hVal << 4;
        hVal += hVal >> 17;
        hVal ^= hVal << 25;
        hVal += hVal >> 6;

        return hVal;
    }
}
