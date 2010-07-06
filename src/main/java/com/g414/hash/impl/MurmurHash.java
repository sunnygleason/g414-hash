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

import static com.g414.hash.LongHashMethods.gatherIntLE;
import static com.g414.hash.LongHashMethods.gatherLongLE;
import static com.g414.hash.LongHashMethods.gatherPartialIntLE;
import static com.g414.hash.LongHashMethods.gatherPartialLongLE;

import java.io.UnsupportedEncodingException;

import com.g414.hash.LongHash;

/**
 * MurmurHash implementation suitable for Bloom Filter usage.
 * 
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 * 
 * <p>
 * The C version of MurmurHash 2.0 by Austin Appleby found at that site was
 * ported to Java by Andrzej Bialecki (ab at getopt org).
 * </p>
 * 
 * HISTORY: Updated 2010/05/01 by TS; performance enhancements (use int
 * arithmetic instead of long). Functionality should be exactly the same, but
 * created new class name for conservatism.
 */
public class MurmurHash implements LongHash {
    private final static long M_LONG = 0xc6a4a7935bd1e995L;
    private final static int R_LONG = 47;
    private final static int M_INT = 0x5bd1e995;
    private final static int R_INT = 24;
    private final static int R1_INT = 13;
    private final static int R2_INT = 15;

    /** @see LongHash#getMagic() */
    @Override
    public byte[] getMagic() {
        return "__MRMR__".getBytes();
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
            return computeMurmurLongHash(object.getBytes("UTF-8"), 0L);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getLongHashCode(byte[]) */
    @Override
    public long getLongHashCode(byte[] data) {
        return computeMurmurLongHash(data, 0L);
    }

    /** @see LongHash#getIntHashCode(String) */
    @Override
    public int getIntHashCode(String object) {
        try {
            return computeMurmurIntHash(object.getBytes("UTF-8"), 0);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getIntHashCode(byte[]) */
    @Override
    public int getIntHashCode(byte[] data) {
        return computeMurmurIntHash(data, 0);
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

            for (int i = 0; i < k; i++) {
                hashCodes[i] = computeMurmurLongHash(representation, i);
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

            for (int i = 0; i < k; i++) {
                hashCodes[i] = computeMurmurIntHash(representation, i);
            }

            return hashCodes;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /**
     * Implementation of Murmur Hash, ported from 64-bit version.
     * 
     * @param data
     * @param seed
     * @return
     */
    public long computeMurmurLongHash(byte[] data, long seed) {
        final int len = data.length;
        long h = seed ^ len;
        int i = 0;

        for (int end = len - 8; i <= end; i += 8) {
            long k = gatherLongLE(data, i);

            k *= M_LONG;
            k ^= k >> R_LONG;
            k *= M_LONG;

            h ^= k;
            h *= M_LONG;
        }

        if (i < len) {
            h ^= gatherPartialLongLE(data, i, (len - i));
            h *= M_LONG;
        }

        h ^= h >> R_LONG;
        h *= M_LONG;
        h ^= h >> R_LONG;

        return h;
    }

    /**
     * Implementation of Murmur Hash, ported from 32-bit version.
     * 
     * @param data
     * @param seed
     * @return
     */
    public int computeMurmurIntHash(byte[] data, int seed) {
        final int len = data.length;
        int h = seed ^ len;
        int i = 0;

        for (int end = len - 4; i <= end; i += 4) {
            int k = gatherIntLE(data, i);

            k *= M_INT;
            k ^= k >> R_INT;
            k *= M_INT;

            h *= M_INT;
            h ^= k;
        }

        if (i < len) {
            h ^= gatherPartialIntLE(data, i, (len - i));
            h *= M_INT;
        }

        h ^= h >> R1_INT;
        h *= M_INT;
        h ^= h >> R2_INT;

        return h;
    }

}
