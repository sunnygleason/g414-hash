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
 */
public class MurmurHash_v9ABA implements LongHash {
    private final static long M = 0xc6a4a7935bd1e995L;

    /** @see LongHash#getName() */
    @Override
    public String getName() {
        return this.getClass().getName();
    }

    /** @see LongHash#getLongHashCode(String) */
    @Override
    public long getLongHashCode(String object) {
        return computeMurmurHash(object.getBytes(), 0L);
    }

    /** @see LongHash#getLongHashCodes(String, int) */
    @Override
    public long[] getLongHashCodes(String object, int k) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1");
        }

        long[] hashCodes = new long[k];
        byte[] representation = object.getBytes();

        for (int i = 0; i < k; i++) {
            hashCodes[i] = computeMurmurHash(representation, i);
        }

        return hashCodes;
    }

    /**
     * Implementation of Murmur Hash, ported from 64-bit version.
     * 
     * @param data
     * @param seed
     * @return
     */
    public long computeMurmurHash(byte[] data, long seed) {
        long r = 47;
        final int len = data.length;
        long h = seed ^ len;
        int i = 0;

        for (int end = len-8; i <= end; i += 8) {
            long k = _gatherLongLE(data, i);

            k *= M;
            k ^= k >>> r;
            k *= M;
            h *= M;
            h ^= k;
        }
        
        if (i < len) {
            switch (len - i) {
            case 7:
                h ^= (long) data[len - 7] << 48;
            case 6:
                h ^= (long) data[len - 6] << 40;
            case 5:
                h ^= (long) data[len - 5] << 32;
            case 4:
                h ^= (long) data[len - 4] << 24;
            case 3:
                h ^= (long) data[len - 3] << 16;
            case 2:
                h ^= (long) data[len - 2] << 8;
            case 1:
                h ^= (long) data[len - 1];
            }
            h *= M;
        }

        h ^= h >> r;
        h *= M;
        h ^= h >> r;

        return h;
    }

    private final static long _gatherLongLE(byte[] data, int index)
    {
        long l1 = _gatherIntLE(data, index);
        long l2 = _gatherIntLE(data, index+4);
        // need to do bit of juggling to get rid of pesky sign extension...
        return ((l1 << 32) >> 32) | (l2 << 32);
    }
    
    private final static int _gatherIntLE(byte[] data, int index)
    {    
        int i = data[index] & 0xFF;
        i |= (data[++index] & 0xFF) << 8;
        i |= (data[++index] & 0xFF) << 16;
        i |= (data[++index] << 24);
        return i;
    }
}
