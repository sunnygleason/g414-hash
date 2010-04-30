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

import static com.g414.hash.LongHashMethods.*;

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
public class MurmurHash_new implements LongHash {
    private final static long M = 0xc6a4a7935bd1e995L;
    private final static int R = 47;

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

    @Override
    public long getLongHashCode(byte[] data) {
        return computeMurmurHash(data, 0L);
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
        final long m = M;
        long h = seed ^ data.length;
        int i = 0;

        for (int end = data.length-8; i <= end; i += 8) {
            long k = gatherLongLE(data, i);

            k *= m;
            k ^= k >>> R;
            k *= m;
            h *= m;
            h ^= k;
        }
        
        final int len = data.length;
        if (i < len) {
            h *= gatherPartialLongLE(data, i, (len-i));
        }

        h ^= h >> R;
        h *= m;
        h ^= h >> R;

        return h;
    }
}
