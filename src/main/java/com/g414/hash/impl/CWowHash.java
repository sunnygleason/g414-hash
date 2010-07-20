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

import static com.g414.hash.LongHashMethods.LONG_LO_MASK;

import java.io.UnsupportedEncodingException;

import com.g414.hash.LongHash;
import com.g414.hash.LongHashMethods;

/**
 * Implementation of CrapWow hash, described at:
 * 
 * http://www.team5150.com/~andrew/noncryptohashzoo/
 */
public class CWowHash implements LongHash {
    public final static int CWOW_32_M = 0x57559429;
    public final static int CWOW_32_N = 0x5052acdb;

    public final static long CWOW_64_M = 0x95b47aa3355ba1a1L;
    public final static long CWOW_64_M_LO = CWOW_64_M & LONG_LO_MASK;
    public final static long CWOW_64_M_HI = CWOW_64_M >>> 32;
    
    public final static long CWOW_64_N = 0x8a970be7488fda55L;
    public final static long CWOW_64_N_LO = CWOW_64_N & LONG_LO_MASK;
    public final static long CWOW_64_N_HI = CWOW_64_N >>> 32;

    /** @see LongHash#getMagic() */
    @Override
    public byte[] getMagic() {
        return "__CWOW__".getBytes();
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
            return computeCWowLongHash(object.getBytes("UTF-8"), 0L);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getLongHashCode(byte[]) */
    @Override
    public long getLongHashCode(byte[] data) {
        return computeCWowLongHash(data, 0L);
    }

    /** @see LongHash#getIntHashCode(String) */
    @Override
    public int getIntHashCode(String object) {
        try {
            return computeCWowIntHash(object.getBytes("UTF-8"), 0);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getIntHashCode(byte[]) */
    @Override
    public int getIntHashCode(byte[] data) {
        return computeCWowIntHash(data, 0);
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
                long newHash = computeCWowLongHash(representation, i);
                hashCodes[i] = newHash;
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
                int newHash = computeCWowIntHash(representation, i);
                hashCodes[i] = newHash;
            }

            return hashCodes;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /**
     * Implementation of CrapWow Hash, ported from 64-bit version.
     */
    public long computeCWowLongHash(byte[] data, long seed) {
        final int length = data.length;
        /* cwfold( a, b, lo, hi ): */
        /* p = (u64)(a) * (u128)(b); lo ^=(u64)p; hi ^= (u64)(p >> 64) */
        /* cwmixa( in ): cwfold( in, m, k, h ) */
        /* cwmixb( in ): cwfold( in, n, h, k ) */

        long hVal = seed;
        long k = length + seed + CWOW_64_N;

        int pos = 0;
        int len = length;

        long aL, aH, bL, bH;
        long r1, r2, r3, rML;
        long pL;
        long pH;

        while (len >= 16) {
            /* cwmixb(X) = cwfold( X, N, hVal, k ) */
            aL = LongHashMethods.gatherIntLE(data, pos) & LONG_LO_MASK; pos += 4;
            aH = LongHashMethods.gatherIntLE(data, pos) & LONG_LO_MASK; pos += 4;
            bL = CWOW_64_N_LO; bH = CWOW_64_N_HI;
            r1 = aL * bL; r2 = aH * bL; r3 = aL * bH;
            rML = (r1 >>> 32) + (r2 & LONG_LO_MASK) + (r3 & LONG_LO_MASK);
            pL = (r1 & LONG_LO_MASK) + ((rML & LONG_LO_MASK) << 32);
            pH = (aH * bH) + (rML >>> 32);
            hVal ^= pL; k ^= pH;

            /* cwmixa(Y) = cwfold( Y, M, k, hVal ) */
            aL = LongHashMethods.gatherIntLE(data, pos) & LONG_LO_MASK; pos += 4;
            aH = LongHashMethods.gatherIntLE(data, pos) & LONG_LO_MASK; pos += 4;
            bL = CWOW_64_M_LO; bH = CWOW_64_M_HI;
            r1 = aL * bL; r2 = aH * bL; r3 = aL * bH;
            rML = (r1 >>> 32) + (r2 & LONG_LO_MASK) + (r3 & LONG_LO_MASK);
            pL = (r1 & LONG_LO_MASK) + ((rML & LONG_LO_MASK) << 32);
            pH = (aH * bH) + (rML >>> 32);
            k ^= pL; hVal ^= pH;

            len -= 16;
        }

        if (len >= 8) {
            /* cwmixb(X) = cwfold( X, N, hVal, k ) */
            aL = LongHashMethods.gatherIntLE(data, pos) & LONG_LO_MASK; pos += 4;
            aH = LongHashMethods.gatherIntLE(data, pos) & LONG_LO_MASK; pos += 4;
            bL = CWOW_64_N_LO; bH = CWOW_64_N_HI;
            r1 = aL * bL; r2 = aH * bL; r3 = aL * bH;
            rML = (r1 >>> 32) + (r2 & LONG_LO_MASK) + (r3 & LONG_LO_MASK);
            pL = (r1 & LONG_LO_MASK) + ((rML & LONG_LO_MASK) << 32);
            pH = (aH * bH) + (rML >>> 32);
            hVal ^= pL; k ^= pH;

            len -= 8;
        }

        if (len > 0) {
            aL = LongHashMethods.gatherPartialLongLE(data, pos, len);
            aH = aL >> 32;
            aL = aL & LONG_LO_MASK;
            
            /* cwmixa(Y) = cwfold( Y, M, k, hVal ) */
            bL = CWOW_64_M_LO;
            bH = CWOW_64_M_HI;
            r1 = aL * bL; r2 = aH * bL; r3 = aL * bH;
            rML = (r1 >>> 32) + (r2 & LONG_LO_MASK) + (r3 & LONG_LO_MASK);
            pL = (r1 & LONG_LO_MASK) + ((rML & LONG_LO_MASK) << 32);
            pH = (aH * bH) + (rML >>> 32);
            k ^= pL; hVal ^= pH;
        }

        /* cwmixb(X) = cwfold( X, N, hVal, k ) */
        aL = (hVal ^ (k + CWOW_64_N));
        aH = aL >> 32;
        aL = aL & LONG_LO_MASK;
        
        bL = CWOW_64_N_LO;
        bH = CWOW_64_N_HI;
        r1 = aL * bL; r2 = aH * bL; r3 = aL * bH;
        rML = (r1 >>> 32) + (r2 & LONG_LO_MASK) + (r3 & LONG_LO_MASK);
        pL = (r1 & LONG_LO_MASK) + ((rML & LONG_LO_MASK) << 32);
        pH = (aH * bH) + (rML >>> 32);
        hVal ^= pL; k ^= pH;

        hVal ^= k;

        return hVal;
    }

    /**
     * Implementation of CrapWow Hash, ported from 32-bit version.
     */
    public int computeCWowIntHash(byte[] data, int seed) {
        final int length = data.length;

        /* cwfold( a, b, lo, hi ): */
        /* p = (u32)(a) * (u64)(b); lo ^=(u32)p; hi ^= (u32)(p >> 32) */
        /* cwmixa( in ): cwfold( in, m, k, h ) */
        /* cwmixb( in ): cwfold( in, n, h, k ) */

        int hVal = seed;
        int k = length + seed + CWOW_32_N;
        long p = 0;

        int pos = 0;
        int len = length;

        while (len >= 8) {
            int i1 = LongHashMethods.gatherIntLE(data, pos);
            int i2 = LongHashMethods.gatherIntLE(data, pos + 4);

            /* cwmixb(i1) = cwfold( i1, N, hVal, k ) */
            p = i1 * (long) CWOW_32_N;
            k ^= p & LONG_LO_MASK;
            hVal ^= (p >> 32);
            /* cwmixa(i2) = cwfold( i2, M, k, hVal ) */
            p = i2 * (long) CWOW_32_M;
            hVal ^= p & LONG_LO_MASK;
            k ^= (p >> 32);

            pos += 8;
            len -= 8;
        }

        if (len >= 4) {
            int i1 = LongHashMethods.gatherIntLE(data, pos);

            /* cwmixb(i1) = cwfold( i1, N, hVal, k ) */
            p = i1 * (long) CWOW_32_N;
            k ^= p & LONG_LO_MASK;
            hVal ^= (p >> 32);

            pos += 4;
            len -= 4;
        }

        if (len > 0) {
            int i1 = LongHashMethods.gatherPartialIntLE(data, pos, len);

            /* cwmixb(i1) = cwfold( i1, N, hVal, k ) */
            p = (i1 & ((1 << (len * 8)) - 1)) * (long) CWOW_32_M;
            hVal ^= p & LONG_LO_MASK;
            k ^= (p >> 32);
        }

        p = (hVal ^ (k + CWOW_32_N)) * (long) CWOW_32_N;
        k ^= p & LONG_LO_MASK;
        hVal ^= (p >> 32);
        hVal ^= k;

        return hVal;
    }
}
