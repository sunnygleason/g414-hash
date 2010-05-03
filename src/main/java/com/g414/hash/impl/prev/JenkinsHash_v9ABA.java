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
package com.g414.hash.impl.prev;

import java.io.UnsupportedEncodingException;

import com.g414.hash.LongHash;

/**
 * <pre>
 * lookup8.c, by Bob Jenkins, May 2006, Public Domain.
 * 
 * You can use this free for any purpose. It's in the public domain.
 * It has no warranty.
 * </pre>
 * 
 * @see <a href="http://burtleburtle.net/bob/c/lookup3.c">lookup3.c</a>
 * @see <a href="http://www.ddj.com/184410284">Hash Functions (and how this
 *      function compares to others such as CRC, MD?, etc</a>
 * @see <a href="http://burtleburtle.net/bob/hash/doobs.html">Has update on the
 *      Dr. Dobbs Article</a>
 */
public class JenkinsHash_v9ABA implements LongHash {
    /** @see LongHash#getName() */
    @Override
    public String getName() {
        return this.getClass().getName();
    }

    /** @see LongHash#getLongHashCode(String) */
    @Override
    public long getLongHashCode(String object) {
        try {
            return computeJenkinsHash(object.getBytes("UTF-8"), 0L);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    @Override
    public long getLongHashCode(byte[] data) {
        return computeJenkinsHash(data, 0L);
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
            hashCodes[i] = computeJenkinsHash(representation, i);
        }

        return hashCodes;
    }

    /*
     * --------------------------------------------------------------------
     * hash() -- hash a variable-length key into a 64-bit value k : the key (the
     * unaligned variable-length array of bytes) level : can be any 8-byte value
     * Returns a 64-bit value. Every bit of the key affects every bit of the
     * return value. No funnels. Every 1-bit and 2-bit delta achieves avalanche.
     * About 41+5len instructions.
     * 
     * The best hash table sizes are powers of 2. There is no need to do mod a
     * prime (mod is sooo slow!). If you need less than 64 bits, use a bitmask.
     * For example, if you need only 10 bits, do h = (h & hashmask(10)); In
     * which case, the hash table should have hashsize(10) elements.
     * 
     * If you are hashing n strings (ub1 **)k, do it like this: for (i=0, h=0;
     * i<n; ++i) h = hash( k[i], len[i], h);
     * 
     * By Bob Jenkins, Jan 4 1997. bob_jenkins@burtleburtle.net. You may use
     * this code any way you wish, private, educational, or commercial, but I
     * would appreciate if you give me credit.
     * 
     * See http://burtleburtle.net/bob/hash/evahash.html Use for hash table
     * lookup, or anything where one collision in 2^^64 is acceptable. Do NOT
     * use for cryptographic purposes.
     * --------------------------------------------------------------------
     */
    public long computeJenkinsHash(byte[] k, long level) {
        /* Set up the internal state */
        long a = level;
        long b = level;
        /* the golden ratio; an arbitrary value */
        long c = 0x9e3779b97f4a7c13L;
        int len = k.length;

        /*---------------------------------------- handle most of the key */
        int i = 0;
        while (len >= 24) {
            a += (k[i] + ((long) k[i + 1] << 8) + ((long) k[i + 2] << 16)
                    + ((long) k[i + 3] << 24) + ((long) k[i + 4] << 32)
                    + ((long) k[i + 5] << 40) + ((long) k[i + 6] << 48) + ((long) k[i + 7] << 56));
            b += (k[i + 8] + ((long) k[i + 9] << 8) + ((long) k[i + 10] << 16)
                    + ((long) k[i + 11] << 24) + ((long) k[i + 12] << 32)
                    + ((long) k[i + 13] << 40) + ((long) k[i + 14] << 48) + ((long) k[i + 15] << 56));
            c += (k[i + 16] + ((long) k[i + 17] << 8)
                    + ((long) k[i + 18] << 16) + ((long) k[i + 19] << 24)
                    + ((long) k[i + 20] << 32) + ((long) k[i + 21] << 40)
                    + ((long) k[i + 22] << 48) + ((long) k[i + 23] << 56));

            /* mix64(a, b, c); */
            a -= b;
            a -= c;
            a ^= (c >> 43);
            b -= c;
            b -= a;
            b ^= (a << 9);
            c -= a;
            c -= b;
            c ^= (b >> 8);
            a -= b;
            a -= c;
            a ^= (c >> 38);
            b -= c;
            b -= a;
            b ^= (a << 23);
            c -= a;
            c -= b;
            c ^= (b >> 5);
            a -= b;
            a -= c;
            a ^= (c >> 35);
            b -= c;
            b -= a;
            b ^= (a << 49);
            c -= a;
            c -= b;
            c ^= (b >> 11);
            a -= b;
            a -= c;
            a ^= (c >> 12);
            b -= c;
            b -= a;
            b ^= (a << 18);
            c -= a;
            c -= b;
            c ^= (b >> 22);
            /* mix64(a, b, c); */

            i += 24;
            len -= 24;
        }

        /*------------------------------------- handle the last 23 bytes */
        c += k.length;
        switch (len) /* all the case statements fall through */
        {
        case 23:
            c += ((long) k[i + 22] << 56);
        case 22:
            c += ((long) k[i + 21] << 48);
        case 21:
            c += ((long) k[i + 20] << 40);
        case 20:
            c += ((long) k[i + 19] << 32);
        case 19:
            c += ((long) k[i + 18] << 24);
        case 18:
            c += ((long) k[i + 17] << 16);
        case 17:
            c += ((long) k[i + 16] << 8);
            /* the first byte of c is reserved for the length */
        case 16:
            b += ((long) k[i + 15] << 56);
        case 15:
            b += ((long) k[i + 14] << 48);
        case 14:
            b += ((long) k[i + 13] << 40);
        case 13:
            b += ((long) k[i + 12] << 32);
        case 12:
            b += ((long) k[i + 11] << 24);
        case 11:
            b += ((long) k[i + 10] << 16);
        case 10:
            b += ((long) k[i + 9] << 8);
        case 9:
            b += ((long) k[i + 8]);
        case 8:
            a += ((long) k[i + 7] << 56);
        case 7:
            a += ((long) k[i + 6] << 48);
        case 6:
            a += ((long) k[i + 5] << 40);
        case 5:
            a += ((long) k[i + 4] << 32);
        case 4:
            a += ((long) k[i + 3] << 24);
        case 3:
            a += ((long) k[i + 2] << 16);
        case 2:
            a += ((long) k[i + 1] << 8);
        case 1:
            a += ((long) k[i]);
            /* case 0: nothing left to add */
        }

        /* mix64(a, b, c); */
        a -= b;
        a -= c;
        a ^= (c >> 43);
        b -= c;
        b -= a;
        b ^= (a << 9);
        c -= a;
        c -= b;
        c ^= (b >> 8);
        a -= b;
        a -= c;
        a ^= (c >> 38);
        b -= c;
        b -= a;
        b ^= (a << 23);
        c -= a;
        c -= b;
        c ^= (b >> 5);
        a -= b;
        a -= c;
        a ^= (c >> 35);
        b -= c;
        b -= a;
        b ^= (a << 49);
        c -= a;
        c -= b;
        c ^= (b >> 11);
        a -= b;
        a -= c;
        a ^= (c >> 12);
        b -= c;
        b -= a;
        b ^= (a << 18);
        c -= a;
        c -= b;
        c ^= (b >> 22);
        /* mix64(a, b, c); */

        return c;
    }
}
