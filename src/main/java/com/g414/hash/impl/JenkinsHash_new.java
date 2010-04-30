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
public class JenkinsHash_new implements LongHash {
    /** @see LongHash#getName() */
    @Override
    public String getName() {
        return this.getClass().getName();
    }

    /** @see LongHash#getLongHashCode(String) */
    @Override
    public long getLongHashCode(String object) {
        return computeJenkinsHash(object.getBytes(), 0L);
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
            a += gatherLongLE(k, i);
            b += gatherLongLE(k, i+8);
            c += gatherLongLE(k, i+16);            
            
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
        
        if (len > 0) {
            if (len >= 8) {
                a += gatherLongLE(k, i);                
                if (len >= 16) {
                    b += gatherLongLE(k, i+8);
                    // this is bit asymmetric; LSB is reserved for length (see above)
                    if (len > 16) {
                        c += (gatherPartialLongLE(k, i+16, len-16) << 8);
                    }
                } else if (len > 8) {
                    b += gatherPartialLongLE(k, i+8, len-8);
                }
            } else {
                a += gatherPartialLongLE(k, i, len);         
            }
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
