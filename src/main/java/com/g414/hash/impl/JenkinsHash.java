/*
 * You can use this free for any purpose. It's in the public domain.
 * It has no warranty.
 */
package com.g414.hash.impl;

import java.io.UnsupportedEncodingException;

import com.g414.hash.LongHash;
import com.g414.hash.LongHashMethods;

/**
 * <pre>
 * lookup8.c, by Bob Jenkins, May 2006, Public Domain.
 * 
 * You can use this free for any purpose. It's in the public domain.
 * It has no warranty.
 * </pre>
 * 
 * HISTORY: Updated 2010/05/01 by TS; performance enhancements (use int
 * arithmetic instead of long). Functionality should be exactly the same, but
 * created new class name for conservatism.
 * 
 * @see <a href="http://burtleburtle.net/bob/c/lookup3.c">lookup3.c</a>
 * @see <a href="http://www.ddj.com/184410284">Hash Functions (and how this
 *      function compares to others such as CRC, MD?, etc</a>
 * @see <a href="http://burtleburtle.net/bob/hash/doobs.html">Has update on the
 *      Dr. Dobbs Article</a>
 */
public class JenkinsHash implements LongHash {
    /** @see LongHash#getMagic() */
    @Override
    public byte[] getMagic() {
        return "__JENK__".getBytes();
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
            return computeJenkinsLongHash(object.getBytes("UTF-8"), 0L);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    @Override
    public long getLongHashCode(byte[] data) {
        return computeJenkinsLongHash(data, 0L);
    }

    /** @see LongHash#getLongHashCodes(String, int) */
    @Override
    public long[] getLongHashCodes(String object, int k) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1");
        }

        long[] hashCodes = new long[k];
        try {
            byte[] representation = object.getBytes("UTF-8");
    
            for (int i = 0; i < k; i++) {
                hashCodes[i] = computeJenkinsLongHash(representation, i);
            }
    
            return hashCodes;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getIntHashCode(String) */
    @Override
    public int getIntHashCode(String object) {
        try {
            return computeJenkinsIntHash(object.getBytes("UTF-8"), 0);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getIntHashCode(byte[]) */
    @Override
    public int getIntHashCode(byte[] data) {
        return computeJenkinsIntHash(data, 0);
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
                hashCodes[i] = computeJenkinsIntHash(representation, i);
            }
    
            return hashCodes;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
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
    public long computeJenkinsLongHash(byte[] k, long level) {
        /* Set up the internal state */
        long a = level;
        long b = level;
        /* the golden ratio; an arbitrary value */
        long c = 0x9e3779b97f4a7c13L;
        int len = k.length;

        /*---------------------------------------- handle most of the key */
        int i = 0;
        while (len >= 24) {
            a += LongHashMethods.gatherLongLE(k, i);
            b += LongHashMethods.gatherLongLE(k, i + 8);
            c += LongHashMethods.gatherLongLE(k, i + 16);

            /* mix64(a, b, c); */
            a -= b; a -= c; a ^= (c >> 43);
            b -= c; b -= a; b ^= (a << 9);
            c -= a; c -= b; c ^= (b >> 8);
            a -= b; a -= c; a ^= (c >> 38);
            b -= c; b -= a; b ^= (a << 23);
            c -= a; c -= b; c ^= (b >> 5);
            a -= b; a -= c; a ^= (c >> 35);
            b -= c; b -= a; b ^= (a << 49);
            c -= a; c -= b; c ^= (b >> 11); 
            a -= b; a -= c; a ^= (c >> 12);
            b -= c; b -= a; b ^= (a << 18);
            c -= a; c -= b; c ^= (b >> 22);
            /* mix64(a, b, c); */

            i += 24;
            len -= 24;
        }

        /*------------------------------------- handle the last 23 bytes */
        c += k.length;

        if (len > 0) {
            if (len >= 8) {
                a += LongHashMethods.gatherLongLE(k, i);
                if (len >= 16) {
                    b += LongHashMethods.gatherLongLE(k, i + 8);
                    // this is bit asymmetric; LSB is reserved for length (see
                    // above)
                    if (len > 16) {
                        c += (LongHashMethods.gatherPartialLongLE(k, i + 16,
                                len - 16) << 8);
                    }
                } else if (len > 8) {
                    b += LongHashMethods.gatherPartialLongLE(k, i + 8, len - 8);
                }
            } else {
                a += LongHashMethods.gatherPartialLongLE(k, i, len);
            }
        }

        /* mix64(a, b, c); */
        a -= b; a -= c; a ^= (c >> 43);
        b -= c; b -= a; b ^= (a << 9);
        c -= a; c -= b; c ^= (b >> 8);
        a -= b; a -= c; a ^= (c >> 38);
        b -= c; b -= a; b ^= (a << 23);
        c -= a; c -= b; c ^= (b >> 5);
        a -= b; a -= c; a ^= (c >> 35);
        b -= c; b -= a; b ^= (a << 49);
        c -= a; c -= b; c ^= (b >> 11); 
        a -= b; a -= c; a ^= (c >> 12);
        b -= c; b -= a; b ^= (a << 18);
        c -= a; c -= b; c ^= (b >> 22);
        /* mix64(a, b, c); */

        return c;
    }
    
    /*
    -------------------------------------------------------------------------------
    hashlittle() -- hash a variable-length key into a 32-bit value
      k       : the key (the unaligned variable-length array of bytes)
      length  : the length of the key, counting by bytes
      initval : can be any 4-byte value
    Returns a 32-bit value.  Every bit of the key affects every bit of
    the return value.  Two keys differing by one or two bits will have
    totally different hash values.

    The best hash table sizes are powers of 2.  There is no need to do
    mod a prime (mod is sooo slow!).  If you need less than 32 bits,
    use a bitmask.  For example, if you need only 10 bits, do
      h = (h & hashmask(10));
    In which case, the hash table should have hashsize(10) elements.

    If you are hashing n strings (uint8_t **)k, do it like this:
      for (i=0, h=0; i<n; ++i) h = hashlittle( k[i], len[i], h);

    By Bob Jenkins, 2006.  bob_jenkins@burtleburtle.net.  You may use this
    code any way you wish, private, educational, or commercial.  It's free.

    Use for hash table lookup, or anything where one collision in 2^^32 is
    acceptable.  Do NOT use for cryptographic purposes.
    -------------------------------------------------------------------------------
    */
    public int computeJenkinsIntHash(byte[] k, int level) {
        /* Set up the internal state */
        int a, b, c;
        a = b = c = (0xdeadbeef + (k.length << 2) + level);
        
        int len = k.length;

        /*---------------------------------------- handle most of the key */
        int i = 0;
        while (len >= 12) {
            a += LongHashMethods.gatherIntLE(k, i);
            b += LongHashMethods.gatherIntLE(k, i + 4);
            c += LongHashMethods.gatherIntLE(k, i + 8);

            /* mix(a, b, c); */
            a -= c;  a ^= LongHashMethods.rotateInt(c, 4);  c += b;
            b -= a;  b ^= LongHashMethods.rotateInt(a, 6);  a += c;
            c -= b;  c ^= LongHashMethods.rotateInt(b, 8);  b += a;
            a -= c;  a ^= LongHashMethods.rotateInt(c,16);  c += b;
            b -= a;  b ^= LongHashMethods.rotateInt(a,19);  a += c;
            c -= b;  c ^= LongHashMethods.rotateInt(b, 4);  b += a;
            /* mix(a, b, c); */

            i += 12;
            len -= 12;
        }

        /*------------------------------------- handle the last 23 bytes */
        c += k.length;

        if (len > 0) {
            if (len >= 4) {
                a += LongHashMethods.gatherIntLE(k, i);
                if (len >= 8) {
                    b += LongHashMethods.gatherIntLE(k, i + 4);
                    // this is bit asymmetric; LSB is reserved for length (see
                    // above)
                    if (len > 8) {
                        c += (LongHashMethods.gatherPartialIntLE(k, i + 8,
                                len - 8) << 8);
                    }
                } else if (len > 4) {
                    b += LongHashMethods.gatherPartialIntLE(k, i + 4, len - 4);
                }
            } else {
                a += LongHashMethods.gatherPartialIntLE(k, i, len);
            }
        }

        /* final(a, b, c); */
        c ^= b; c -= LongHashMethods.rotateInt(b,14);
        a ^= c; a -= LongHashMethods.rotateInt(c,11);
        b ^= a; b -= LongHashMethods.rotateInt(a,25);
        c ^= b; c -= LongHashMethods.rotateInt(b,16);
        a ^= c; a -= LongHashMethods.rotateInt(c,4);
        b ^= a; b -= LongHashMethods.rotateInt(a,14);
        c ^= b; c -= LongHashMethods.rotateInt(b,24);
        /* final(a, b, c); */

        return c;
    }
}
