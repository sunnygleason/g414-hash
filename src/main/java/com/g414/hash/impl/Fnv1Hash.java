/*
 * fnv - Fowler/Noll/Vo- hash code
 *
 * @(#) $Revision: 5.4 $
 * @(#) $Id: fnv.h,v 5.4 2009/07/30 22:49:13 chongo Exp $
 * @(#) $Source: /usr/local/src/cmd/fnv/RCS/fnv.h,v $
 *
 ***
 *
 * Fowler/Noll/Vo- hash
 *
 * The basis of this hash algorithm was taken from an idea sent
 * as reviewer comments to the IEEE POSIX P1003.2 committee by:
 *
 *      Phong Vo (http://www.research.att.com/info/kpv/)
 *      Glenn Fowler (http://www.research.att.com/~gsf/)
 *
 * In a subsequent ballot round:
 *
 *      Landon Curt Noll (http://www.isthe.com/chongo/)
 *
 * improved on their algorithm.  Some people tried this hash
 * and found that it worked rather well.  In an EMail message
 * to Landon, they named it the ``Fowler/Noll/Vo'' or FNV hash.
 *
 * FNV hashes are designed to be fast while maintaining a low
 * collision rate. The FNV speed allows one to quickly hash lots
 * of data while maintaining a reasonable collision rate.  See:
 *
 *      http://www.isthe.com/chongo/tech/comp/fnv/index.html
 *
 * for more details as well as other forms of the FNV hash.
 *
 * Please do not copyright this code.  This code is in the public domain.
 *
 * LANDON CURT NOLL DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
 * INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO
 * EVENT SHALL LANDON CURT NOLL BE LIABLE FOR ANY SPECIAL, INDIRECT OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF
 * USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 *
 * By:
 *  chongo <Landon Curt Noll> /\oo/\
 *      http://www.isthe.com/chongo/
 *
 * Share and Enjoy! :-)
 */
package com.g414.hash.impl;

import java.io.UnsupportedEncodingException;

import com.g414.hash.LongHash;

/**
 * FNV (Fowler/Noll/Vo) Hash "1" implementation suitable for Bloom Filter usage.
 * The difference between "1" and "1a" is the order of multiply / xor.
 * 
 * FNV hashes are designed to be fast while maintaining a low collision rate.
 * The FNV speed allows one to quickly hash lots of data while maintaining a
 * reasonable collision rate. The high dispersion of the FNV hashes makes them
 * well suited for hashing nearly identical strings such as URLs, hostnames,
 * filenames, text, IP addresses, etc.
 */
public class Fnv1Hash implements LongHash {
    public final static int FNV_32_PRIME = 0x01000193;
    public final static int FNV_32_INIT = 0x811c9dc5;

    public final static long FNV_64_PRIME = 0x100000001b3L;
    public final static long FNV_64_INIT = 0xcbf29ce484222325L;

    /** @see LongHash#getMagic() */
    @Override
    public byte[] getMagic() {
        return "__FNV1__".getBytes();
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
            return computeFnv1LongHash(object.getBytes("UTF-8"), FNV_64_INIT);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getLongHashCode(byte[]) */
    @Override
    public long getLongHashCode(byte[] data) {
        return computeFnv1LongHash(data, FNV_64_INIT);
    }

    /** @see LongHash#getIntHashCode(String) */
    @Override
    public int getIntHashCode(String object) {
        try {
            return computeFnv1IntHash(object.getBytes("UTF-8"), FNV_32_INIT);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getIntHashCode(byte[]) */
    @Override
    public int getIntHashCode(byte[] data) {
        return computeFnv1IntHash(data, FNV_32_INIT);
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

            long lastHash = FNV_64_INIT;

            for (int i = 0; i < k; i++) {
                long newHash = computeFnv1LongHash(representation, lastHash);
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

            int lastHash = FNV_32_INIT;

            for (int i = 0; i < k; i++) {
                int newHash = computeFnv1IntHash(representation, lastHash);
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
    public long computeFnv1LongHash(byte[] data, long seed) {
        final int len = data.length;
        long hVal = seed;

        for (int i = 0; i < len; i++) {
            hVal *= FNV_64_PRIME;
            hVal ^= data[i];
        }

        return hVal;
    }

    /**
     * Implementation of Fnv1 Hash, ported from 32-bit version.
     * 
     * @param data
     * @param seed
     * @return
     */
    public int computeFnv1IntHash(byte[] data, int seed) {
        final int len = data.length;
        int hVal = seed;

        for (int i = 0; i < len; i++) {
            hVal *= FNV_32_PRIME;
            hVal ^= data[i];
        }

        return hVal;
    }
}
