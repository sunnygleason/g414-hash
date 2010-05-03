package com.g414.hash.impl.prev;

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
 */
public class MurmurHash_v9ABA implements LongHash {
    /** @see LongHash#getName() */
    @Override
    public String getName() {
        return this.getClass().getName();
    }

    /** @see LongHash#getLongHashCode(String) */
    @Override
    public long getLongHashCode(String object) {
        try {
            return computeMurmurHash(object.getBytes("UTF-8"), 0L);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
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
        long m = 0xc6a4a7935bd1e995L;
        long r = 47;

        long h = seed ^ data.length;

        int len = data.length;
        int len_8 = len >> 3;

        for (int i = 0; i < len_8; i++) {
            int i_8 = i << 3;

            long k = data[i_8 + 7];
            k = k << 8;
            k = k | (data[i_8 + 6] & 0xff);
            k = k << 8;
            k = k | (data[i_8 + 5] & 0xff);
            k = k << 8;
            k = k | (data[i_8 + 4] & 0xff);
            k = k << 8;
            k = k | (data[i_8 + 3] & 0xff);
            k = k << 8;
            k = k | (data[i_8 + 2] & 0xff);
            k = k << 8;
            k = k | (data[i_8 + 1] & 0xff);
            k = k << 8;
            k = k | (data[i_8 + 0] & 0xff);

            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        long len_m = len_8 << 3;
        long left = len - len_m;

        if (left != 0) {
            if (left >= 7) {
                h ^= (long) data[len - 7] << 48;
            }
            if (left >= 6) {
                h ^= (long) data[len - 6] << 40;
            }
            if (left >= 5) {
                h ^= (long) data[len - 5] << 32;
            }
            if (left >= 4) {
                h ^= (long) data[len - 4] << 24;
            }
            if (left >= 3) {
                h ^= (long) data[len - 3] << 16;
            }
            if (left >= 2) {
                h ^= (long) data[len - 2] << 8;
            }
            if (left >= 1) {
                h ^= (long) data[len - 1];
            }

            h *= m;
        }

        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }
}