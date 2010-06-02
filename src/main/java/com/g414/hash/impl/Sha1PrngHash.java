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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

import com.g414.hash.LongHash;
import com.g414.hash.LongHashMethods;

/**
 * LongHash implementation that uses SHA1 Message Digest and PRNG to generate
 * hash codes. Pretty trusty. Version 2009-11-15T22:00.
 */
public class Sha1PrngHash implements LongHash {
    /** @see LongHash#getName() */
    @Override
    public String getName() {
        return this.getClass().getName();
    }

    /** @see LongHash#getLongHashCode(String) */
    @Override
    public long getLongHashCode(String object) {
        try {
            byte[] signature = getDigest(object.getBytes("UTF-8"));

            return LongHashMethods.condenseBytesIntoLong(signature);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getLongHashCode(byte[]) */
    @Override
    public long getLongHashCode(byte[] data) {
        byte[] signature = getDigest(data);

        return LongHashMethods.condenseBytesIntoLong(signature);
    }

    /** @see LongHash#getLongHashCodes(String, int) */
    @Override
    public long[] getLongHashCodes(String object, int k) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1");
        }

        try {
            byte[] signature = getDigest(object.getBytes("UTF-8"));
            long seed = LongHashMethods.condenseBytesIntoLong(signature);

            Random random = getRandom(seed);

            long[] hashCodes = new long[k];
            hashCodes[0] = seed;

            for (int i = 1; i < k; i++) {
                hashCodes[i] = random.nextLong();
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
            byte[] signature = getDigest(object.getBytes("UTF-8"));

            return LongHashMethods.condenseBytesIntoInt(signature);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** @see LongHash#getIntHashCode(byte[]) */
    @Override
    public int getIntHashCode(byte[] data) {
        byte[] signature = getDigest(data);

        return LongHashMethods.condenseBytesIntoInt(signature);
    }

    /** @see LongHash#getIntHashCodes(String, int) */
    @Override
    public int[] getIntHashCodes(String object, int k) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1");
        }

        try {
            byte[] signature = getDigest(object.getBytes("UTF-8"));
            long seed = LongHashMethods.condenseBytesIntoLong(signature);

            Random random = getRandom(seed);

            int[] hashCodes = new int[k];
            hashCodes[0] = (int) ((seed >> 32) & 0xFFFFFFFF)
                    | ((int) (seed & 0xFFFFFFFF));

            for (int i = 1; i < k; i++) {
                hashCodes[i] = random.nextInt();
            }

            return hashCodes;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Java doesn't recognize UTF-8?!");
        }
    }

    /** returns the message digest of the given object bytes */
    private static byte[] getDigest(byte[] object) {
        try {
            MessageDigest digest = MessageDigest.getInstance("sha1");

            return digest.digest(object);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** creates a SecureRandom using the specified seed */
    private static Random getRandom(long seed) {
        try {
            Random random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(seed);

            return random;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
