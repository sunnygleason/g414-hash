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
package com.g414.hash.file.impl;

import com.g414.hash.impl.MurmurHash;

/**
 * Encapsulates calculations related to HashFiles.
 */
public class Calculations {
    /** HashFile magic value */
    public static final String MAGIC = "HaSH";

    /** File format version identifier */
    public static final int VERSION = 0x01000000;

    /** Size of a hash pointer (long hashcode plus long location) */
    public static final int LONG_POINTER_SIZE = 8 + 8;

    /** Number of radix files */
    public static final int RADIX_FILE_COUNT_POWER_OF_2 = 8;

    /** Number of radix files */
    public static final int RADIX_FILE_COUNT = (1 << RADIX_FILE_COUNT_POWER_OF_2);

    /** our trusty hash function */
    private static final MurmurHash hash = new MurmurHash();

    /** Computes the long hash value of a given byte[] key */
    public static long computeLongHash(byte[] key) {
        return hash.computeMurmurLongHash(key, 0L);
    }

    /** Computes the int hash value of a given byte[] key */
    public static int computeIntHash(byte[] key) {
        return hash.computeMurmurIntHash(key, 0);
    }
    
    /**
     * Returns the power-of-two number of buckets recommended for the specified
     * number of elements. If the specified number of elements is zero or less,
     * we use 24 as the bucket power (corresponding to 16MM buckets). Otherwise
     * the value is the log base 2 of the ceiling power of 2 minus 6
     * (corresponding to at most 64 items per bucket). This value is then
     * truncated between the maximum and minumum values of 28 and 8.
     */
    public static int getBucketPower(long expectedElements) {
        if (expectedElements <= 0) {
            return 24;
        }

        int powerOf2 = (64 - Long.numberOfLeadingZeros(expectedElements));
        long ceiling = 1L << powerOf2;

        if (expectedElements > ceiling) {
            powerOf2 += 1;
        }

        int recommended = powerOf2 - 6;

        return Math.min(28, Math.max(8, recommended));
    }

    /** returns the hash "slot" for a given hash value */
    public static int getBucket(long hashValue, int buckets) {
        return (int) (hashValue & ((1L << buckets) - 1L));
    }

    /** returns the base slot (by radix) corresponding to a given hash value */
    public static int getBaseBucketForHash(long hashValue, int buckets) {
        return getRadix(hashValue, buckets) << (buckets - 8);
    }

    /** returns the appropriate radix file for a given hash value */
    public static int getRadix(long hashValue, int buckets) {
        return (int) (getBucket(hashValue, buckets) >> (buckets - 8));
    }

    /** returns the bucket table offset in the header */
    public static int getBucketTableOffset() {
        /* HEADER: MAGIC, VERSION, COUNT, BUCKET_POWER */
        return MAGIC.length() + 8 + 8 + 4;
    }
}
