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
package com.g414.hash.bloom;

import java.util.BitSet;

import com.g414.hash.LongHash;

/**
 * Large Bloom Filter implementation using a pluggable LongHash method. Uses 128
 * bitsets under the hood to allow theoretical scaling up to 256 GB of bits.
 */
public class BloomFilter {
    /** number of bitsets to employ */
    private static final int NUM_BITSETS = 128;

    /** radix mask for determining bitset (most significant byte of int) */
    private static final int BITSET_RADIX_MASK = 0x7F000000;

    /**
     * Factor to determine optimal number of hashes expressed in bits per item;
     * (0.7 * m / n )
     * 
     * @see http://en.wikipedia.org/wiki/Bloom_filter
     */
    private static final double K_FACTOR = 0.7;

    /** Private FilterMethods instance */
    private static final FilterMethods util = new FilterMethods();

    /** BitSet containing Bloom Filter state */
    private BitSet[] bitSet;

    /** Maximum size of this Bloom Filter (not enforced) */
    private final long maxSize;

    /** size of each individual bitset */
    private final int bitSetLength;

    /** Number of hash functions used per get/set */
    private final int k;

    /** LongHash implementation */
    private final LongHash hash;

    private final boolean longHash;

    /**
     * Construct a new Bloom Filter using the specified Hash implementation,
     * maximum size (in # of elements inserted), and bits per item.
     * 
     * @param hash
     * @param maxSize
     * @param bitsPerItem
     * @param longHash
     */
    public BloomFilter(LongHash hash, long maxSize, int bitsPerItem,
            boolean longHash) {
        this.hash = hash;
        this.k = (int) Math.ceil(K_FACTOR * (double) (bitsPerItem));
        this.maxSize = maxSize;
        this.bitSet = new BitSet[NUM_BITSETS];
        this.bitSetLength = (int) ((this.maxSize * bitsPerItem) / NUM_BITSETS);

        for (int i = 0; i < NUM_BITSETS; i++) {
            this.bitSet[i] = new BitSet(this.bitSetLength);
        }

        this.longHash = longHash;
    }

    public BloomFilter(LongHash hash, long maxSize, int bitsPerItem) {
        this(hash, maxSize, bitsPerItem, true);
    }

    /**
     * Construct a new Bloom Filter using the specified FilterState.
     * 
     * @param state
     */
    public BloomFilter(FilterState state) {
        try {
            this.hash = (LongHash) Class.forName(state.getHashName())
                    .newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Error while instantiating hash: (" + state.getHashName()
                            + ")");
        }

        this.bitSet = state.getState();
        this.maxSize = state.getMaxSize();
        this.bitSetLength = state.getBitSetLength();
        this.k = state.getK();
        this.longHash = state.isLongHash();
    }

    /**
     * Insert an object into the Bloom Filter. Simply ignores the value of the
     * putIfAbsent return value.
     * 
     * @param object
     */
    public void put(String object) {
        this.putIfAbsent(object);
    }

    /**
     * Insert an object into the Bloom Filter. Return true if object was
     * actually inserted, false if not inserted (already existed, possibly by
     * false positive).
     * 
     * @param object
     */
    public boolean putIfAbsent(String object) {
        boolean newlyInserted = false;

        if (this.longHash) {
            long[] hashIndex = hash.getLongHashCodes(object, this.k);

            for (long code : hashIndex) {
                int radix = util.computeRadix(code, BITSET_RADIX_MASK);
                BitSet bitSet = this.bitSet[radix];
                int pos = util.normalizeLong(code, this.bitSetLength);
                if (!bitSet.get(pos)) {
                    bitSet.set(pos);
                    newlyInserted = true;
                }
            }
        } else {
            int[] hashIndex = hash.getIntHashCodes(object, this.k);

            for (int code : hashIndex) {
                int radix = util.computeRadix(code, BITSET_RADIX_MASK);
                BitSet bitSet = this.bitSet[radix];
                int pos = util.normalizeInt(code, this.bitSetLength);
                if (!bitSet.get(pos)) {
                    bitSet.set(pos);
                    newlyInserted = true;
                }
            }
        }

        return newlyInserted;
    }

    /**
     * Tests an object for presence in the Bloom Filter.
     * 
     * @param object
     */
    public boolean contains(String object) {
        if (this.longHash) {
            long[] hashIndex = hash.getLongHashCodes(object, this.k);

            for (long code : hashIndex) {
                int radix = util.computeRadix(code, BITSET_RADIX_MASK);
                if (!bitSet[radix].get(util.normalizeLong(code,
                        this.bitSetLength))) {
                    return false;
                }
            }
        } else {
            int[] hashIndex = hash.getIntHashCodes(object, this.k);

            for (int code : hashIndex) {
                int radix = util.computeRadix(code, BITSET_RADIX_MASK);
                if (!bitSet[radix].get(util.normalizeInt(code,
                        this.bitSetLength))) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Adds the contents of the specified bloom filter into this bloom filter.
     * 
     * @param other
     */
    public void putAll(BloomFilter other) {
        if ((this.k != other.k) || (this.maxSize != other.maxSize)
                || (!this.hash.getName().equals(other.hash.getName()))) {
            throw new IllegalArgumentException("Incompatible Bloom Filters");
        }

        for (int i = 0; i < NUM_BITSETS; i++) {
            this.bitSet[i].or(other.bitSet[i]);
        }
    }

    /**
     * Returns the internal Bloom State (for serialization, presumably). NOTE:
     * external synchronization must be provided to protect against concurrent
     * writes during serialization.
     * 
     * @return
     */
    public FilterState getState() {
        return new FilterState(this.hash.getName(), this.bitSet, this.maxSize,
                this.bitSetLength, this.k, this.longHash);
    }
}
