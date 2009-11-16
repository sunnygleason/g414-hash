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
package com.g414.bloom;

import java.util.BitSet;

import com.g414.hash.LongHash;

/**
 * Bloom Filter implementation using a pluggable LongHash method.
 */
public class BloomFilter {
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
	private BitSet bitSet;

	/** Maximum size of this Bloom Filter (not enforced) */
	private final int maxSize;

	/** Maximum size of this Bloom Filter (not enforced) */
	private final int bitSetLength;

	/** Number of hash functions used per get/set */
	private final int k;

	/** LongHash implementation */
	private final LongHash hash;

	/**
	 * Construct a new Bloom Filter using the specified Hash implementation,
	 * maximum size (in # of elements inserted), and bits per item.
	 * 
	 * @param hash
	 * @param maxSize
	 * @param bitsPerItem
	 */
	public BloomFilter(LongHash hash, int maxSize, int bitsPerItem) {
		this.hash = hash;
		this.maxSize = maxSize;
		this.bitSetLength = this.maxSize * bitsPerItem;
		this.k = (int) Math.ceil(K_FACTOR * (double) (bitsPerItem));
		this.bitSet = new BitSet(bitSetLength);
	}

	/**
	 * Construct a new Bloom Filter using the specified Hash implementation and
	 * predefined FilterState.
	 * 
	 * @param hash
	 * @param maxSize
	 * @param bitsPerItem
	 */
	public BloomFilter(LongHash hash, FilterState state) {
		if (!hash.getName().equals(state.getHashName())) {
			throw new IllegalArgumentException(
					"Incompatible hash implementations: (" + hash.getName()
							+ " provided, " + state.getHashName()
							+ " expected)");
		}

		this.hash = hash;
		this.bitSet = state.getState();
		this.maxSize = state.getMaxSize();
		this.bitSetLength = state.getBitSetLength();
		this.k = state.getK();
	}

	/**
	 * Insert an object into the Bloom Filter.
	 * 
	 * @param object
	 */
	public void put(String object) {
		long[] hashIndex = hash.getLongHashCodes(object, this.k);

		for (long code : hashIndex) {
			this.bitSet.set(util.normalizeLong(code, this.bitSetLength));
		}
	}

	/**
	 * Tests an object for presence in the Bloom Filter.
	 * 
	 * @param object
	 */
	public boolean contains(String object) {
		long[] hashIndex = hash.getLongHashCodes(object, this.k);

		for (long code : hashIndex) {
			if (!bitSet.get(util.normalizeLong(code, this.bitSetLength))) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Merges two compatible bloom filters.
	 * 
	 * @param other
	 */
	public void union(BloomFilter other) {
		if ((this.k != other.k) || (this.maxSize != other.maxSize)
				|| (!this.hash.getName().equals(other.hash.getName()))) {
			throw new IllegalArgumentException("Incompatible Bloom Filters");
		}

		this.bitSet.or(other.bitSet);
	}

	/**
	 * Returns the internal Bloom State (for serialization, presumably).
	 * 
	 * @return
	 */
	public FilterState getState() {
		return new FilterState(this.hash.getName(), (BitSet) this.bitSet
				.clone(), this.maxSize, this.bitSetLength, this.k);
	}
}
