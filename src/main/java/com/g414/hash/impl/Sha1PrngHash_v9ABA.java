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
public class Sha1PrngHash_v9ABA implements LongHash {
	/** HashMethods implementation */
	private static final LongHashMethods util = new LongHashMethods();

	/** @see LongHash#getName() */
	@Override
	public String getName() {
		return this.getClass().getName();
	}

	/** @see LongHash#getLongHashCode(String) */
	@Override
	public long getLongHashCode(String object) {
		byte[] signature = getDigest(object.getBytes());
		long seed = util.condenseBytesIntoLong(signature);

		return seed;
	}

	/** @see LongHash#getLongHashCodes(String, int) */
	@Override
	public long[] getLongHashCodes(String object, int k) {
		if (k < 1) {
			throw new IllegalArgumentException("k must be >= 1");
		}

		byte[] signature = getDigest(object.getBytes());
		long seed = util.condenseBytesIntoLong(signature);

		Random random = getRandom(seed);

		long[] hashCodes = new long[k];
		hashCodes[0] = seed;

		for (int i = 1; i < k; i++) {
			hashCodes[i] = random.nextLong();
		}

		return hashCodes;
	}

	/** returns the message digest of the given object bytes */
	private static byte[] getDigest(byte[] object) {
		try {
			MessageDigest digest = MessageDigest.getInstance("sha1");

			return digest.digest(object);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	/** creates a SecureRandom using the specified seed */
	private static Random getRandom(long seed) {
		try {
			Random random = SecureRandom.getInstance("SHA1PRNG");
			random.setSeed(seed);

			return random;
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
}
