package com.g414.bloom;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

import junit.framework.TestCase;

import com.g414.hash.LongHash;

public abstract class BloomFilterTestBase extends TestCase {
	public abstract LongHash getHash();

	public BloomTestConfig[] configs = new BloomTestConfig[] {
			new BloomTestConfig(10, 1, 8, 0),
			new BloomTestConfig(100, 50, 8, 0),
			new BloomTestConfig(1000, 500, 8, 0),
			new BloomTestConfig(1000000, 1000, 8, 0),
			new BloomTestConfig(1000000, 1000, 16, 0),
			new BloomTestConfig(1000000, 1000, 24, 0),
			new BloomTestConfig(10000000, 100000, 16, 0),
			new BloomTestConfig(10000000, 100000, 24, 0),
			new BloomTestConfig(1000000000, 1000000, 24, 0),
			new BloomTestConfig(1000000000, 1000000, 32, 0), };

	public void testBloomFilter() throws NoSuchAlgorithmException {
		for (BloomTestConfig config : this.configs) {
			System.out.println(config);

			BloomFilter filter = new BloomFilter(this.getHash(),
					config.maxSize, config.bitsPerItem);
			Random random = SecureRandom.getInstance("SHA1PRNG");
			random.setSeed(config.seed);

			for (int i = 0; i < config.maxSize; i++) {
				filter.put("test__" + random.nextInt(config.MAX_DOMAIN));
			}

			int pos = 0;
			for (int i = 0; i < config.MAX_DOMAIN; i++) {
				if (filter.contains("test__" + Integer.toString(i))) {
					pos += 1;
				}
			}

			int falsePos = pos - config.maxSize;

			int projectedErrors = (int) Math.ceil(config.MAX_DOMAIN
					* Math.pow(0.62, config.bitsPerItem));

			System.out.println(falsePos + "  " + projectedErrors);

			assertTrue(falsePos * 0.95 <= 1 + Math.ceil(config.MAX_DOMAIN
					* Math.pow(0.62, config.bitsPerItem)));
		}
	}

	public static class BloomTestConfig {
		public int MAX_DOMAIN;
		public int maxSize;
		public int bitsPerItem;
		public long seed;

		public BloomTestConfig(int MAX_DOMAIN, int maxSize, int bitsPerItem,
				long seed) {
			this.MAX_DOMAIN = MAX_DOMAIN;
			this.maxSize = maxSize;
			this.bitsPerItem = bitsPerItem;
			this.seed = seed;
		}

		@Override
		public String toString() {
			return "{MAX_DOMAIN=" + MAX_DOMAIN + ", maxSize=" + maxSize
					+ ", bitsPerItem=" + bitsPerItem + ", seed=" + seed + "}";
		}
	}
}
