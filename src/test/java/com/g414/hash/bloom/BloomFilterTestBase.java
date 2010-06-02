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

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.hash.LongHash;
import com.g414.hash.bloom.BloomFilter;

/**
 * Base class for Bloom Filter test implementations; runs a standard test to
 * ensure that false positives are under 5% for general cases. Note that this is
 * only a spot check - this should be augmented by more rigorous tests with more
 * expansive domain sizes.
 */
public abstract class BloomFilterTestBase {
    public abstract LongHash getHash();

    public BloomTestConfig[] fastConfigs = new BloomTestConfig[] {
            new BloomTestConfig(1000, 100, 8, 1),
            new BloomTestConfig(1000, 100, 12, 1),
            new BloomTestConfig(10000, 1000, 12, 1),
            new BloomTestConfig(10000, 5000, 16, 1),
            new BloomTestConfig(1000000, 50000, 16, 1),
            new BloomTestConfig(1000000, 100000, 8, 1),
            new BloomTestConfig(1000000, 100000, 16, 1),
            new BloomTestConfig(1000000, 100000, 24, 1) };

    public BloomTestConfig[] slowConfigs = new BloomTestConfig[] {
            new BloomTestConfig(10000000, 100000, 16, 1),
            new BloomTestConfig(10000000, 100000, 24, 1),
            new BloomTestConfig(1000000000, 1000000, 24, 1),
            new BloomTestConfig(1000000000, 1000000, 32, 1),
            new BloomTestConfig(10000000000L, 10000000, 24, 1),
            new BloomTestConfig(10000000000L, 10000000, 32, 1), };

    @Test
    public void testLongBloom_random_fast() throws Exception {
        doTestLongBloomFilter_Randomized(this.fastConfigs);
    }

    @Test(groups = "slow")
    public void testLongBloom_random_slow() throws Exception {
        doTestLongBloomFilter_Randomized(this.slowConfigs);
    }

    @Test
    public void testLongBloom_deterministic_fast() throws Exception {
        doTestLongBloomFilter_Deterministic(this.fastConfigs);
    }

    @Test(groups = "slow")
    public void testLongBloom_deterministic_slow() throws Exception {
        doTestLongBloomFilter_Deterministic(this.slowConfigs);
    }

    @Test
    public void testIntBloom_random_fast() throws Exception {
        doTestIntBloomFilter_Randomized(this.fastConfigs);
    }

    @Test(groups = "slow")
    public void testIntBloom_random_slow() throws Exception {
        doTestIntBloomFilter_Randomized(this.slowConfigs);
    }

    @Test
    public void testIntBloom_deterministic_fast() throws Exception {
        doTestIntBloomFilter_Deterministic(this.fastConfigs);
    }

    @Test(groups = "slow")
    public void testIntBloom_deterministic_slow() throws Exception {
        doTestIntBloomFilter_Deterministic(this.slowConfigs);
    }

    public void doTestLongBloomFilter_Randomized(BloomTestConfig[] configs)
            throws NoSuchAlgorithmException {
        for (BloomTestConfig config : configs) {
            LongHash hash = this.getHash();

            System.out.println("long bloom test randomized config ("
                    + hash.getName() + ") : " + config);

            BloomFilter filter = new BloomFilter(hash, config.maxSize,
                    config.bitsPerItem);

            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(config.seed);

            for (int i = 0; i < config.maxSize; i++) {
                filter.put("test__"
                        + (Math.abs(random.nextLong()) % config.MAX_DOMAIN));
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

            System.out.println("long bloom test randomized result : "
                    + falsePos + "  " + projectedErrors);

            Assert.assertTrue(falsePos * 0.95 <= 10 + Math
                    .ceil(config.MAX_DOMAIN
                            * Math.pow(0.62, config.bitsPerItem)));
        }
    }

    public void doTestLongBloomFilter_Deterministic(BloomTestConfig[] configs)
            throws NoSuchAlgorithmException {
        for (BloomTestConfig config : configs) {
            LongHash hash = this.getHash();

            System.out.println("long bloom test deterministic config ("
                    + hash.getName() + ") : " + config);

            BloomFilter filter = new BloomFilter(hash, config.maxSize,
                    config.bitsPerItem);

            for (int i = 0; i < config.maxSize; i++) {
                filter.put("test__" + i);
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

            System.out.println("long bloom test deterministic result : "
                    + falsePos + "  " + projectedErrors);

            Assert.assertTrue(falsePos * 0.95 <= 10 + Math
                    .ceil(config.MAX_DOMAIN
                            * Math.pow(0.62, config.bitsPerItem)));
        }
    }

    public void doTestIntBloomFilter_Randomized(BloomTestConfig[] configs)
            throws NoSuchAlgorithmException {
        for (BloomTestConfig config : configs) {
            LongHash hash = this.getHash();

            System.out.println("int bloom test randomized config ("
                    + hash.getName() + ") : " + config);

            BloomFilter filter = new BloomFilter(hash, config.maxSize,
                    config.bitsPerItem, false);

            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(config.seed);

            for (int i = 0; i < config.maxSize; i++) {
                filter.put("test__"
                        + (Math.abs(random.nextLong()) % config.MAX_DOMAIN));
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

            System.out.println("int bloom test randomized result : " + falsePos
                    + "  " + projectedErrors);

            Assert.assertTrue(falsePos * 0.95 <= 10 + Math
                    .ceil(config.MAX_DOMAIN
                            * Math.pow(0.62, config.bitsPerItem)));
        }
    }

    public void doTestIntBloomFilter_Deterministic(BloomTestConfig[] configs)
            throws NoSuchAlgorithmException {
        for (BloomTestConfig config : configs) {
            LongHash hash = this.getHash();

            System.out.println("int bloom test deterministic config ("
                    + hash.getName() + ") : " + config);

            BloomFilter filter = new BloomFilter(hash, config.maxSize,
                    config.bitsPerItem, false);

            for (int i = 0; i < config.maxSize; i++) {
                filter.put("test__" + i);
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

            System.out.println("int bloom test deterministic result : "
                    + falsePos + "  " + projectedErrors);

            Assert.assertTrue(falsePos * 0.95 <= 10 + Math
                    .ceil(config.MAX_DOMAIN
                            * Math.pow(0.62, config.bitsPerItem)));
        }
    }

    public static class BloomTestConfig {
        public long MAX_DOMAIN;
        public int maxSize;
        public int bitsPerItem;
        public long seed;

        public BloomTestConfig(long MAX_DOMAIN, int maxSize, int bitsPerItem,
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
