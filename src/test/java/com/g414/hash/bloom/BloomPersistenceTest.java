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

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.g414.hash.impl.cur.JenkinsHash_vA5E5;
import com.g414.hash.impl.cur.MurmurHash_vA5E5;

@Test
public class BloomPersistenceTest {
    public void testBloomPersist() {
        BloomFilter bloom1 = new BloomFilter(new MurmurHash_vA5E5(), 1000, 8);
        bloom1.put("hello");
        bloom1.put("world");

        FilterState state = bloom1.getState();
        bloom1.put("not_included");

        BloomFilter bloom2 = new BloomFilter(new MurmurHash_vA5E5(), state);
        Assert.assertTrue(bloom2.contains("hello"));
        Assert.assertTrue(bloom2.contains("world"));
        Assert.assertTrue(!bloom2.contains("not_included"));

        try {
            new BloomFilter(new JenkinsHash_vA5E5(), state);

            throw new RuntimeException("unexpected success");
        } catch (IllegalArgumentException expected) {
            // good - expected
        }

        bloom2.put("this is a test");

        bloom1.union(bloom2);

        Assert.assertTrue(bloom1.contains("hello"));
        Assert.assertTrue(bloom1.contains("world"));
        Assert.assertTrue(bloom1.contains("not_included"));
        Assert.assertTrue(bloom1.contains("this is a test"));
        Assert.assertTrue(!bloom1.contains("probably not"));
    }
}
