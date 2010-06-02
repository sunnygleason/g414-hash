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

import java.io.Serializable;
import java.util.BitSet;

/**
 * Serializable Bloom Filter state. This class should never change or else
 * previously-serialized classes will bust.
 */
public class FilterState implements Serializable {
    /** serial version uid */
    private static final long serialVersionUID = 1000001L;

    /** name of hash used to create this filter state */
    private final String hashName;

    /** BitSet containing bloom state */
    private final BitSet[] state;

    /** maxSize of bloom filter */
    private final long maxSize;

    /** size of each component bit set */
    private final int bitSetLength;

    /** number of hash functions per get/put operation */
    private final int k;

    /** whether to use long or int hash */
    private final boolean longHash;
    
    /**
     * Construct a new filter state object using the specified hash name,
     * bitset, maxSize and k value.
     * 
     * @param hashName
     * @param state
     * @param maxSize
     * @param k
     */
    public FilterState(String hashName, BitSet[] state, long maxSize,
            int bitSetLength, int k, boolean longHash) {
        this.hashName = hashName;
        this.state = state;
        this.maxSize = maxSize;
        this.bitSetLength = bitSetLength;
        this.k = k;
        this.longHash = longHash;
    }

    /** @return String hash name */
    public String getHashName() {
        return hashName;
    }

    /** @return BitSet filter state */
    public BitSet[] getState() {
        return state;
    }

    /** @return int max items in filter */
    public long getMaxSize() {
        return maxSize;
    }

    /** @return length of each bit set */
    public int getBitSetLength() {
        return bitSetLength;
    }

    /** @return int k number of hash values used */
    public int getK() {
        return k;
    }
    
    /** @return true if long hash, false otherwise */
    public boolean isLongHash() {
        return longHash;
    }
}
