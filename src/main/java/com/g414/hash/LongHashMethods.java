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
package com.g414.hash;

/**
 * Utility methods for nifty hash implementations.
 */
public class LongHashMethods {
    /** take a bunch of random bytes and turn them into a single long */
    public long condenseBytesIntoLong(byte[] representation) {
        long seed = 0L;
        int pos = 0;

        for (byte b : representation) {
            long bLong = ((long) b) << (pos * 8);
            seed ^= bLong;
            pos = (pos + 1) % 8;
        }

        return seed;
    }
}
