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

/**
 * Encapsulates a hash pointer: a long hash value and long positional offset.
 */
public class LongPointer {
    /** The hash value of the object */
    private final long hash;

    /** The offset position in the stream */
    private final long pos;

    /** Creates a new LongPointer */
    public LongPointer(long hash, long pos) {
        this.hash = hash;
        this.pos = pos;
    }

    /** get the hash value */
    public long getHash() {
        return hash;
    }

    /** get the position */
    public long getPos() {
        return pos;
    }
}