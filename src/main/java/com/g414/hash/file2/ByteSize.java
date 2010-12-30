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
package com.g414.hash.file2;

public enum ByteSize {
    ZERO(0), ONE(1), TWO(2), FOUR(4), EIGHT(8);

    private final int size;

    private ByteSize(int size) {
        this.size = size;
    }

    public int getSize() {
        return size;
    }

    public static ByteSize valueOf(int value) {
        switch (value) {
        case 0:
            return ZERO;
        case 1:
            return ONE;
        case 2:
            return TWO;
        case 4:
            return FOUR;
        case 8:
            return EIGHT;
        default:
            throw new IllegalArgumentException("Illegal Byte Size: " + value);
        }
    }
}
