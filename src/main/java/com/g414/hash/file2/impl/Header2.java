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
package com.g414.hash.file2.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;

import com.g414.hash.file2.ByteSize;

/**
 * Encapsulates information about a HashFile, version 2.
 */
public class Header2 {
    /** HashFile magic value */
    public static final String MAGIC = "HF*2";

    /** File format version identifier */
    public static final int VERSION = 0x02000000;

    /** expected number of elements, used for computing number of buckets */
    private final long expectedElements;

    /** the number of buckets */
    private final int buckets;

    /** log base 2 of the number of buckets */
    private final int bucketPower;

    private final AtomicLong elementCount = new AtomicLong();

    /** length of the bucket table in the file header */
    private final int bucketTableLength;

    /** total length of the header */
    private final long totalHeaderLength;

    private final int slotSize;

    private final ByteSize keySize;

    private final ByteSize valueSize;

    private final boolean isLongHash;

    private final boolean isLargeFile;

    private final boolean isLargeCapacity;

    private final boolean isAssociative;

    private volatile boolean isFinished;

    public Header2(long expectedElements, ByteSize keySize, ByteSize valueSize,
            boolean isLongHash, boolean isLargeCapacity, boolean isLargeFile) {
        this.expectedElements = expectedElements;
        this.keySize = keySize;
        this.valueSize = valueSize;

        if (keySize.getSize() > 4 || valueSize.getSize() > 4) {
            throw new IllegalArgumentException(
                    "HashFile Key and Value sizes must be 4 bytes or less");
        }

        this.isAssociative = ByteSize.ZERO.equals(keySize);
        this.isLongHash = isLongHash;
        this.isLargeCapacity = isLargeCapacity;
        this.isLargeFile = isLargeFile;

        this.bucketPower = Calculations2.getBucketPower(expectedElements);
        this.buckets = 1 << this.bucketPower;

        if (this.bucketPower < 8 || this.bucketPower > 28) {
            throw new IllegalArgumentException(
                    "Bucket power must be between 8 and 28");
        }

        this.slotSize = Calculations2.getBucketTableEntrySize(isLargeCapacity,
                isLargeFile);
        this.bucketTableLength = this.buckets * this.slotSize;
        this.totalHeaderLength = getBucketTableOffset()
                + this.bucketTableLength;

        this.isFinished = false;
    }

    protected Header2(boolean finished, long expectedElements, long count,
            ByteSize keySize, ByteSize valueSize, boolean isLongHash,
            boolean isLargeCapacity, boolean isLargeFile) {
        this(expectedElements, keySize, valueSize, isLongHash, isLargeCapacity,
                isLargeFile);
        this.elementCount.set(count);

        if (finished) {
            this.setFinished();
        }
    }

    public int getRadixFileCount() {
        return Calculations2.RADIX_FILE_COUNT;
    }

    public boolean isFinished() {
        return this.isFinished;
    }

    public void setFinished() {
        this.isFinished = true;
    }

    public void incrementElementCount() {
        if (this.isFinished) {
            throw new IllegalStateException("Cannot add to finished HashFile!");
        }

        this.elementCount.getAndIncrement();
    }

    public long getElementCount() {
        return elementCount.get();
    }

    public ByteSize getKeySize() {
        return keySize;
    }

    public int getBuckets() {
        return buckets;
    }

    public ByteSize getValueSize() {
        return valueSize;
    }

    public boolean isAssociative() {
        return isAssociative;
    }

    public boolean isLargeCapacity() {
        return isLargeCapacity;
    }

    public boolean isLargeFile() {
        return isLargeFile;
    }

    public boolean isLongHash() {
        return isLongHash;
    }

    public int getBucketPower() {
        return bucketPower;
    }

    public long getExpectedElements() {
        return expectedElements;
    }

    public int getSlotSize() {
        return slotSize;
    }

    public int getBucketTableLength() {
        return bucketTableLength;
    }

    public long getTotalHeaderLength() {
        return totalHeaderLength;
    }

    /** returns the bucket table offset in the header */
    public static int getBucketTableOffset() {
        /*
         * HEADER: MAGIC, VERSION, BUCKET_POWER, KEY_SIZE, VALUE_SIZE,
         * LONG_HASH, LONG_POS, LONG_SIZE, COUNT, NEGATIVE_ONE, NEGATIVE_ONE
         */
        return MAGIC.length() + 4 + 1 + 1 + 1 + 1 + 1 + 1 + 2 + 8 + 8 + 8;
    }

    public void write(RandomAccessFile file) throws IOException {
        file.writeBytes(MAGIC);
        file.writeInt(VERSION);
        file.writeByte(this.bucketPower);
        file.writeByte(this.keySize.getSize());
        file.writeByte(this.valueSize.getSize());
        file.writeByte(isLongHash ? 8 : 4);
        file.writeByte(isLargeCapacity ? 8 : 4);
        file.writeByte(isLargeFile ? 8 : 4);
        file.writeChar(0);
        file.writeLong(this.elementCount.get());
        file.writeLong(0xFFFFFFFFFFFFFFFFL);
        file.writeLong(0xFFFFFFFFFFFFFFFFL);
    }

    public static Header2 readHeader(RandomAccessFile hashFile)
            throws IOException {
        byte[] inMagic = new byte[MAGIC.length()];
        hashFile.readFully(inMagic);
        int version = (int) hashFile.readInt();

        String magic = new String(inMagic);
        if (!MAGIC.equals(magic) && version != VERSION) {
            throw new IOException("Incompatible HashFile file version");
        }

        int bucketPower = hashFile.readByte();

        ByteSize keySize = ByteSize.valueOf(hashFile.readByte());
        ByteSize valueSize = ByteSize.valueOf(hashFile.readByte());

        boolean isLongHash = hashFile.readByte() == 8;
        boolean isLargeCapacity = hashFile.readByte() == 8;
        boolean isLargeFile = hashFile.readByte() == 8;
        hashFile.readChar(); // unused

        long count = hashFile.readLong();

        final long marker1 = hashFile.readLong();
        final long marker2 = hashFile.readLong();
        if (marker1 != -1L || marker2 != -1L) {
            throw new IllegalArgumentException(
                    "Malformed marker block in header: m1=" + marker1 + ", m2="
                            + marker2);
        }

        Header2 header = new Header2(true, 1 << bucketPower, count, keySize,
                valueSize, isLongHash, isLargeCapacity, isLargeFile);
        header.setFinished();

        return header;
    }
}
