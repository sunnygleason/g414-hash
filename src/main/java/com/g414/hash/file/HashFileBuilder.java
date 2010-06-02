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
package com.g414.hash.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import com.g414.hash.file.impl.Calculations;

/**
 * Creates a HashFile. Inspired by DJB's CDB file format, we just introduce a
 * different hash function (murmur) and 64-bit hash cades and position offsets.
 */
public final class HashFileBuilder {
    /** size of write buffer for main data file */
    private static final int MAIN_WRITE_BUFFER_SIZE = 16 * 1024 * 1024; // 16MB

    /** size of write buffer for each of the radix files */
    private static final int HASH_WRITE_BUFFER_SIZE = 512 * 1024; // 512K

    /** size of write buffer for main data file */
    private static final int SEQUENTIAL_READ_BUFFER_SIZE = 16 * 1024 * 1024; // 16MB

    /** path to the main data file */
    private final String dataFilePath;

    /** filename prefix for each of the radix files */
    private final String radixFilePrefix;

    /** The RandomAccessFile for the hash file contents */
    private final DataOutputStream dataFile;

    /** The RandomAccessFile for the hash file pointers */
    private final DataOutputStream[] hashCodeList;

    /** log base 2 of the number of buckets */
    private final int bucketPower;

    /** length of the bucket table in the file header */
    private final int bucketTableLength;

    /** total length of the header */
    private final long totalHeaderLength;

    /** total number of elements in the hash table */
    private long count = 0;

    /** The number of entries in each bucket */
    private long[] bucketCounts = null;

    /** The position of the next key insertion in the file */
    private long dataFilePosition = -1;

    /** Whether finished has already been called */
    private boolean isFinished = false;

    /**
     * Constructs a HashFileBuilder object and prepares it for the creation of a
     * HashFile.
     */
    public HashFileBuilder(String filepath, long expectedElements)
            throws IOException {
        this.bucketPower = Calculations.getBucketPower(expectedElements);
        int buckets = 1 << this.bucketPower;

        if (this.bucketPower < 8 || this.bucketPower > 28) {
            throw new IllegalArgumentException(
                    "Bucket power must be between 8 and 28");
        }

        this.bucketTableLength = buckets * Calculations.LONG_POINTER_SIZE;
        this.totalHeaderLength = Calculations.getBucketTableOffset()
                + this.bucketTableLength;

        this.bucketCounts = new long[buckets];

        this.dataFilePath = filepath;
        this.radixFilePrefix = filepath + ".list.";

        this.dataFile = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(filepath), MAIN_WRITE_BUFFER_SIZE));

        this.hashCodeList = new DataOutputStream[Calculations.RADIX_FILE_COUNT];
        for (int i = 0; i < Calculations.RADIX_FILE_COUNT; i++) {
            String filename = String.format("%s%02X", radixFilePrefix, i);
            File hashFile = new File(filename);
            hashCodeList[i] = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(hashFile), HASH_WRITE_BUFFER_SIZE));
            hashFile.deleteOnExit();
        }

        this.dataFilePosition = this.totalHeaderLength;
        this.dataFile.write(new byte[(int) dataFilePosition]);
    }

    /**
     * Adds a new entry to the HashFile.
     * 
     * @param key
     *            The key to add to the database.
     * @param data
     *            The data associated with this key.
     * @exception java.io.IOException
     *                If an error occurs adding the key to the HashFile.
     */
    public synchronized void add(byte[] key, byte[] data) throws IOException {
        if (this.isFinished) {
            throw new IllegalStateException(
                    "cannot add() to a finished hashFile");
        }

        this.count += 1;

        this.dataFile.writeInt(key.length);
        this.dataFile.writeInt(data.length);
        this.dataFile.write(key);
        this.dataFile.write(data);

        long hashValue = Calculations.computeLongHash(key);
        int radix = Calculations.getRadix(hashValue, this.bucketPower);
        int bucket = Calculations.getBucket(hashValue, this.bucketPower);

        this.hashCodeList[radix].writeLong(hashValue);
        this.hashCodeList[radix].writeLong(this.dataFilePosition);
        this.bucketCounts[bucket]++;
        this.dataFilePosition = advanceBytes(dataFilePosition, 8 + key.length
                + data.length);
    }

    /**
     * Finishes building the HashFile.
     */
    public synchronized void finish() throws IOException {
        this.isFinished = true;

        this.dataFile.close();
        for (DataOutputStream stream : this.hashCodeList) {
            stream.close();
        }

        long[] bucketOffsets = computeBucketOffsets(this.bucketCounts);
        long pos = this.dataFilePosition;

        RandomAccessFile dataFileRandomAccess = new RandomAccessFile(
                dataFilePath, "rw");
        dataFileRandomAccess.seek(dataFilePosition);

        writeHashTable(radixFilePrefix, this.bucketPower, bucketOffsets,
                bucketCounts, dataFileRandomAccess);

        ByteBuffer slotTable = getBucketPositionTable(bucketOffsets,
                this.bucketCounts, pos);

        dataFileRandomAccess.seek(0L);
        dataFileRandomAccess.writeBytes(Calculations.MAGIC);
        dataFileRandomAccess.writeLong(Calculations.VERSION);
        dataFileRandomAccess.writeLong(this.count);
        dataFileRandomAccess.writeInt(this.bucketPower);
        dataFileRandomAccess.write(slotTable.array());

        for (int i = 0; i < Calculations.RADIX_FILE_COUNT; i++) {
            String filename = String.format("%s%02X", radixFilePrefix, i);
            (new File(filename)).delete();
        }

        dataFileRandomAccess.close();
    }

    /** Writes out a merged hash table file from all of the radix files */
    private static void writeHashTable(String radixFilePrefix, int bucketPower,
            long[] bucketStarts, long[] bucketCounts, DataOutput hashTableFile)
            throws IOException {
        for (int i = 0; i < Calculations.RADIX_FILE_COUNT; i++) {
            File radixFile = new File(getRadixFileName(radixFilePrefix, i));
            long radixFileLength = radixFile.length();

            /*
             * FIXME : int number of entries implies a limit of 32 billion
             * entries (2GB / 16bytes = 128MM, 128MM * 256 = 32BN); this is a
             * property of ByteBuffer only being able to allocate 2GB
             */
            if (radixFileLength > Integer.MAX_VALUE) {
                throw new RuntimeException("radix file too huge");
            }

            int entries = (int) radixFileLength
                    / Calculations.LONG_POINTER_SIZE;
            if (entries < 1) {
                continue;
            }

            final DataInputStream radixFileLongs = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(radixFile),
                            SEQUENTIAL_READ_BUFFER_SIZE));

            ByteBuffer hashTableBytes = ByteBuffer
                    .allocate((int) radixFileLength);
            LongBuffer hashTableLongs = hashTableBytes.asLongBuffer();

            for (int j = 0; j < entries; j++) {
                long hashCode = radixFileLongs.readLong();
                long position = radixFileLongs.readLong();

                int slot = Calculations.getBucket(hashCode, bucketPower);
                int baseSlot = Calculations.getBaseBucketForHash(hashCode,
                        bucketPower);

                int bucketStartIndex = (int) bucketStarts[slot];
                int baseBucketStart = (int) bucketStarts[baseSlot];
                int relativeBucketStartOffset = (int) (bucketStartIndex - baseBucketStart);
                int bucketCount = (int) bucketCounts[slot];

                int hashProbe = (int) (Math.abs(hashCode) % bucketCount);
                int slotIndexPos = relativeBucketStartOffset + hashProbe;

                boolean finished = false;
                while (!finished && bucketCount > 0) {
                    int probedHashCodeIndex = slotIndexPos * 2;
                    int probedPositionIndex = probedHashCodeIndex + 1;

                    long probedPosition = hashTableLongs
                            .get(probedPositionIndex);

                    if (probedPosition == 0) {
                        hashTableLongs.put(probedHashCodeIndex, hashCode);
                        hashTableLongs.put(probedPositionIndex, position);
                        finished = true;
                    } else {
                        if (bucketCount == 1) {
                            throw new RuntimeException(
                                    "shouldn't happen: collision in bucket of size 1!");
                        }

                        slotIndexPos += 1;
                        if (slotIndexPos >= (relativeBucketStartOffset + bucketCount)) {
                            slotIndexPos = relativeBucketStartOffset;
                        }
                    }
                }
            }

            hashTableFile.write(hashTableBytes.array());
        }
    }

    /**
     * Returns the hash list file name String corresponding to index i.
     */
    private static String getRadixFileName(String radixFilePrefix, int i) {
        return String.format("%s%02X", radixFilePrefix, i);
    }

    /**
     * Computes the bucket position offsets (position-based, not index).
     */
    private static ByteBuffer getBucketPositionTable(long[] bucketOffsets,
            long[] bucketSizes, long dataSegmentEndPosition) {
        int buckets = bucketOffsets.length;
        ByteBuffer slotTableBytes = ByteBuffer.allocate(buckets
                * Calculations.LONG_POINTER_SIZE);
        LongBuffer slotTable = slotTableBytes.asLongBuffer();

        for (int i = 0; i < bucketOffsets.length; i++) {
            long tableSize = bucketSizes[i];
            long tableOffset = bucketOffsets[i];

            slotTable.put(dataSegmentEndPosition
                    + (tableOffset * Calculations.LONG_POINTER_SIZE));
            slotTable.put(tableSize);
        }

        slotTableBytes.rewind();

        return slotTableBytes;
    }

    /**
     * Computes the offsets (index, not byte position) of the buckets.
     */
    private static long[] computeBucketOffsets(long[] bucketCounts) {
        long[] bucketOffsets = new long[bucketCounts.length];

        int curEntry = 0;
        for (int i = 0; i < bucketCounts.length; i++) {
            bucketOffsets[i] = curEntry;
            curEntry += bucketCounts[i];
        }

        return bucketOffsets;
    }

    /**
     * Advances the file pointer by <code>count</code> bytes, throwing an
     * exception if the postion has exhausted a long (hopefully not likely).
     */
    private static long advanceBytes(long pos, long count) throws IOException {
        long newpos = pos + count;
        if (newpos < count)
            throw new IOException("CDB file is too big.");
        return newpos;
    }
}
