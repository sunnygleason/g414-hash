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

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import com.g414.hash.file2.ByteSize;
import com.g414.hash.file2.HashEntry;

public class FileOperations2 {
    /** size of read buffer for iterator */
    public static final int ITERATOR_READ_BUFFER_LENGTH = 16 * 1024 * 1024; // 16MB

    /** size of read buffer for the random-access data file reads */
    public static final int RANDOM_READ_BUFFER_LENGTH = 2 * 1024; // 2KB

    /** size of write buffer for main data file */
    public static final int SEQUENTIAL_READ_BUFFER_SIZE = 16 * 1024 * 1024; // 16MB

    private final boolean isLargeFile;
    private final boolean isLargeCapacity;
    private final int slotSize;

    /** log base 2 of number of buckets */
    private final int bucketPower;

    /** total number of buckets */
    private final int buckets;

    private final ByteSize keySize;

    private final ByteSize valueSize;

    private final boolean isLongHash;

    private final boolean isAssociative;

    private final Header2 header;

    protected FileOperations2(Header2 header, int bucketPower, int buckets,
            ByteSize keySize, ByteSize valueSize, boolean isLongHash,
            boolean isLargeCapacity, boolean isLargeFile) {
        this.header = header;
        this.bucketPower = bucketPower;
        this.buckets = buckets;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.isAssociative = ByteSize.ZERO.equals(keySize);
        this.isLongHash = isLongHash;
        this.isLargeCapacity = isLargeCapacity;
        this.isLargeFile = isLargeFile;

        this.slotSize = Calculations2.getBucketTableEntrySize(isLargeCapacity,
                isLargeFile);
    }

    public static FileOperations2 fromHeader(Header2 header) {
        return new FileOperations2(header, header.getBucketPower(), header
                .getBuckets(), header.getKeySize(), header.getValueSize(),
                header.isLongHash(), header.isLargeCapacity(), header
                        .isLargeFile());
    }

    public void finish(long dataFilePosition, String dataFilePath,
            DataOutputStream dataFile, String radixFilePrefix,
            DataOutputStream[] hashCodeList, long[] bucketCounts)
            throws IOException, FileNotFoundException {
        if (header.isFinished()) {
            throw new IllegalStateException(
                    "HashFile finish() has already been called");
        }
        header.setFinished();

        dataFile.close();
        for (DataOutputStream stream : hashCodeList) {
            stream.close();
        }

        long[] bucketOffsets = Calculations2.computeBucketOffsets(bucketCounts);
        long pos = dataFilePosition;

        RandomAccessFile dataFileRandomAccess = new RandomAccessFile(
                dataFilePath, "rw");
        dataFileRandomAccess.seek(dataFilePosition);

        writeHashTable(radixFilePrefix, bucketOffsets, bucketCounts,
                dataFileRandomAccess);

        ByteBuffer slotTable = Calculations2.getBucketPositionTable(
                bucketOffsets, bucketCounts, pos, header.isLongHash(), header
                        .isLargeFile(), header.isLargeCapacity());

        dataFileRandomAccess.seek(0L);
        header.write(dataFileRandomAccess);

        dataFileRandomAccess.write(slotTable.array());

        for (int i = 0; i < Calculations2.RADIX_FILE_COUNT; i++) {
            String filename = String.format("%s%02X", radixFilePrefix, i);
            (new File(filename)).delete();
        }

        dataFileRandomAccess.close();
    }

    public void writeHashEntry(DataOutputStream[] hashCodeList,
            long[] bucketCounts, long dataFilePosition, byte[] key)
            throws IOException {
        long hashValue = Calculations2.computeHash(key, isLongHash);

        int radix = Calculations2.getRadix(hashValue, bucketPower);
        int bucket = Calculations2.getBucket(hashValue, bucketPower);

        write(hashCodeList[radix], isLongHash ? ByteSize.EIGHT : ByteSize.FOUR,
                hashValue);

        if ((dataFilePosition & 3) != 0) {
            throw new IllegalStateException(
                    "Offset into data file position must be multiple of 4 bytes");
        }

        write(hashCodeList[radix],
                isLargeFile ? ByteSize.EIGHT : ByteSize.FOUR,
                dataFilePosition >> 2);

        bucketCounts[bucket]++;
    }

    public long writeKeyVaue(DataOutputStream dataFile, long pos, byte[] key,
            byte[] value) throws IOException {
        if (header.isFinished()) {
            throw new IllegalStateException(
                    "cannot add() to a finished hashFile");
        }

        this.header.incrementElementCount();

        if (!isAssociative) {
            write(dataFile, keySize, key.length);
        }

        write(dataFile, valueSize, value.length);

        if (!isAssociative) {
            dataFile.write(key);
        }

        dataFile.write(value);

        int paddingSize = isAssociative ? writePadding(dataFile, valueSize,
                value) : writePadding(dataFile, keySize, valueSize, key, value);

        return advanceBytes(pos, (long) keySize.getSize()
                + (long) valueSize.getSize() + (long) key.length
                + (long) value.length + (long) paddingSize, isLargeFile);
    }

    public void readBucketEntries(ByteBuffer hashTableOffsets) {
        int slots = this.buckets;
        for (int i = 0; i < slots; i++) {
            getHashTablePosition(hashTableOffsets, i);
            getHashTableSize(hashTableOffsets, i);
        }
    }

    public long getEndOfData(RandomAccessFile in) throws IOException {
        in.seek(Header2.getBucketTableOffset());

        return read(in, isLargeFile ? ByteSize.EIGHT : ByteSize.FOUR) << 2;
    }

    public HashEntry readHashEntry(final DataInputStream input,
            final AtomicLong pos) {
        try {
            int keyLength = 0;
            if (!isAssociative) {
                keyLength = (int) read(input, header.getKeySize());
                pos.addAndGet(header.getKeySize().getSize());
            }

            int dataLength = (int) read(input, header.getValueSize());
            pos.addAndGet(header.getValueSize().getSize());

            byte[] key = new byte[0];
            if (!header.isAssociative()) {
                key = new byte[keyLength];
                input.readFully(key);
                pos.addAndGet(keyLength);
            }

            byte[] data = new byte[dataLength];
            input.readFully(data);
            pos.addAndGet(dataLength);

            int padding = 4 - ((header.getKeySize().getSize()
                    + header.getValueSize().getSize() + keyLength + dataLength) % 4);
            if (padding == 4) {
                padding = 0;
            }

            input.read(new byte[padding]);
            pos.addAndGet(padding);

            return new HashEntry(key, data);
        } catch (IOException ioException) {
            throw new IllegalArgumentException("invalid HashFile format");
        }
    }

    public byte[] getFirst(RandomAccessFile hashFile,
            ByteBuffer hashTableOffsets, byte[] key) {
        // return getMulti(hashFile, hashTableOffsets, key).iterator().next();
        if (hashFile == null) {
            throw new IllegalStateException(
                    "get() not allowed when HashFile is closed()");
        }

        long currentHashKey = Calculations2.computeHash(key, isLongHash);
        int slot = Calculations2.getBucket(currentHashKey, bucketPower);
        int slotOffset = slot * slotSize;

        long currentHashTableBasePosition = (isLargeCapacity ? hashTableOffsets
                .getLong(slotOffset) : hashTableOffsets.getInt(slotOffset)) << 2;

        int off = isLargeCapacity ? 8 : 4;

        long currentHashTableSize = isLargeCapacity ? hashTableOffsets
                .getLong(slotOffset + off) : hashTableOffsets.getInt(slotOffset
                + off);

        if (currentHashTableSize == 0) {
            return null;
        }

        int probe = (int) (Math.abs(currentHashKey) % currentHashTableSize);

        ByteBuffer tableBytes = ByteBuffer.allocate(Calculations2
                .getHashTableEntrySize(isLongHash, isLargeFile)
                * ((int) currentHashTableSize));

        ByteBuffer fileBytes = ByteBuffer.allocate(RANDOM_READ_BUFFER_LENGTH);

        try {
            synchronized (hashFile) {
                hashFile.seek(currentHashTableBasePosition);
                hashFile.readFully(tableBytes.array());
            }

            long currentFindOperationIndex = 0;

            while (currentFindOperationIndex < currentHashTableSize) {
                tableBytes.rewind();
                int probeSlot = probe * slotSize;
                long probedHashCode = isLongHash ? tableBytes
                        .getLong(probeSlot) : tableBytes.getInt(probeSlot);
                int off2 = isLongHash ? 8 : 4;

                long probedPosition = (isLargeFile ? tableBytes
                        .getLong(probeSlot + off2) : tableBytes
                        .getInt(probeSlot + off2)) << 2;

                if (probedPosition == 0) {
                    return null;
                }

                currentFindOperationIndex += 1;
                probe += 1;

                if (probe >= currentHashTableSize) {
                    probe = 0;
                }

                if (probedHashCode != currentHashKey) {
                    continue;
                }

                synchronized (hashFile) {
                    hashFile.seek(probedPosition);
                    hashFile.read(fileBytes.array());
                }

                long keyLength = isAssociative ? 0
                        : read(fileBytes, keySize, 0);

                if (!isAssociative && keyLength != key.length) {
                    continue;
                }

                long dataLength = read(fileBytes, valueSize, keySize.getSize());

                byte[] probedKey = new byte[(int) keyLength];
                byte[] data = new byte[(int) dataLength];

                int entrySize = valueSize.getSize()
                        + (int) dataLength
                        + (isAssociative ? 0 : keySize.getSize()
                                + (int) keyLength);

                if (entrySize < RANDOM_READ_BUFFER_LENGTH) {
                    fileBytes.rewind();
                    fileBytes.get(new byte[keySize.getSize()
                            + valueSize.getSize()]);
                    fileBytes.get(probedKey);

                    if (!isAssociative && !Arrays.equals(key, probedKey)) {
                        continue;
                    }

                    fileBytes.get(data);
                } else {
                    synchronized (hashFile) {
                        hashFile.seek(probedPosition + keySize.getSize()
                                + valueSize.getSize());
                        hashFile.readFully(probedKey);

                        if (!isAssociative && !Arrays.equals(key, probedKey)) {
                            continue;
                        }

                        hashFile.readFully(data);
                    }
                }

                return data;
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while finding key: "
                    + e.getMessage(), e);
        }

        return null;
    }

    public Iterable<byte[]> getMulti(final RandomAccessFile hashFile,
            final ByteBuffer hashTableOffsets, final byte[] key) {
        if (hashFile == null) {
            throw new IllegalStateException(
                    "get() not allowed when HashFile is closed()");
        }

        return Iterators2.getMultiIterable(hashFile, hashTableOffsets,
                bucketPower, slotSize, keySize, valueSize, isAssociative,
                isLongHash, isLargeCapacity, isLargeFile, key);
    }

    private long getHashTablePosition(ByteBuffer bucketData, int slotIndex) {
        int offset = slotIndex * slotSize;

        return (isLargeFile ? bucketData.getLong(offset) : bucketData
                .getInt(offset)) << 2;
    }

    public long getHashTableSize(ByteBuffer bucketData, int slotIndex) {
        int offset = (slotIndex * slotSize) + (isLargeFile ? 8 : 4);

        return isLargeCapacity ? bucketData.getLong(offset) : bucketData
                .getInt(offset);
    }

    private static int writePadding(DataOutputStream dataFile,
            ByteSize keySize, ByteSize valueSize, byte[] key, byte[] data)
            throws IOException {
        int paddingSize = 4 - ((keySize.getSize() + valueSize.getSize()
                + key.length + data.length) % 4);
        paddingSize = (paddingSize < 4) ? paddingSize : 0;

        if (paddingSize > 0) {
            dataFile.write(new byte[paddingSize]);
        }
        return paddingSize;
    }

    private static int writePadding(DataOutputStream dataFile,
            ByteSize valueSize, byte[] data) throws IOException {
        int paddingSize = 4 - ((valueSize.getSize() + data.length) % 4);
        paddingSize = (paddingSize < 4) ? paddingSize : 0;

        if (paddingSize > 0) {
            dataFile.write(new byte[paddingSize]);
        }
        return paddingSize;
    }

    /**
     * Returns the hash list file name String corresponding to index i.
     */
    private static String getRadixFileName(String radixFilePrefix, int i) {
        return String.format("%s%02X", radixFilePrefix, i);
    }

    /** Writes out a merged hash table file from all of the radix files */
    private void writeHashTable(String radixFilePrefix, long[] bucketStarts,
            long[] bucketCounts, DataOutput hashTableFile) throws IOException {
        int longPointerSize = Calculations2.getHashTableEntrySize(isLongHash,
                isLargeFile);

        for (int i = 0; i < Calculations2.RADIX_FILE_COUNT; i++) {
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

            int entries = (int) radixFileLength / longPointerSize;
            if (entries < 1) {
                continue;
            }

            final DataInputStream radixFileLongs = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(radixFile),
                            SEQUENTIAL_READ_BUFFER_SIZE));

            ByteBuffer hashTableBytes = ByteBuffer
                    .allocate((int) radixFileLength);

            for (int j = 0; j < entries; j++) {
                long hashCode = isLongHash ? radixFileLongs.readLong()
                        : radixFileLongs.readInt();
                long position = isLargeFile ? radixFileLongs.readLong()
                        : radixFileLongs.readInt();

                int slot = Calculations2.getBucket(hashCode, bucketPower);
                int baseSlot = Calculations2.getBaseBucketForHash(hashCode,
                        bucketPower);

                int bucketStartIndex = (int) bucketStarts[slot];
                int baseBucketStart = (int) bucketStarts[baseSlot];
                int relativeBucketStartOffset = (int) (bucketStartIndex - baseBucketStart);
                int bucketCount = (int) bucketCounts[slot];

                int hashProbe = (int) (Math.abs(hashCode) % bucketCount);
                int slotIndexPos = relativeBucketStartOffset + hashProbe;

                boolean finished = false;
                while (!finished && bucketCount > 0) {
                    int probedHashCodeIndex = slotIndexPos * longPointerSize;
                    int probedPositionIndex = probedHashCodeIndex
                            + (isLongHash ? 8 : 4);

                    long probedPosition = isLongHash ? hashTableBytes
                            .getLong(probedPositionIndex) : hashTableBytes
                            .getInt(probedPositionIndex);

                    if (probedPosition == 0) {
                        hashTableBytes.rewind();
                        int off = 0;

                        if (isLongHash) {
                            hashTableBytes.putLong(probedHashCodeIndex,
                                    hashCode);
                            off = 8;
                        } else {
                            hashTableBytes.putInt(probedHashCodeIndex,
                                    (int) hashCode);
                            off = 4;
                        }

                        if (isLargeFile) {
                            hashTableBytes.putLong(probedHashCodeIndex + off,
                                    position);
                        } else {
                            hashTableBytes.putInt(probedHashCodeIndex + off,
                                    (int) position);
                        }
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
     * Advances the file pointer by <code>count</code> bytes, throwing an
     * exception if the postion has exhausted a long (hopefully not likely).
     */
    private static long advanceBytes(long pos, long count, boolean isLongPos)
            throws IOException {
        long newpos = pos + count;
        if (newpos < count || newpos > Integer.MAX_VALUE)
            throw new IOException("HashFile is too big.");
        return newpos;
    }

    private static void write(DataOutput out, ByteSize size, long value)
            throws IOException {
        switch (size) {
        case EIGHT:
            out.writeLong(value);
            break;
        case FOUR:
            if (value > Integer.MAX_VALUE) {
                throw new IOException("Integer overflow : " + value);
            }
            out.writeInt((int) value);
            break;
        case TWO:
            if (value > Character.MAX_VALUE) {
                throw new IOException("Character overflow : " + value);
            }
            out.writeChar((int) value);
            break;
        case ONE:
            if (value > Byte.MAX_VALUE) {
                throw new IOException("Byte overflow : " + value);
            }
            out.writeByte((int) value);
            break;
        case ZERO:
            if (value > 0) {
                throw new IOException("Expected empty value!" + value);
            }
            break;
        }
    }

    public static long read(DataInput in, ByteSize size) throws IOException {
        switch (size) {
        case EIGHT:
            return in.readLong();
        case FOUR:
            return in.readInt();
        case TWO:
            return in.readChar();
        case ONE:
            return in.readByte();
        case ZERO:
            return 0;
        default:
            throw new IllegalArgumentException("Unknown ByteSize: " + size);
        }
    }

    public static long read(ByteBuffer in, ByteSize size, int pos)
            throws IOException {
        switch (size) {
        case EIGHT:
            return in.getLong(pos);
        case FOUR:
            return in.getInt(pos);
        case TWO:
            return in.getChar(pos);
        case ONE:
            return in.get(pos);
        case ZERO:
            return 0;
        default:
            throw new IllegalArgumentException("Unknown ByteSize: " + size);
        }
    }
}
