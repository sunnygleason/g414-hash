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
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import com.g414.hash.file2.ByteSize;
import com.g414.hash.file2.HashEntry;

/**
 * Implementation of iterators used by HashFile.
 */
public class Iterators2 {
    public static Iterable<byte[]> getEmptyIterable() {
        return new Iterable<byte[]>() {
            @Override
            public Iterator<byte[]> iterator() {
                return new Iterator<byte[]>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public byte[] next() {
                        return null;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException(
                                "remove() not supported");
                    }
                };
            }
        };
    }

    public static Iterable<HashEntry> getSequentialIterable(
            final String hashFilePath) {
        return new Iterable<HashEntry>() {
            @Override
            public Iterator<HashEntry> iterator() {
                try {
                    RandomAccessFile in = new RandomAccessFile(hashFilePath,
                            "r");

                    final DataInputStream input = new DataInputStream(
                            new BufferedInputStream(new FileInputStream(
                                    hashFilePath),
                                    FileOperations2.ITERATOR_READ_BUFFER_LENGTH));

                    final Header2 header = Header2.readHeader(in);
                    final FileOperations2 fileOps = FileOperations2
                            .fromHeader(header);

                    final long startPos = header.getTotalHeaderLength();
                    final long eod = fileOps.getEndOfData(in);

                    in.close();

                    input.skipBytes((int) startPos);

                    return new Iterator<HashEntry>() {
                        AtomicLong pos = new AtomicLong(startPos);

                        protected void finalize() {
                            try {
                                input.close();
                            } catch (Exception ignored) {
                            }
                        }

                        @Override
                        public synchronized boolean hasNext() {
                            return pos.get() < eod;
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException(
                                    "HashFile does not support remove()");
                        }

                        @Override
                        public synchronized HashEntry next() {
                            if (!hasNext()) {
                                throw new IllegalStateException(
                                        "next() called past end of iterator");
                            }

                            return fileOps.readHashEntry(input, pos);
                        }
                    };
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Unable to create hashfile iterator: "
                                    + e.getMessage(), e);
                }
            }

        };
    }

    public static Iterable<byte[]> getMultiIterable(
            final RandomAccessFile hashFile, final ByteBuffer hashTableOffsets,
            int bucketPower, final int slotSize, final ByteSize keySize,
            final ByteSize valueSize, final boolean isAssociative,
            final boolean isLongHash, final boolean isLargeCapacity,
            final boolean isLargeFile, final byte[] key) {
        final long currentHashKey = Calculations2.computeHash(key, isLongHash);
        int slot = Calculations2.getBucket(currentHashKey, bucketPower);
        int slotOffset = slot * slotSize;

        final long currentHashTableBasePosition = (isLargeCapacity ? hashTableOffsets
                .getLong(slotOffset)
                : hashTableOffsets.getInt(slotOffset)) << 2;

        int off = isLargeCapacity ? 8 : 4;

        final long currentHashTableSize = isLargeCapacity ? hashTableOffsets
                .getLong(slotOffset + off) : hashTableOffsets.getInt(slotOffset
                + off);

        if (currentHashTableSize == 0) {
            return Iterators2.getEmptyIterable();
        }

        final int initialProbe = (int) (Math.abs(currentHashKey) % currentHashTableSize);

        final ByteBuffer tableBytes = ByteBuffer.allocate(Calculations2
                .getHashTableEntrySize(isLongHash, isLargeFile)
                * ((int) currentHashTableSize));

        final ByteBuffer fileBytes = ByteBuffer
                .allocate(FileOperations2.RANDOM_READ_BUFFER_LENGTH);

        return new Iterable<byte[]>() {
            @Override
            public Iterator<byte[]> iterator() {
                return new Iterator<byte[]>() {
                    int probe = initialProbe;

                    byte[] next = advance();

                    private byte[] advance() {
                        try {
                            synchronized (hashFile) {
                                hashFile.seek(currentHashTableBasePosition);
                                hashFile.readFully(tableBytes.array());
                            }

                            long currentFindOperationIndex = 0;

                            while (currentFindOperationIndex < currentHashTableSize) {
                                int probeSlot = probe * slotSize;
                                long probedHashCode = isLongHash ? tableBytes
                                        .getLong(probeSlot) : tableBytes
                                        .getInt(probeSlot);
                                int off2 = isLongHash ? 8 : 4;

                                long probedPosition = (isLargeFile ? tableBytes
                                        .getLong(probeSlot + off2) : tableBytes
                                        .getInt(probeSlot + off2)) << 2;

                                currentFindOperationIndex += 1;
                                probe += 1;

                                if (probe >= currentHashTableSize) {
                                    probe = 0;
                                }

                                if (probedPosition == 0) {
                                    continue;
                                }

                                if (probedHashCode != currentHashKey) {
                                    continue;
                                }

                                synchronized (hashFile) {
                                    hashFile.seek(probedPosition);
                                    hashFile.read(fileBytes.array());
                                }

                                long keyLength = !isAssociative ? FileOperations2
                                        .read(fileBytes, keySize, 0)
                                        : 0;

                                if (keyLength != key.length) {
                                    continue;
                                }

                                long dataLength = FileOperations2
                                        .read(fileBytes, valueSize, keySize
                                                .getSize());

                                byte[] probedKey = new byte[(int) keyLength];
                                byte[] data = new byte[(int) dataLength];

                                int entrySize = valueSize.getSize()
                                        + (int) dataLength
                                        + (isAssociative ? 0 : keySize
                                                .getSize()
                                                + (int) keyLength);

                                if (entrySize < FileOperations2.RANDOM_READ_BUFFER_LENGTH) {
                                    fileBytes.rewind();
                                    fileBytes.get(new byte[keySize.getSize()
                                            + valueSize.getSize()]);
                                    fileBytes.get(probedKey);

                                    if (!isAssociative
                                            && !Arrays.equals(key, probedKey)) {
                                        continue;
                                    }

                                    fileBytes.get(data);
                                } else {
                                    synchronized (hashFile) {
                                        hashFile.seek(probedPosition
                                                + keySize.getSize()
                                                + valueSize.getSize());
                                        hashFile.readFully(probedKey);

                                        if (!isAssociative
                                                && !Arrays.equals(key,
                                                        probedKey)) {
                                            continue;
                                        }

                                        hashFile.readFully(data);
                                    }
                                }

                                return data;
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(
                                    "Error while finding key: "
                                            + e.getMessage(), e);
                        }

                        return null;
                    }

                    @Override
                    public boolean hasNext() {
                        return next != null;
                    }

                    @Override
                    public byte[] next() {
                        if (!hasNext()) {
                            throw new IllegalStateException(
                                    "next() called past end of iterator");
                        }

                        byte[] result = next;
                        next = advance();

                        return result;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException(
                                "remove() not supported");
                    }
                };
            }
        };
    }
}
