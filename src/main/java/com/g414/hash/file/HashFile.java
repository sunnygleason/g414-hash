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
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Iterator;

import com.g414.hash.file.impl.Calculations;

/**
 * HashFile: inspired by DJB's CDB, we upgrade to 64-bit hash values and file
 * pointers to break the 4GB barrier. Note: the file format is not compatible
 * with CDB at all.
 */
public class HashFile {
    /** size of read buffer for iterator */
    private static final int ITERATOR_READ_BUFFER_LENGTH = 16 * 1024 * 1024; // 16MB

    /** The RandomAccessFile for the HashFile */
    private RandomAccessFile hashFile = null;

    /** log base 2 of number of buckets */
    private final int bucketPower;

    /** total number of buckets */
    private final int buckets;

    /** total number of elements in the hash table */
    private final long count;

    /**
     * The hash table offsets cached for efficiency. Entries consist of (pos,
     * len) values.
     */
    private final LongBuffer hashTableOffsets;

    /** The number of hash slots searched under this key. */
    private long currentFindOperationIndex = 0;

    /** The hash value for the current key. */
    private long currentHashKey = 0;

    /** The number of hash entries in the hash table for the current key */
    private long currentHashTableSize = 0;

    /** The position of the hash table for the current key */
    private long currentHashTableBasePosition = 0;

    /** The position of the current key in the hash table */
    private long currentHashKeyTableIndex = 0;

    /**
     * Creates an instance of HashFile and loads the given file path.
     * 
     * @param hashFileName
     *            The path to the HashFile to open.
     * @exception IOException
     *                if the HashFile could not be opened.
     */
    public HashFile(String hashFileName) throws IOException {
        hashFile = new RandomAccessFile(hashFileName, "r");

        byte[] inMagic = new byte[Calculations.MAGIC.length()];
        hashFile.readFully(inMagic);
        int version = (int) hashFile.readLong();

        String magic = new String(inMagic);
        if (!Calculations.MAGIC.equals(magic)
                && version != Calculations.VERSION) {
            throw new IOException("Incompatible HashFile file version");
        }

        this.count = hashFile.readLong();
        this.bucketPower = hashFile.readInt();

        this.buckets = 1 << bucketPower;
        int slotTableLength = this.buckets * Calculations.LONG_POINTER_SIZE;

        ByteBuffer table = ByteBuffer.allocate(slotTableLength);
        hashFile.readFully(table.array());

        hashTableOffsets = table.asLongBuffer().asReadOnlyBuffer();
    }

    /** returns the number of entries in this HashFile */
    public long getCount() {
        return this.count;
    }

    /**
     * closes the HashFile.
     */
    public synchronized final void close() {
        try {
            hashFile.close();
        } catch (IOException ignored) {
        }
        hashFile = null;
    }

    /**
     * Finds the first value stored under the given key
     * 
     * @param key
     *            The key to search for.
     * @return The value for the given key, or <code>null</code> if no value
     *         with that key could be found.
     */
    public final synchronized byte[] get(byte[] key) {
        findStart(key);

        return findNext(key);
    }

    /**
     * Finds the next value stored under the given key.
     * 
     * @param key
     *            The key to search for.
     * @return The next value stored under the given key, or <code>null</code>
     *         if no record with that key could be found.
     */
    public final synchronized byte[] findNext(byte[] key) {
        currentHashKey = Calculations.computeHash(key);

        int slot = Calculations.getBucket(currentHashKey, this.bucketPower);

        currentHashTableBasePosition = hashTableOffsets.get(slot * 2);
        currentHashTableSize = hashTableOffsets.get((slot * 2) + 1);

        if (currentHashTableSize == 0) {
            return null;
        }

        long probe = Math.abs(currentHashKey) % currentHashTableSize;
        currentHashKeyTableIndex = currentHashTableBasePosition + (probe * 16);

        try {
            while (currentFindOperationIndex < currentHashTableSize) {
                hashFile.seek(currentHashKeyTableIndex);

                long probedHashCode = hashFile.readLong();
                long probedPosition = hashFile.readLong();

                if (probedPosition == 0) {
                    return null;
                }

                currentFindOperationIndex += 1;
                currentHashKeyTableIndex += 16;
                if (currentHashKeyTableIndex >= (currentHashTableBasePosition + (currentHashTableSize * 16))) {
                    currentHashKeyTableIndex = currentHashTableBasePosition;
                }

                if (probedHashCode != currentHashKey) {
                    continue;
                }

                hashFile.seek(probedPosition);
                int keyLength = hashFile.readInt();

                if (keyLength != key.length) {
                    continue;
                }

                int dataLength = hashFile.readInt();

                byte[] probedKey = new byte[keyLength];
                hashFile.readFully(probedKey);
                boolean match = Arrays.equals(key, probedKey);

                if (!match) {
                    continue;
                }

                byte[] data = new byte[dataLength];
                hashFile.readFully(data);

                return data;
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while finding key: "
                    + e.getMessage(), e);
        }

        return null;
    }

    /**
     * Prepares the class to search for the given key.
     * 
     * @param key
     *            The key to search for.
     */
    private final void findStart(byte[] key) {
        currentFindOperationIndex = 0;
    }

    /**
     * Returns an Iterable containing a HashEntry for each entry in the
     * HashFile.
     * 
     * @param hashFilePath
     *            The HashFile to read.
     * @return An Iterable containing a HashEntry for each entry in the HashFile
     * @exception IOException
     *                if an error occurs reading the constant database.
     */
    public static Iterable<HashEntry> elements(final String hashFilePath)
            throws IOException {
        return new Iterable<HashEntry>() {
            @Override
            public Iterator<HashEntry> iterator() {
                try {
                    RandomAccessFile in = new RandomAccessFile(hashFilePath,
                            "r");

                    final DataInputStream input = new DataInputStream(
                            new BufferedInputStream(new FileInputStream(
                                    hashFilePath), ITERATOR_READ_BUFFER_LENGTH));

                    byte[] inMagic = new byte[Calculations.MAGIC.length()];
                    in.readFully(inMagic);
                    int version = (int) in.readLong();

                    String magic = new String(inMagic);
                    if (!Calculations.MAGIC.equals(magic)
                            && version != Calculations.VERSION) {
                        throw new IOException(
                                "Incompatible HashFile file version");
                    }

                    final long _unused_count = in.readLong();
                    final int bucketPower = in.readInt();

                    final int buckets = 1 << bucketPower;
                    final long slotTableLength = buckets
                            * Calculations.LONG_POINTER_SIZE;
                    final long headerLength = Calculations.MAGIC.length() + 8
                            + 8 + 4 + slotTableLength;

                    final long eod = in.readLong();

                    in.close();

                    input.skipBytes((int) headerLength);

                    return new Iterator<HashEntry>() {
                        long pos = headerLength;

                        protected void finalize() {
                            try {
                                input.close();
                            } catch (Exception ignored) {
                            }
                        }

                        @Override
                        public synchronized boolean hasNext() {
                            return pos < eod;
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException(
                                    "HashFile does not support remove()");
                        }

                        @Override
                        public synchronized HashEntry next() {
                            try {
                                int keyLength = input.readInt();
                                pos += 4;
                                int dataLength = input.readInt();
                                pos += 4;

                                byte[] key = new byte[keyLength];
                                input.readFully(key);
                                pos += keyLength;

                                byte[] data = new byte[dataLength];
                                input.readFully(data);
                                pos += dataLength;

                                return new HashEntry(key, data);
                            } catch (IOException ioException) {
                                throw new IllegalArgumentException(
                                        "invalid HashFile format");
                            }
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
}
