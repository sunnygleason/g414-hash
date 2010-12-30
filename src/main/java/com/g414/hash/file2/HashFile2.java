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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import com.g414.hash.file2.impl.FileOperations2;
import com.g414.hash.file2.impl.Header2;
import com.g414.hash.file2.impl.Iterators2;

/**
 * HashFile: inspired by DJB's CDB, we upgrade to 64-bit hash values and file
 * pointers to break the 4GB barrier. Note: the file format is not compatible
 * with CDB at all.
 */
public class HashFile2 {
    /** The RandomAccessFile for the HashFile */
    private RandomAccessFile hashFile = null;

    /**
     * The hash table offsets cached for efficiency. Entries consist of (pos,
     * len) values.
     */
    private final ByteBuffer hashTableOffsets;

    /** Header2 instance encapsulating HashFile v2 information */
    private final Header2 header;

    /** FileOperations2 instance encapsulating file access */
    private final FileOperations2 fileOps;

    /**
     * Creates an instance of HashFile and loads the given file path.
     * 
     * @param hashFileName
     *            The path to the HashFile to open.
     * @exception IOException
     *                if the HashFile could not be opened.
     */
    public HashFile2(String hashFileName) throws IOException {
        this(hashFileName, true);
    }

    /**
     * Creates an instance of HashFile and loads the given file path.
     * 
     * @param hashFileName
     *            The path to the HashFile to open.
     * @param eager
     *            whether to read bucket table eagerly
     * @exception IOException
     *                if the HashFile could not be opened.
     */
    public HashFile2(String hashFileName, boolean eager) throws IOException {
        hashFile = new RandomAccessFile(hashFileName, "r");

        this.header = Header2.readHeader(hashFile);
        this.fileOps = FileOperations2.fromHeader(header);

        this.hashTableOffsets = hashFile.getChannel().map(MapMode.READ_ONLY,
                Header2.getBucketTableOffset(), header.getBucketTableLength())
                .asReadOnlyBuffer();

        if (eager) {
            this.fileOps.readBucketEntries(hashTableOffsets);
        }
    }

    /** returns the number of entries in this HashFile */
    public long getCount() {
        return this.header.getElementCount();
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
    public byte[] get(byte[] key) {
        return this.fileOps.getFirst(this.hashFile, this.hashTableOffsets, key);
    }

    /**
     * Returns an iterable of values stored under the given key
     * 
     * @param key
     *            The key to search for.
     * @return The value for the given key, or <code>null</code> if no value
     *         with that key could be found.
     */
    public Iterable<byte[]> getMulti(byte[] key) {
        return this.fileOps.getMulti(this.hashFile, this.hashTableOffsets, key);
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
        return Iterators2.getSequentialIterable(hashFilePath);
    }
}
