package com.g414.hash.file2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.g414.hash.file.basetest.HashFileGeneric;
import com.g414.hash.file.basetest.RigorousHashFileTestBase;
import com.g414.hash.file.basetest.HashFileGeneric.Builder;
import com.g414.hash.file.basetest.HashFileGeneric.Storage;

@Test
public class RigorousHashFile2Test extends RigorousHashFileTestBase<HashEntry> {
    boolean isAssociative = false;
    ByteSize keySize = ByteSize.ONE;
    ByteSize valueSize = ByteSize.ONE;
    boolean isLongHash = false;
    boolean isLargeCapacity = true;
    boolean isLargeFile = true;

    @Test(groups = "slow")
    public void testHashFileRigorously() throws Exception {
        long[] testCases = { 0L, 1L, 7L, 101L, 128L, 255L, 256L, 264L, 1011L,
                1024L, 512L * 1024L, 1024 * 1024L, 10 * 1024L * 1024L,
                50 * 1024L * 1024L };
        boolean[] trueFalse = { false, true };

        for (boolean longHash : trueFalse) {
            this.isLongHash = longHash;

            for (long entries : testCases) {
                for (ByteSize size : ByteSize.values()) {
                    if (size.equals(ByteSize.ZERO)
                            || size.equals(ByteSize.EIGHT)) {
                        continue;
                    }

                    this.keySize = size;
                    this.valueSize = size;

                    System.out.println("--------------");
                    System.out.println("Testing hashfile: long? " + longHash
                            + " : key/value size = " + size + " : entries = "
                            + entries);
                    performTest(longHash, entries);
                }
            }
        }
    }

    @Override
    public Builder getBuilder(final File tmp, final long entries) {

        return new HashFileGeneric.Builder() {
            HashFile2Builder instance = init();

            public HashFile2Builder init() {
                try {
                    return new HashFile2Builder(isAssociative, tmp.toString(),
                            entries, keySize, valueSize, isLongHash,
                            isLargeCapacity, isLargeFile);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void finish() {
                try {
                    instance.finish();
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
            }

            @Override
            public void add(byte[] key, byte[] value) {
                try {
                    instance.add(key, value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public Storage<HashEntry> getStorage(final File tmp) {
        return new HashFileGeneric.Storage<HashEntry>() {
            HashFile2 instance = init();

            public HashFile2 init() {
                try {
                    return new HashFile2(tmp.toString(), true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[] get(byte[] key) {
                return instance.get(key);
            }

            @Override
            public List<byte[]> getMulti(byte[] key) {
                List<byte[]> toReturn = new ArrayList<byte[]>();

                Iterable<byte[]> iter = instance.getMulti(key);
                for (byte[] val : iter) {
                    toReturn.add(val);
                }

                return toReturn;
            }

            @Override
            public Iterable<HashEntry> elements() {
                try {
                    return HashFile2.elements(tmp.toString());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[][] asPair(HashEntry object) {
                return object.asPair();
            }
        };
    }
}
