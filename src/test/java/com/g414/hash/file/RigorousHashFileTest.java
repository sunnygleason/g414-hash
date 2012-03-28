package com.g414.hash.file;

import java.io.File;
import java.util.List;

import org.testng.annotations.Test;

import com.g414.hash.file.HashEntry;
import com.g414.hash.file.HashFile;
import com.g414.hash.file.HashFileBuilder;
import com.g414.hash.file.basetest.HashFileGeneric;
import com.g414.hash.file.basetest.RigorousHashFileTestBase;
import com.g414.hash.file.basetest.HashFileGeneric.Builder;
import com.g414.hash.file.basetest.HashFileGeneric.Storage;

@Test
public class RigorousHashFileTest extends RigorousHashFileTestBase<HashEntry> {
    @Test(groups = "slow")
    public void testHashFileRigorously() throws Exception {
        long[] testCases = { 0L, 1L, 17L, 128L, 256L, 1024L, 1024L * 1024L,
                10L * 1024L * 1024L, 50L * 1024L * 1024L };

        boolean[] trueFalse = { false, true };

        for (long entries : testCases) {
            for (boolean longHash : trueFalse) {
                System.out.println("Testing hashfile: long? " + longHash
                        + " : key/value size = 4 : entries = " + entries);
                performTest(longHash, entries);
            }
        }
    }

    @Override
    public Builder getBuilder(final File tmp, final long entries) {
        return new HashFileGeneric.Builder() {
            HashFileBuilder instance = init();

            public HashFileBuilder init() {
                try {
                    return new HashFileBuilder(tmp.toString(), entries);
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
            HashFile instance = init();

            public HashFile init() {
                try {
                    return new HashFile(tmp.toString(), true);
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
                return instance.getMulti(key);
            }

            @Override
            public Iterable<HashEntry> elements() {
                try {
                    return HashFile.elements(tmp.toString());
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
