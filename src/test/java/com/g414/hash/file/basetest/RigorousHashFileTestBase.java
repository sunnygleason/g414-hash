package com.g414.hash.file.basetest;

import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

import org.testng.Assert;

public abstract class RigorousHashFileTestBase<T> {
    public abstract HashFileGeneric.Builder getBuilder(File tmp,
            final long entries);

    public abstract HashFileGeneric.Storage<T> getStorage(File tmp);

    public abstract void testHashFileRigorously() throws Exception;

    public void performTest(final boolean longHash, final long entries)
            throws Exception {
        final File tmp = File.createTempFile("hhhhhh", "ff");
        tmp.deleteOnExit();

        System.out.println(tmp.getAbsolutePath());

        HashFileGeneric.Builder hashWrite = getBuilder(tmp, entries);

        writeElements(hashWrite, entries, 0L, "key", "data");

        System.out.println(new Date() + " finishing...");
        hashWrite.finish();
        System.out.println(new Date() + " finished.");

        HashFileGeneric.Storage<T> hashFile = getStorage(tmp);

        verifyElements(hashFile, entries, 0L, "key", "data");

        tmp.delete();
    }

    private void writeElements(HashFileGeneric.Builder hf, long numElements,
            long seed, String keyPre, String valPre) throws Exception {
        Random rand = new Random(seed);
        for (long i = 0; i < numElements; i++) {
            byte[] key = (keyPre + rand.nextLong()).getBytes();
            hf.add(key, (valPre + rand.nextLong()).getBytes());
            hf.add(key, (valPre + rand.nextLong()).getBytes());
            hf.add(key, (valPre + rand.nextLong()).getBytes());
            reportStatus("wrote ", i, " records");
        }

        reportStatus("wrote ", numElements, " records", true);
    }

    private void verifyElements(HashFileGeneric.Storage<T> hf,
            long numElements, long seed, String keyPre, String valPre)
            throws Exception {
        Random rand = new Random(seed);
        for (long i = 0; i < numElements; i++) {
            byte[] keyBytes = (keyPre + rand.nextLong()).getBytes();
            byte[] expect1 = (valPre + rand.nextLong()).getBytes();
            byte[] expect2 = (valPre + rand.nextLong()).getBytes();
            byte[] expect3 = (valPre + rand.nextLong()).getBytes();

            byte[] theValue = hf.get(keyBytes);
            Assert.assertEquals(theValue, expect1);

            Iterator<byte[]> iter = hf.getMulti(keyBytes).iterator();
            byte[] actual1 = iter.next();
            byte[] actual2 = iter.next();
            byte[] actual3 = iter.next();

            Assert.assertEquals(actual1, expect1);
            Assert.assertEquals(actual2, expect2);
            Assert.assertEquals(actual3, expect3);

            reportStatus("verified ", i, " records");
        }
        reportStatus("verified ", numElements, " records", true);

        Random rand2 = new Random(seed);
        Iterator<T> iter2 = hf.elements().iterator();

        int count = 0;

        while (iter2.hasNext()) {
            byte[][] e1 = hf.asPair(iter2.next());
            byte[][] e2 = hf.asPair(iter2.next());
            byte[][] e3 = hf.asPair(iter2.next());

            byte[] keyBytes = (keyPre + rand2.nextLong()).getBytes();
            byte[] expect1 = (valPre + rand2.nextLong()).getBytes();
            byte[] expect2 = (valPre + rand2.nextLong()).getBytes();
            byte[] expect3 = (valPre + rand2.nextLong()).getBytes();

            byte[] actual1 = e1[1];
            byte[] actual2 = e2[1];
            byte[] actual3 = e3[1];

            Assert.assertEquals(e1[0], keyBytes);
            Assert.assertEquals(actual1, expect1);

            Assert.assertEquals(e2[0], keyBytes);
            Assert.assertEquals(actual2, expect2);

            Assert.assertEquals(e3[0], keyBytes);
            Assert.assertEquals(actual3, expect3);

            count += 1;

            reportStatus("verified iterator ", count, " records");
        }
        reportStatus("verified iterator ", count, " records", true);
    }

    private static void reportStatus(String prefix, long i, String suffix) {
        reportStatus(prefix, i, suffix, false);
    }

    private static void reportStatus(String prefix, long i, String suffix,
            boolean force) {
        if (force || (i % 100000 == 0)) {
            System.out.println(new Date() + " " + prefix + i + suffix);
        }
    }
}
