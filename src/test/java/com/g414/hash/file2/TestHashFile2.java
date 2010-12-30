package com.g414.hash.file2;

import java.io.File;
import java.util.Iterator;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.hash.impl.MurmurHash;

@Test
public class TestHashFile2 {
    MurmurHash hash = new MurmurHash();

    public void testHashFile1k() throws Exception {
        doIt(ByteSize.ONE, ByteSize.ONE, false, 1000, 0L);
        doIt(ByteSize.ONE, ByteSize.TWO, false, 1000, 0L);
        doIt(ByteSize.TWO, ByteSize.ONE, false, 1000, 0L);
        doIt(ByteSize.TWO, ByteSize.TWO, false, 1000, 0L);
        doIt(ByteSize.FOUR, ByteSize.ONE, false, 1000, 0L);
        doIt(ByteSize.FOUR, ByteSize.TWO, false, 1000, 0L);
        doIt(ByteSize.FOUR, ByteSize.FOUR, false, 1000, 0L);
    }

    public void testHashFile10k() throws Exception {
        doIt(ByteSize.ONE, ByteSize.ONE, false, 10000, 12345L);
        doIt(ByteSize.ONE, ByteSize.TWO, false, 10000, 12345L);
        doIt(ByteSize.TWO, ByteSize.ONE, false, 10000, 12345L);
        doIt(ByteSize.TWO, ByteSize.TWO, false, 10000, 12345L);
        doIt(ByteSize.FOUR, ByteSize.ONE, false, 10000, 12345L);
        doIt(ByteSize.FOUR, ByteSize.TWO, false, 10000, 12345L);
        doIt(ByteSize.FOUR, ByteSize.FOUR, false, 10000, 12345L);
    }

    private static void doIt(ByteSize keySize, ByteSize valueSize,
            boolean longHash, long entries, long seed) throws Exception {
        File tmp = File.createTempFile("hhhhhh", "ff");
        tmp.deleteOnExit();
        System.out.println(tmp.getAbsolutePath());

        HashFile2Builder hashWrite = new HashFile2Builder(false, tmp
                .getAbsolutePath(), 8, keySize, valueSize, longHash, false,
                false);

        writeElements(hashWrite, 10000, 0L, "key", "data");

        hashWrite.finish();

        verifyElements(tmp, 10000, 0L, "key", "data");
    }

    private static void writeElements(HashFile2Builder hf, long numElements,
            long seed, String keyPre, String valPre) throws Exception {

        Random rand = new Random(seed);
        for (long i = 0; i < numElements; i++) {
            byte[] key = (keyPre + rand.nextLong()).getBytes();
            hf.add(key, (valPre + rand.nextLong()).getBytes());
            hf.add(key, (valPre + rand.nextLong()).getBytes());
            hf.add(key, (valPre + rand.nextLong()).getBytes());
        }
    }

    private static void verifyElements(File tmp, long numElements, long seed,
            String keyPre, String valPre) throws Exception {
        HashFile2 hf = new HashFile2(tmp.toString());

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
        }

        Random rand2 = new Random(seed);
        Iterator<HashEntry> iter2 = hf.elements(tmp.toString()).iterator();

        int count = 0;

        while (iter2.hasNext()) {
            HashEntry e1 = iter2.next();
            HashEntry e2 = iter2.next();
            HashEntry e3 = iter2.next();

            byte[] keyBytes = (keyPre + rand2.nextLong()).getBytes();
            byte[] expect1 = (valPre + rand2.nextLong()).getBytes();
            byte[] expect2 = (valPre + rand2.nextLong()).getBytes();
            byte[] expect3 = (valPre + rand2.nextLong()).getBytes();

            byte[] actual1 = e1.getValue();
            byte[] actual2 = e2.getValue();
            byte[] actual3 = e3.getValue();

            Assert.assertEquals(e1.getKey(), keyBytes);
            Assert.assertEquals(actual1, expect1);

            Assert.assertEquals(e2.getKey(), keyBytes);
            Assert.assertEquals(actual2, expect2);

            Assert.assertEquals(e3.getKey(), keyBytes);
            Assert.assertEquals(actual3, expect3);

            count += 1;
        }
    }

    @Test
    public void testEmptyHashFile() throws Exception {
        File tmp = File.createTempFile("hhhhhh", "ff");
        tmp.deleteOnExit();

        HashFile2Builder hashWrite = new HashFile2Builder(
                tmp.getAbsolutePath(), 10);
        hashWrite.finish();

        for (HashEntry _entry : HashFile2.elements(tmp.getAbsolutePath())) {
            Assert.assertTrue(false, "hashfile should have no entries");
        }
    }
}
