package com.g414.hash.file;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.hash.impl.MurmurHash;

public class TestHashFile {
    MurmurHash hash = new MurmurHash();

    @Test
    public void testEmptyHashFile() throws Exception {
        File tmp = File.createTempFile("hhhhhh", "ff");

        System.out.println(tmp.getAbsolutePath());

        HashFileBuilder hashWrite = new HashFileBuilder(tmp.getAbsolutePath(),
                100);
        hashWrite.finish();

        for (HashEntry entry : HashFile.elements(tmp.getAbsolutePath())) {
            // should be empty
            Assert.assertTrue(false);
        }

        tmp.delete();
    }

    @Test
    public void testTinyHashFile() throws Exception {
        doHashFileTest(1, 10000);
    }

    @Test
    public void testSmallHashFile() throws Exception {
        doHashFileTest(100, 10000);
    }

    @Test
    public void testMediumHashFile() throws Exception {
        doHashFileTest(1000000, 10000);
    }

    @Test
    public void testMediumHashFileMulti() throws Exception {
        doHashFileMultiTest(1000000, 1000);
    }

    @Test(groups = "slow")
    public void testLargeHashFile() throws Exception {
        doHashFileTest(100000000, 10000);
    }

    @Test(groups = "slow")
    public void testXLargeHashFile() throws Exception {
        doHashFileTest(250000000L, 10000);
    }

    @Test(groups = "slow")
    public void testXXLargeHashFile() throws Exception {
        doHashFileTest(10000000000L, 10000);
    }

    public void doHashFileTest(long size, long confidence) throws Exception {
        File tmp = File.createTempFile("hhhhhhh", "ff");

        System.out.println(tmp.getAbsolutePath());
        System.out.println(new Date() + " writing...");

        HashFileBuilder hashWrite = new HashFileBuilder(tmp.getAbsolutePath(),
                size);

        for (long i = 0; i < size; i++) {
            byte[] k = Long.valueOf(i).toString().getBytes();
            byte[] v = Long.valueOf(-i).toString().getBytes();
            hashWrite.add(k, v);
            if (i % 1000000 == 0) {
                System.out.println(new Date() + " wrote: " + i);
            }
        }

        System.out.println(new Date() + " finishing...");
        hashWrite.finish();

        System.out.println(new Date() + " checking iter...");
        int found = 0;
        for (HashEntry entry : HashFile.elements(tmp.getAbsolutePath())) {
            byte[] k = entry.getKey();
            byte[] v = entry.getValue();
            long kk = Long.parseLong(new String(k));
            long kv = Long.parseLong(new String(v));
            Assert.assertTrue(kk == -kv);
            found += 1;
        }

        Assert.assertTrue(found == size);
        Random rand = new Random();

        System.out.println(new Date() + " checking find...");
        HashFile hRead = new HashFile(tmp.getAbsolutePath());
        for (long t = 0; t < confidence; t++) {
            int i = size < Integer.MAX_VALUE ? rand.nextInt((int) size)
                    : (int) (Math.abs(rand.nextLong()) % size);

            byte[] k = Integer.valueOf(i).toString().getBytes();
            byte[] v = Integer.valueOf(-i).toString().getBytes();
            long kk = Long.parseLong(new String(k));
            long kv = Long.parseLong(new String(v));

            byte[] f = hRead.get(k);
            if (f == null) {
                System.out.println("failed on " + i);
            }

            Assert.assertNotNull(f);

            long kf = Long.parseLong(new String(f));

            Assert.assertTrue(kv == kf);
            Assert.assertTrue(kk == -kf);
        }

        System.out.println(new Date() + " checking not found...");
        HashFile hRead2 = new HashFile(tmp.getAbsolutePath());
        for (long t = 0; t < confidence; t++) {
            int r = size < Integer.MAX_VALUE ? rand.nextInt((int) size)
                    : (int) (Math.abs(rand.nextLong()) % size);
            long i = size + r + 1;

            byte[] k = Long.valueOf(i).toString().getBytes();
            byte[] f = hRead2.get(k);
            Assert.assertNull(f);
        }
        System.out.println(new Date() + " done...");

        tmp.delete();
    }

    public void doHashFileMultiTest(long size, long confidence)
            throws Exception {
        File tmp = File.createTempFile("hhhhhmm", "ff");

        System.out.println(tmp.getAbsolutePath());
        System.out.println(new Date() + " writing...");

        HashFileBuilder hashWrite = new HashFileBuilder(tmp.getAbsolutePath(),
                size);

        for (long i = 0; i < size; i++) {
            byte[] k = Long.valueOf(i).toString().getBytes();
            byte[] v = Long.valueOf(-i).toString().getBytes();
            hashWrite.add(k, k);
            hashWrite.add(k, v);

            if (i % 1000000 == 0) {
                System.out.println(new Date() + " wrote: " + i);
            }
        }

        System.out.println(new Date() + " finishing...");
        hashWrite.finish();

        System.out.println(new Date() + " checking multi...");
        Random rand = new Random();

        HashFile hRead = new HashFile(tmp.getAbsolutePath());
        for (long t = 0; t < confidence; t++) {
            int i = size < Integer.MAX_VALUE ? rand.nextInt((int) size)
                    : (int) (Math.abs(rand.nextLong()) % size);

            byte[] k = Long.valueOf(i).toString().getBytes();
            byte[] v = Long.valueOf(-i).toString().getBytes();
            List<byte[]> f = hRead.getMulti(k);

            Assert.assertTrue(!f.isEmpty());
            Assert.assertEquals(f.size(), 2);
            Assert.assertEquals(f.get(0), k);
            Assert.assertEquals(f.get(1), v);
        }

        System.out.println(new Date() + " done...");
        hRead.close();
        tmp.delete();
    }
}
