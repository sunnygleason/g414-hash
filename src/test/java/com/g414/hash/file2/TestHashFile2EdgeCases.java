package com.g414.hash.file2;

import java.io.File;
import java.util.Iterator;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.hash.impl.MurmurHash;

@Test
public class TestHashFile2EdgeCases {
    MurmurHash hash = new MurmurHash();

    public void testSingleEntryMultiGet() throws Exception {
        File tmp = File.createTempFile("hhhhhh", "ff");
        tmp.deleteOnExit();

        HashFile2Builder hashWrite = new HashFile2Builder(tmp.getAbsolutePath(), 1000);

        byte[] key = new byte[] { 1, 2, 3, 4, 5 };
        byte[] value = new byte[] { 5, 4, 3, 2, 1 };
        hashWrite.add(key, value);

        hashWrite.finish();

        HashFile2 file = new HashFile2(tmp.getAbsolutePath());

        Iterator<byte[]> iter = file.getMulti(key).iterator();

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(iter.next(), value);
        Assert.assertFalse(iter.hasNext());
    }
}
