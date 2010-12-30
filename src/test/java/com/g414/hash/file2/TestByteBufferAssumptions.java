package com.g414.hash.file2;

import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.testng.annotations.Test;

@Test
public class TestByteBufferAssumptions {
    public void testByteBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(24);

        buf.putLong(0, 0xCAFECAFECAFECAFEL);
        buf.putLong(2, 0x0102030405060708L);
        printBuf(buf);
        assertBuf(buf);
    }

    private static void assertBuf(ByteBuffer buf) {
        buf.rewind();
        Assert.assertEquals(buf.getLong(), 0xcafe010203040506L);
        Assert.assertEquals(buf.getLong(), 0x0708000000000000L);
    }

    private static void printBuf(ByteBuffer buf) {
        buf.rewind();
        StringBuilder b = new StringBuilder();

        for (int i = 0; i < buf.capacity(); i += 8) {
            b.append(Long.toHexString(buf.getLong()));
            b.append(" ");
        }

        System.out.println(b.toString());
    }
}
