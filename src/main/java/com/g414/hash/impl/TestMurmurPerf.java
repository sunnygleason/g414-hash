package com.g414.hash.impl;

import com.g414.hash.LongHash;

/**
 * Silly micro-benchmark for testing relative speeds of Murmur hash implementations.
 */
public class TestMurmurPerf {
    public static void main(String[] args) throws Exception
    {
        LongHash h1 = new MurmurHash_new();
        LongHash h2 = new MurmurHash_v9ABA();

        int round = 0;
        final byte[] TEST_DATA = "This is some funky String we will be used for hashing to see if performance differs".getBytes("UTF-8");
        
        while (true) {
            long start = System.currentTimeMillis();

            LongHash h;
            
            if ((++round & 1) == 0) {
                h = h1;
            } else {
                h = h2;
            }

            int total = 0;
            for (int i = 150000; --i >= 0; ){
                total += (int) h.getLongHashCode(TEST_DATA);
            }
            long time = System.currentTimeMillis() - start;
            System.out.println("Time for "+h.getClass().getName()+": "+time);
            Thread.sleep(100L);
        }
    }

}
