package com.g414.hash.impl;

import com.g414.hash.LongHash;

/**
 * Silly micro-benchmark for testing relative speeds of Murmur hash implementations.
 */
public class TestLongHashPerf {
    final static LongHash[] hashes = new LongHash[] {
            new MurmurHash_new(),
            new MurmurHash_v9ABA(),
            new JenkinsHash_new(),
            new JenkinsHash_v9ABA()
    };

    public static void main(String[] args) throws Exception
    {

        int round = 0;
        
        // Performance is different for different lengths, due to optimization of tail (or lack thereof)
        //final byte[] TEST_DATA = "This is some funky String we use".getBytes("UTF-8");
        final byte[] TEST_DATA = "This is some funky String we will be used for hashing to see if performance differs".getBytes("UTF-8");
        
        while (true) {
            LongHash h = hashes[++round % hashes.length];
            long start = System.currentTimeMillis();
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
