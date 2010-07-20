package com.g414.hash.impl;

import static com.sun.faban.driver.CycleType.THINKTIME;
import static com.sun.faban.driver.Timing.MANUAL;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.g414.hash.LongHash;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.sun.faban.driver.BenchmarkDefinition;
import com.sun.faban.driver.BenchmarkDriver;
import com.sun.faban.driver.BenchmarkOperation;
import com.sun.faban.driver.DriverContext;
import com.sun.faban.driver.FlatMix;
import com.sun.faban.driver.NegativeExponential;
import com.sun.faban.driver.engine.GuiceContext;

@BenchmarkDefinition(name = "SampleDriver", version = "1.0")
@BenchmarkDriver(name = "SampleDriver", responseTimeUnit = TimeUnit.MICROSECONDS)
@FlatMix(operations = { "Foo" }, mix = { 1.0 }, deviation = 1.0)
public class HashDriver {
    @Inject
    private LongHash hash;
    private Random random = new Random(0L);

    @BenchmarkOperation(name = "Foo", max90th = 1000000, timing = MANUAL)
    @NegativeExponential(cycleType = THINKTIME, cycleMean = 0, cycleDeviation = 0.0)
    public void doFoo() {
        byte[] value = new byte[128];
        random.nextBytes(value);

        DriverContext.getContext().recordTime();

        for (int i = 0; i < 10000; i++) {
            hash.getLongHashCode(value);
        }

        DriverContext.getContext().recordTime();
    }

    public static class GuiceModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(Key.get(Object.class, BenchmarkDriver.class)).to(
                    HashDriver.class);
            try {
                bind(LongHash.class).to(
                        (Class<LongHash>) Class.forName(GuiceContext
                                .getNamedProperty("longhash")));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
