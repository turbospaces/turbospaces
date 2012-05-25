package com.turbospaces.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.samples.jpetstore.domain.Account;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.turbospaces.core.PerformanceMonitor;

@SuppressWarnings("javadoc")
public class GuavaCacheCombinedPerformanceTest extends AbstractBenchmark {
    Cache<String, Account> cache;
    PerformanceMonitor<Account> performanceMonitor;

    @Before
    public void before() {
        PetStoreModelProvider petStoreModelProvider = new PetStoreModelProvider();
        cache = CacheBuilder.newBuilder().build();
        performanceMonitor = petStoreModelProvider.guavaMonitor( cache, false );
    }

    @After
    public void after() {
        cache.invalidateAll();
    }

    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 5)
    @Test
    public void run() {
        performanceMonitor.run();
    }
}
