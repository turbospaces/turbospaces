package com.turbospaces.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.samples.jpetstore.domain.Account;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.cache.Cache;
import com.turbospaces.core.PerformanceMonitor;

@SuppressWarnings("javadoc")
public class GuavaOffHeapCacheCombinedPerformanceTest extends AbstractBenchmark {
    Cache<String, Account> cache;
    PerformanceMonitor<Account> performanceMonitor;

    @Before
    public void before() {
        PetStoreModelProvider petStoreModelProvider = new PetStoreModelProvider();
        cache = new GuavaOffHeapCacheBuilder<String, Account>().build( Account.class );
        performanceMonitor = petStoreModelProvider.guavaMonitor( cache );
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
