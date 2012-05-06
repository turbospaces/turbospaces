package com.turbospaces.api;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;

import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.spaces.OffHeapJSpace;

@SuppressWarnings("javadoc")
public class SpaceErrorsTest {
    static final long MAX_TIMEOUT = Long.MAX_VALUE;
    static final int MAX_RESULTS = Integer.MAX_VALUE;

    static SpaceConfiguration configuration;
    static OffHeapJSpace offHeapJavaSpace;

    @BeforeClass
    public static void before()
                               throws Exception {
        configuration = TestEntity1.configurationFor();
        offHeapJavaSpace = new OffHeapJSpace( configuration );
    }

    @AfterClass
    public static void after()
                              throws Exception {
        configuration.destroy();
    }

    @Test(expected = IllegalArgumentException.class)
    public void exceptionOnNegativeTimeoutOnFetch() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.fetch( template, -1, MAX_RESULTS, JSpace.READ_ONLY );
    }

    @Test(expected = IllegalArgumentException.class)
    public void exceptionOnNegativeTimeoutOnWrite() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.write( template, 0, -1, JSpace.WRITE_OR_UPDATE );
    }

    @Test(expected = IllegalArgumentException.class)
    public void exceptionOnNegativeTtlOnWrite() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.write( template, -1, 10, JSpace.WRITE_OR_UPDATE );
    }

    @Test(expected = IllegalArgumentException.class)
    public void exceptionOnNonPositiveMaxResults() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.fetch( template, 0, 0, JSpace.READ_ONLY );
    }

    @Test(expected = InvalidDataAccessApiUsageException.class)
    public void exceptionOnMatchByIdAndNonProvidedId() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.fetch( template, MAX_TIMEOUT, MAX_RESULTS, JSpace.READ_ONLY | JSpace.MATCH_BY_ID );
    }

    @Test(expected = InvalidDataAccessResourceUsageException.class)
    public void exceptionOnTakeAndReadOnFetch() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.fetch( template, MAX_TIMEOUT, MAX_RESULTS, JSpace.READ_ONLY | JSpace.TAKE_ONLY );
    }

    @Test(expected = InvalidDataAccessResourceUsageException.class)
    public void exceptionOnReadAndEvictOnFetch() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.fetch( template, MAX_TIMEOUT, MAX_RESULTS, JSpace.READ_ONLY | JSpace.EVICT_ONLY );
    }

    @Test(expected = InvalidDataAccessResourceUsageException.class)
    public void exceptionOnTakeAndExclusiveReadOnFetch() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.fetch( template, MAX_TIMEOUT, MAX_RESULTS, JSpace.TAKE_ONLY | JSpace.EXCLUSIVE_READ_LOCK );
    }

    @Test(expected = InvalidDataAccessResourceUsageException.class)
    public void exceptionOnTakeAndEvictOnFetch() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.fetch( template, MAX_TIMEOUT, MAX_RESULTS, JSpace.TAKE_ONLY | JSpace.EVICT_ONLY );
    }

    @Test(expected = InvalidDataAccessResourceUsageException.class)
    public void exceptionOnReadAndEvictOnlyOnFetch() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.fetch( template, MAX_TIMEOUT, MAX_RESULTS, JSpace.READ_ONLY | JSpace.EVICT_ONLY );
    }

    @Test(expected = InvalidDataAccessResourceUsageException.class)
    public void exceptionOnEvictOnlyAndExclusiveReadOnFetch() {
        TestEntity1 template = new TestEntity1();
        offHeapJavaSpace.fetch( template, MAX_TIMEOUT, MAX_RESULTS, JSpace.EXCLUSIVE_READ_LOCK | JSpace.EVICT_ONLY );
    }

    @Test(expected = InvalidDataAccessResourceUsageException.class)
    public void exceptionOnWriteOnlyAndUpdateOnlyOnWrite() {
        TestEntity1 template = new TestEntity1();
        template.afterPropertiesSet();
        offHeapJavaSpace.write( template, Long.MAX_VALUE, MAX_TIMEOUT, JSpace.WRITE_ONLY | JSpace.UPDATE_ONLY );
    }

    @Test(expected = InvalidDataAccessResourceUsageException.class)
    public void exceptionOnWriteOrUpdateOnlyAndUpdateOnlyOnWrite() {
        TestEntity1 template = new TestEntity1();
        template.afterPropertiesSet();
        offHeapJavaSpace.write( template, Long.MAX_VALUE, MAX_TIMEOUT, JSpace.WRITE_OR_UPDATE | JSpace.UPDATE_ONLY );
    }

    @Test(expected = InvalidDataAccessResourceUsageException.class)
    public void exceptionOnWriteOrUpdateOnlyAndWriteOnlyOnWrite() {
        TestEntity1 template = new TestEntity1();
        template.afterPropertiesSet();
        offHeapJavaSpace.write( template, Long.MAX_VALUE, MAX_TIMEOUT, JSpace.WRITE_OR_UPDATE | JSpace.WRITE_ONLY );
    }
}
