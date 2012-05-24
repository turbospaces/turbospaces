package com.turbospaces.offmemory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.turbospaces.api.EmbeddedJSpaceRunnerTest;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.PerformanceMonitor;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.spaces.OffHeapJSpace;
import com.turbospaces.spaces.SimplisticJSpace;

@SuppressWarnings("javadoc")
public class CombinedPerformanceTest {
    PerformanceMonitor<TestEntity1> monitor;
    SpaceConfiguration configuration;
    SimplisticJSpace space;

    @Before
    public void before()
                        throws Exception {
        configuration = EmbeddedJSpaceRunnerTest.configurationFor();
        space = new SimplisticJSpace( new OffHeapJSpace( configuration ) );
        space.afterPropertiesSet();
        monitor = new PerformanceMonitor<TestEntity1>( new Function<String, TestEntity1>() {
            @Override
            public TestEntity1 apply(final String input) {
                TestEntity1 entity1 = new TestEntity1();
                entity1.afterPropertiesSet( input );
                space.write( entity1 );
                return entity1;
            }
        }, new Function<String, TestEntity1>() {
            @Override
            public TestEntity1 apply(final String input) {
                return space.readByID( input, TestEntity1.class ).orNull();
            }
        }, new Function<String, TestEntity1>() {
            @Override
            public TestEntity1 apply(final String input) {
                return space.takeByID( input, TestEntity1.class ).orNull();
            }
        } );
        monitor.withNumberOfIterations( 10 * 1000000 );
    }

    @After
    public void after()
                       throws Exception {
        try {
            space.destroy();
        }
        finally {
            configuration.destroy();
        }
    }

    @Test
    public void run() {
        monitor.run();
    }
}
