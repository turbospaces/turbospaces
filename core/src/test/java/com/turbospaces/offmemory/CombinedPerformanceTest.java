package com.turbospaces.offmemory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.PerformanceMonitor;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.spaces.OffHeapJSpace;

@SuppressWarnings("javadoc")
public class CombinedPerformanceTest {
    PerformanceMonitor monitor;
    PerformanceMonitor.FastObjectFactory objectFactory = new PerformanceMonitor.FastObjectFactory() {

        @Override
        public Object newInstance() {
            TestEntity1 entity1 = new TestEntity1();
            entity1.afterPropertiesSet();
            return entity1;
        }

        @Override
        public void invalidate(final Object obj) {}

        @Override
        public Object setId(final Object target,
                            final Object id) {
            ( (TestEntity1) target ).uniqueIdentifier = (String) id;
            return target;
        }
    };

    SpaceConfiguration configuration;
    OffHeapJSpace space;

    @Before
    public void before()
                        throws Exception {
        configuration = TestEntity1.configurationFor();
        space = new OffHeapJSpace( configuration );
        monitor = new PerformanceMonitor( space, objectFactory ).applyDefaultSettings();
        monitor.withThreadsCount( Runtime.getRuntime().availableProcessors() );
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
