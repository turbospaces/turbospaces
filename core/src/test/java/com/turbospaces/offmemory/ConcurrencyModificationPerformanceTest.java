package com.turbospaces.offmemory;

import java.util.LinkedList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.spaces.OffHeapJSpace;
import com.turbospaces.spaces.SimplisticJSpace;

@SuppressWarnings("javadoc")
public class ConcurrencyModificationPerformanceTest {
    SimplisticJSpace jSpace;
    SpaceConfiguration configuration;

    @Before
    public void before()
                        throws Exception {
        configuration = TestEntity1.configurationFor();
        OffHeapJSpace space = new OffHeapJSpace( configuration );
        jSpace = new SimplisticJSpace( space );
    }

    @After
    public void after()
                       throws Exception {
        try {
            jSpace.destroy();
        }
        finally {
            configuration.destroy();
        }
    }

    @Test
    public void noConcurrencyIssues()
                                     throws InterruptedException {
        final TestEntity1 e1 = new TestEntity1();
        e1.afterPropertiesSet();

        LinkedList<Throwable> errors = SpaceUtility.repeatConcurrently(
                Runtime.getRuntime().availableProcessors(),
                1000000,
                new Function<Integer, Object>() {

                    @Override
                    public Object apply(final Integer iteration) {
                        if ( iteration % 2 == 0 )
                            jSpace.write( e1 );
                        else
                            jSpace.takeByID( e1.getUniqueIdentifier(), TestEntity1.class );

                        return this;
                    }
                } );

        if ( !errors.isEmpty() )
            throw new AssertionError( "unexpected errors = " + errors.toString() );
    }
}
