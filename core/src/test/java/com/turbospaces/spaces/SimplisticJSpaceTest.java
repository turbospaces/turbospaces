package com.turbospaces.spaces;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.turbospaces.api.EmbeddedJSpaceRunnerTest;
import com.turbospaces.api.SpaceCapacityOverflowException;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.model.TestEntity1;

@SuppressWarnings("javadoc")
public class SimplisticJSpaceTest {
    SimplisticJSpace jSpace;
    SpaceConfiguration configuration;

    @Before
    public void before()
                        throws Exception {
        configuration = EmbeddedJSpaceRunnerTest.configurationFor();
        jSpace = new SimplisticJSpace( new OffHeapJSpace( configuration ) );
        jSpace.afterPropertiesSet();
    }

    @After
    public void after()
                       throws Exception {
        jSpace.mbUsed();
        jSpace.size();
        jSpace.toString();
        jSpace.destroy();

        configuration.destroy();
    }

    @Test(expected = SpaceCapacityOverflowException.class)
    public void canGetOverflowException() {
        long maxElements = configuration.getCapacityRestriction().getMaxElements();
        try {
            configuration.getCapacityRestriction().setMaxElements( 1 );
            TestEntity1 entity1 = new TestEntity1();
            TestEntity1 entity2 = new TestEntity1();
            entity1.afterPropertiesSet();
            entity2.afterPropertiesSet();
            jSpace.write( entity1, Integer.MAX_VALUE, 0 );
            jSpace.write( entity2, Integer.MAX_VALUE, 0 );
            Assert.fail();
        }
        finally {
            configuration.getCapacityRestriction().setMaxElements( maxElements );
        }
    }

    @Test
    public void canWriteOrUpdateWithProvidedTTLAndTimeout() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        jSpace.write( entity1, Integer.MAX_VALUE, 0 );

        jSpace.readByID( entity1.getUniqueIdentifier(), entity1.getClass() ).get().assertMatch( entity1 );
    }

    @Test
    public void canWriteOrUpdateWithProvidedTimeout() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        jSpace.write( entity1, 0 );

        jSpace.readExclusivelyByID( entity1.getUniqueIdentifier(), entity1.getClass(), 0 ).get().assertMatch( entity1 );
    }

    @Test
    public void canWriteOrUpdateWithJustEntity() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        jSpace.write( entity1 );

        jSpace.readByID( entity1.getUniqueIdentifier(), entity1.getClass(), 0 ).get().assertMatch( entity1 );
    }

    @Test
    public void canWriteAndTakeById() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        jSpace.write( entity1 );

        jSpace.takeByID( entity1.getUniqueIdentifier(), entity1.getClass() ).get().assertMatch( entity1 );
    }

    @Test
    public void canWriteAndTakeByIdWithTimeout() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        jSpace.write( entity1 );

        jSpace.takeByID( entity1.getUniqueIdentifier(), entity1.getClass(), 0 ).get().assertMatch( entity1 );
    }

    @Test
    public void canWriteAndEvictById() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        jSpace.write( entity1 );

        jSpace.evictByID( entity1.getUniqueIdentifier(), entity1.getClass() ).get().assertMatch( entity1 );
    }

    @Test
    public void canWriteAndEvictByIdWithTimeout() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        jSpace.write( entity1 );

        jSpace.evictByID( entity1.getUniqueIdentifier(), entity1.getClass(), 0 ).get().assertMatch( entity1 );
    }

    @Test
    public void cantDeleteById()
                                throws InterruptedException {
        final TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        jSpace.write( entity1 );

        Thread t = new Thread() {

            @Override
            public void run() {
                assertThat( jSpace.takeByID( entity1.getUniqueIdentifier(), TestEntity1.class ), is( notNullValue() ) );
            }
        };
        t.start();
        t.join();
    }
}
