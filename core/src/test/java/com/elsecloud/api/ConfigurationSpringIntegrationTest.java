package com.elsecloud.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.elsecloud.model.TestEntity1;
import com.elsecloud.spaces.OffHeapJSpace;
import com.elsecloud.spaces.SimplisticJSpace;
import com.elsecloud.spaces.tx.SpaceTransactionManager;
import com.google.common.base.Optional;

@SuppressWarnings("javadoc")
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:META-INF/spring/jspace-test-context.xml" })
@TransactionConfiguration(defaultRollback = false)
@Transactional
public class ConfigurationSpringIntegrationTest {
    @Autowired
    private SpaceConfiguration configuration;
    @Autowired
    private SimplisticJSpace simplisticJSpace;
    @Autowired
    private OffHeapJSpace offHeapJavaSpace;
    @Autowired
    private SpaceTransactionManager transactionManager;

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    @Test
    public void configured() {
        assertThat( configuration.getConversionService(), is( notNullValue() ) );
        assertThat( configuration.getMappingContext(), is( notNullValue() ) );
        assertThat( configuration.getKryo(), is( notNullValue() ) );

        assertThat( simplisticJSpace, is( notNullValue() ) );
        assertThat( offHeapJavaSpace, is( notNullValue() ) );

        assertThat( simplisticJSpace.getSpaceConfiguration(), is( notNullValue() ) );
        assertThat( offHeapJavaSpace.getSpaceConfiguration(), is( notNullValue() ) );

        assertThat( (SimplisticJSpace) transactionManager.getResourceFactory(), is( simplisticJSpace ) );
    }

    @Test
    public void canDoSomethingUnderTransaction() {
        TestEntity1 entity1 = new TestEntity1();
        TestEntity1 entity2 = new TestEntity1();

        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();

        simplisticJSpace.write( entity1 );
        simplisticJSpace.write( entity2 );
    }

    @Test
    @Rollback
    public void canDoSomethingAndRollback() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        simplisticJSpace.write( entity1 );
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    @SuppressWarnings("unchecked")
    public void canDoSomeManipulationWithTransactionTemplate() {
        TransactionTemplate template = new TransactionTemplate( transactionManager );
        template.afterPropertiesSet();

        TestEntity1 entity1 = template.execute( new TransactionCallback<TestEntity1>() {

            @Override
            public TestEntity1 doInTransaction(final TransactionStatus status) {
                TestEntity1 obj = new TestEntity1();
                obj.afterPropertiesSet();
                simplisticJSpace.write( obj );

                return obj;
            }
        } );

        Optional<TestEntity1> clone = (Optional<TestEntity1>) simplisticJSpace.readByID( entity1.getUniqueIdentifier(), entity1.getClass() );
        clone.get().assertMatch( entity1 );

        TestEntity1 entity2 = template.execute( new TransactionCallback<TestEntity1>() {

            @Override
            public TestEntity1 doInTransaction(final TransactionStatus status) {
                TestEntity1 obj = new TestEntity1();
                obj.afterPropertiesSet();
                simplisticJSpace.write( obj );

                status.setRollbackOnly();

                return obj;
            }
        } );

        assertThat( read( entity2.getUniqueIdentifier() ), is( nullValue() ) );
    }

    @Test
    @Transactional(isolation = Isolation.DEFAULT, readOnly = false, propagation = Propagation.REQUIRED)
    public void canSuspendAndResumeTransaction() {
        TestEntity1 obj1 = new TestEntity1();
        obj1.afterPropertiesSet();
        simplisticJSpace.write( obj1 );

        TestEntity1 obj2 = someTransactionMethod();

        read( obj1.getUniqueIdentifier() ).assertMatch( obj1 );
        read( obj2.getUniqueIdentifier() ).assertMatch( obj2 );
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public TestEntity1 someTransactionMethod() {
        TestEntity1 obj = new TestEntity1();
        obj.afterPropertiesSet();
        simplisticJSpace.write( obj );
        return obj;
    }

    @Transactional(readOnly = true, propagation = Propagation.NOT_SUPPORTED)
    public TestEntity1 read(final Object id) {
        return simplisticJSpace.readByID( id, TestEntity1.class ).get();
    }
}
