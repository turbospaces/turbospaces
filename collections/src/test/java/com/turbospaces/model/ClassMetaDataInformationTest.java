package com.turbospaces.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Routing;
import org.springframework.data.annotation.Version;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;

import com.esotericsoftware.minlog.Log;
import com.turbospaces.serialization.DecoratedKryo;

@SuppressWarnings({ "javadoc", "rawtypes" })
public class ClassMetaDataInformationTest {
    BO bo;
    DecoratedKryo kryo;

    @Before
    public void beforeClass() {
        kryo = new DecoratedKryo();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void verifyTypeInformation()
                                       throws SecurityException,
                                       NoSuchMethodException {
        bo = TestEntity1.getPersistentEntity();
        bo.getOriginalPersistentEntity().verify();
        PersistentProperty idProperty = bo.getIdProperty();
        PersistentProperty versionProperty = bo.getOptimisticLockVersionProperty();
        PersistentProperty routingProperty = bo.getRoutingProperty();
        Class<TestEntity1> type = bo.getOriginalPersistentEntity().getType();
        String name = bo.getOriginalPersistentEntity().getName();
        Object typeAlias = bo.getOriginalPersistentEntity().getTypeAlias();
        TypeInformation<TestEntity1> information = bo.getOriginalPersistentEntity().getTypeInformation();

        assertThat( idProperty, is( notNullValue() ) );
        assertThat( information, is( notNullValue() ) );
        assertThat( versionProperty, is( notNullValue() ) );
        assertThat( routingProperty, is( notNullValue() ) );
        assertThat( type.getName(), is( TestEntity1.class.getName() ) );
        assertThat( typeAlias, is( nullValue() ) );
        assertThat( bo.getPersistentProperty( "s1" ), is( notNullValue() ) );
        assertThat( bo.getPersistentProperty( "s2" ), is( notNullValue() ) );
        assertThat( bo.getPersistentProperty( "s3" ), is( notNullValue() ) );
        assertThat( bo.getPersistentProperty( "s4" ), is( notNullValue() ) );

        Log.info( "Entity = " + name );
        Log.info( "Type = " + type );
        Log.info( "TypeInformation = " + information );
        Log.info( "IdProperty= " + idProperty.getName() );
        Log.info( "VersionProperty= " + versionProperty.getName() );
        Log.info( "RoutingProperty= " + routingProperty.getName() );

        bo.getOriginalPersistentEntity().doWithAssociations( new AssociationHandler() {
            @Override
            public void doWithAssociation(final Association association) {
                Log.info( "Association discovered =" + association.toString() );
            }
        } );
    }

    @Test
    public void verifyCGlibOptimization()
                                         throws SecurityException,
                                         NoSuchMethodException {
        bo = TestEntity1.getPersistentEntity();
        assertThat( bo.getBrokenProperties().size(), is( 0 ) );

        TestEntity1 entity1 = (TestEntity1) bo.newInstance();
        entity1.afterPropertiesSet();
        entity1.setOptimisticLockVersion( Integer.valueOf( 2 ) );
        entity1.setUniqueIdentifier( "abc" );
        entity1.setRouting( "routing-123" );
        CacheStoreEntryWrapper cacheEntry1 = CacheStoreEntryWrapper.writeValueOf( bo, entity1 );
        Object[] bulkPropertyValues = bo.getBulkPropertyValues( cacheEntry1 );

        assertThat( cacheEntry1.getPersistentEntity(), is( notNullValue() ) );
        assertThat( (String) cacheEntry1.getId(), is( "abc" ) );
        assertThat( cacheEntry1.getOptimisticLockVersion(), is( 2 ) );
        assertThat( (String) cacheEntry1.getRouting(), is( "routing-123" ) );
        assertThat( (String) bulkPropertyValues[0], is( "abc" ) );
        assertThat( (Integer) bulkPropertyValues[1], is( 2 ) );
        assertThat( (String) bulkPropertyValues[2], is( "routing-123" ) );

        TestEntity1 entity2 = (TestEntity1) bo.newInstance();
        bo.setBulkPropertyValues( entity2, bulkPropertyValues );
        entity1.assertMatch( entity2 );
        entity2.assertMatch( entity1 );

        CacheStoreEntryWrapper cacheEntry2 = CacheStoreEntryWrapper.readByIdValueOf( bo, entity1.getUniqueIdentifier() );
        bulkPropertyValues = bo.getBulkPropertyValues( cacheEntry2 );
        assertThat( (String) bulkPropertyValues[0], is( "abc" ) );
        assertThat( bulkPropertyValues[1], is( nullValue() ) );
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void canSetAndGetValuesWithoutCGlibJustForFields()
                                                             throws Exception {
        SimpleMappingContext mappingContext = new SimpleMappingContext();
        mappingContext.setInitialEntitySet( Collections.singleton( TestEntity2.class ) );
        mappingContext.afterPropertiesSet();
        bo = new BO( (BasicPersistentEntity) mappingContext.getPersistentEntity( TestEntity2.class ) );
        TestEntity2 entity1 = new TestEntity2();
        entity1.id = 213L;
        entity1.str1 = "str213";
        CacheStoreEntryWrapper cacheEntry1 = CacheStoreEntryWrapper.writeValueOf( bo, entity1 );
        Object[] bulkPropertyValues = bo.getBulkPropertyValues( cacheEntry1 );
        assertThat( (Long) bulkPropertyValues[0], is( 213L ) );
        assertThat( (String) bulkPropertyValues[1], is( "str213" ) );
        TestEntity2 entity2 = (TestEntity2) bo.newInstance();
        bo.setBulkPropertyValues( entity2, bulkPropertyValues );
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void canSetAndGetValuesWithoutCGlib()
                                                throws Exception {
        SimpleMappingContext mappingContext = new SimpleMappingContext();
        mappingContext.setInitialEntitySet( Collections.singleton( TestEntity3.class ) );
        mappingContext.afterPropertiesSet();
        bo = new BO( (BasicPersistentEntity) mappingContext.getPersistentEntity( TestEntity3.class ) );
        TestEntity3 entity1 = new TestEntity3();
        entity1.id = 213L;
        entity1.version = 1;
        entity1.routing = "routing-123";
        CacheStoreEntryWrapper cacheEntry1 = CacheStoreEntryWrapper.writeValueOf( bo, entity1 );
        Object[] bulkPropertyValues = bo.getBulkPropertyValues( cacheEntry1 );
        assertThat( (Long) bulkPropertyValues[0], is( 213L ) );
        assertThat( (Integer) bulkPropertyValues[1], is( 1 ) );
        assertThat( (String) bulkPropertyValues[2], is( "routing-123" ) );
        TestEntity3 entity2 = (TestEntity3) bo.newInstance();
        bo.setBulkPropertyValues( entity2, bulkPropertyValues );
    }

    @SuppressWarnings("unused")
    public static class TestEntity2 {
        @Id
        private Long id;
        private String str1;
    }

    @SuppressWarnings("unused")
    public static class TestEntity3 {
        @Id
        private Long id;
        @Version
        private Integer version;
        @Routing
        private String routing;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }
    }
}
