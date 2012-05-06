package com.elsecloud.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Routing;
import org.springframework.data.annotation.Version;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;

import com.elsecloud.api.SpaceConfiguration;
import com.elsecloud.spaces.CacheStoreEntryWrapper;

@SuppressWarnings({ "javadoc", "rawtypes" })
public class ClassMetaDataInformationTest {
    Logger logger = LoggerFactory.getLogger( getClass() );

    SpaceConfiguration configuration;
    BO bo;

    @SuppressWarnings("unchecked")
    @Before
    public void beforeClass()
                             throws Exception {
        configuration = TestEntity1.configurationFor();

        BasicMongoPersistentEntity<TestEntity1> entity = (BasicMongoPersistentEntity<TestEntity1>) configuration
                .getMappingContext()
                .getPersistentEntity( TestEntity1.class );
        bo = new BO<TestEntity1, MongoPersistentProperty>( entity );
    }

    @After
    public void after()
                       throws Exception {
        configuration.destroy();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void verifyTypeInformation() {
        bo.verify();
        bo.setIdProperty( bo.getIdProperty() );
        MongoPersistentProperty idProperty = (MongoPersistentProperty) bo.getIdProperty();
        MongoPersistentProperty versionProperty = (MongoPersistentProperty) bo.getOptimisticLockVersionProperty();
        MongoPersistentProperty routingProperty = (MongoPersistentProperty) bo.getRoutingProperty();
        PreferredConstructor<TestEntity1> constructor = bo.getPreferredConstructor();
        Class<TestEntity1> type = bo.getType();
        String name = bo.getName();
        Object typeAlias = bo.getTypeAlias();
        TypeInformation<TestEntity1> information = bo.getTypeInformation();

        assertThat( idProperty, is( notNullValue() ) );
        assertThat( information, is( notNullValue() ) );
        assertThat( versionProperty, is( notNullValue() ) );
        assertThat( routingProperty, is( notNullValue() ) );
        assertThat( constructor, is( notNullValue() ) );
        assertThat( constructor, is( notNullValue() ) );
        assertThat( type.getName(), is( TestEntity1.class.getName() ) );
        assertThat( typeAlias, is( nullValue() ) );
        assertThat( bo.getPersistentProperty( "s1" ), is( notNullValue() ) );
        assertThat( bo.getPersistentProperty( "s2" ), is( notNullValue() ) );
        assertThat( bo.getPersistentProperty( "s3" ), is( notNullValue() ) );
        assertThat( bo.getPersistentProperty( "s4" ), is( notNullValue() ) );

        logger.info( "Entity = {}", name );
        logger.info( "Type = {}", type );
        logger.info( "TypeInformation = {}", information );
        logger.info( "IdProperty= {}", idProperty.getFieldName() );
        logger.info( "VersionProperty= {}", versionProperty.getFieldName() );
        logger.info( "RoutingProperty= {}", routingProperty.getFieldName() );
        logger.info( "PreferedConstructor = {}", constructor.getConstructor() );

        bo.doWithAssociations( new AssociationHandler<MongoPersistentProperty>() {

            @Override
            public void doWithAssociation(final Association<MongoPersistentProperty> association) {
                logger.info( "Association discovered = {}", association.toString() );
            }
        } );
    }

    @Test
    public void verifyCGlibOptimization() {
        assertThat( bo.getBrokenProperties().size(), is( 0 ) );

        TestEntity1 entity1 = (TestEntity1) bo.newInstance();
        entity1.afterPropertiesSet();
        entity1.setOptimisticLockVersion( Integer.valueOf( 2 ) );
        entity1.setUniqueIdentifier( "abc" );
        entity1.setRouting( "routing-123" );
        CacheStoreEntryWrapper cacheEntry1 = CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 );
        Object[] bulkPropertyValues = bo.getBulkPropertyValues( cacheEntry1, configuration.getConversionService() );

        assertThat( cacheEntry1.getPersistentEntity(), is( notNullValue() ) );
        assertThat( (String) cacheEntry1.getId(), is( "abc" ) );
        assertThat( cacheEntry1.getOptimisticLockVersion(), is( 2 ) );
        assertThat( (String) cacheEntry1.getRouting(), is( "routing-123" ) );
        assertThat( (String) bulkPropertyValues[0], is( "abc" ) );
        assertThat( (Integer) bulkPropertyValues[1], is( 2 ) );
        assertThat( (String) bulkPropertyValues[2], is( "routing-123" ) );

        TestEntity1 entity2 = (TestEntity1) bo.newInstance();
        bo.setBulkPropertyValues( entity2, bulkPropertyValues, configuration.getConversionService() );
        entity1.assertMatch( entity2 );
        entity2.assertMatch( entity1 );

        CacheStoreEntryWrapper cacheEntry2 = CacheStoreEntryWrapper.valueOf( bo, entity1.getUniqueIdentifier() );
        bulkPropertyValues = bo.getBulkPropertyValues( cacheEntry2, configuration.getConversionService() );
        assertThat( (String) bulkPropertyValues[0], is( "abc" ) );
        assertThat( bulkPropertyValues[1], is( nullValue() ) );
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void canSetAndGetValuesWithoutCGlibJustForFields()
                                                             throws Exception {
        SpaceConfiguration configuration = new SpaceConfiguration();
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setInitialEntitySet( Collections.singleton( TestEntity2.class ) );
        mappingContext.afterPropertiesSet();
        configuration.setMappingContext( mappingContext );
        configuration.afterPropertiesSet();

        bo = new BO( (BasicPersistentEntity) configuration.getMappingContext().getPersistentEntity( TestEntity2.class ) );
        TestEntity2 entity1 = new TestEntity2();
        entity1.id = 213L;
        entity1.str1 = "str213";
        CacheStoreEntryWrapper cacheEntry1 = CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 );
        Object[] bulkPropertyValues = bo.getBulkPropertyValues( cacheEntry1, configuration.getConversionService() );
        assertThat( (Long) bulkPropertyValues[0], is( 213L ) );
        assertThat( (String) bulkPropertyValues[1], is( "str213" ) );
        TestEntity2 entity2 = (TestEntity2) bo.newInstance();
        bo.setBulkPropertyValues( entity2, bulkPropertyValues, configuration.getConversionService() );
        configuration.destroy();
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void canSetAndGetValuesWithoutCGlib()
                                                throws Exception {
        SpaceConfiguration configuration = new SpaceConfiguration();
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setInitialEntitySet( Collections.singleton( TestEntity3.class ) );
        mappingContext.afterPropertiesSet();
        configuration.setMappingContext( mappingContext );
        configuration.afterPropertiesSet();

        bo = new BO( (BasicPersistentEntity) configuration.getMappingContext().getPersistentEntity( TestEntity3.class ) );
        TestEntity3 entity1 = new TestEntity3();
        entity1.id = 213L;
        entity1.version = 1;
        entity1.routing = "routing-123";
        CacheStoreEntryWrapper cacheEntry1 = CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 );
        Object[] bulkPropertyValues = bo.getBulkPropertyValues( cacheEntry1, configuration.getConversionService() );
        assertThat( (Long) bulkPropertyValues[0], is( 213L ) );
        assertThat( (Integer) bulkPropertyValues[1], is( 1 ) );
        assertThat( (String) bulkPropertyValues[2], is( "routing-123" ) );
        TestEntity3 entity2 = (TestEntity3) bo.newInstance();
        bo.setBulkPropertyValues( entity2, bulkPropertyValues, configuration.getConversionService() );
        configuration.destroy();
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
