/**
 * Copyright (C) 2011 Andrey Borisov <aandrey.borisov@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.turbospaces.spaces;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Routing;
import org.springframework.data.mapping.model.BeanWrapper;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.SpaceErrors;
import com.turbospaces.model.BO;
import com.turbospaces.pool.ObjectFactory;
import com.turbospaces.pool.ObjectPool;
import com.turbospaces.pool.SimpleObjectPool;

/**
 * this is low-level cache store entity designed to be used with {@link SpaceStore} interface directly.
 * 
 * @since 0.1
 */
@SuppressWarnings("rawtypes")
public final class CacheStoreEntryWrapper implements SpaceErrors {
    private static final ObjectPool<CacheStoreEntryWrapper> OBJECT_POOL;

    static {
        OBJECT_POOL = new SimpleObjectPool<CacheStoreEntryWrapper>( new ObjectFactory<CacheStoreEntryWrapper>() {

            @Override
            public CacheStoreEntryWrapper newInstance() {
                return new CacheStoreEntryWrapper();
            }

            @Override
            public void invalidate(final CacheStoreEntryWrapper obj) {
                obj.persistentEntity = null;
                obj.id = null;
                obj.version = null;
                obj.routing = null;
                obj.bean = null;
                obj.propertyValues = null;
                obj.beanAsBytes = null;
                obj.beanWrapper = null;
                obj.configuration = null;
            }
        } );
    }

    private BO persistentEntity;
    private Object id;
    private Integer version;
    private Object routing;
    private Object bean;
    private Object[] propertyValues;
    private byte[] beanAsBytes;
    private BeanWrapper beanWrapper;
    private AbstractSpaceConfiguration configuration;

    private CacheStoreEntryWrapper() {}

    /**
     * get pooled cache store instance for the given persistent class over actual JSpace bean and associate space
     * configuration with bean.
     * 
     * @param persistentEntity
     * @param bean
     * @param configuration
     * @return pooled instance
     */
    public static CacheStoreEntryWrapper valueOf(final BO persistentEntity,
                                                 final AbstractSpaceConfiguration configuration,
                                                 final Object bean) {
        CacheStoreEntryWrapper borrowObject = OBJECT_POOL.borrowObject();

        borrowObject.configuration = configuration;
        borrowObject.bean = bean;
        borrowObject.persistentEntity = persistentEntity;
        borrowObject.persistentEntity.fillIdVersionRouting( borrowObject );

        return borrowObject;
    }

    /**
     * get pooled cache store instance for given persistent class with provided unique identifier.
     * 
     * @param persistentEntity
     * @param id
     * @return pooled object
     */
    public static CacheStoreEntryWrapper valueOf(final BO persistentEntity,
                                                 final Object id) {
        CacheStoreEntryWrapper borrowObject = OBJECT_POOL.borrowObject();

        borrowObject.persistentEntity = persistentEntity;
        borrowObject.setId( id );

        return borrowObject;
    }

    /**
     * return pool instance to the object's pool
     * 
     * @param cacheStoreEntryWrapper
     */
    public static void recycle(final CacheStoreEntryWrapper cacheStoreEntryWrapper) {
        OBJECT_POOL.returnObject( cacheStoreEntryWrapper );
    }

    /**
     * @return persistent entity meta-data(class information) associated with this cache store entity.
     */
    public BO getPersistentEntity() {
        return persistentEntity;
    }

    /**
     * @return optimistic lock version of the entity.
     */
    public Integer getOptimisticLockVersion() {
        return version;
    }

    /**
     * set unique identifier(id) field
     * 
     * @param version
     *            the version to set
     */
    public void setOptimisticLockVersion(final Integer version) {
        this.version = version;
    }

    /**
     * @return unique identifier of the entity wrapped into unicity object wrapper.
     */
    public Object getId() {
        return id;
    }

    /**
     * set the primary key field value
     * 
     * @param id
     */
    public void setId(final Object id) {
        this.id = id;
    }

    /**
     * @return routing field value.
     */
    public Object getRouting() {
        return routing;
    }

    /**
     * @return routing field value (if such routing field explicitly defined via {@link Routing} annotation or
     *         {@link Id}'s field value)
     */
    public Object getRoutingOrId() {
        return routing != null ? routing : id;
    }

    /**
     * set the routing field
     * 
     * @param routing
     */
    public void setRouting(final Object routing) {
        this.routing = routing;
    }

    /**
     * for remote communications there is no need to do serialization/de-serialization twice, so if serialization is
     * done on client side and uses the same kryo configuration, there is nothing do on server except accept byte array
     * as is.
     * 
     * @param beanAsBytes
     */
    public void setBeanAsBytes(final byte[] beanAsBytes) {
        this.beanAsBytes = beanAsBytes;
    }

    /**
     * @return actual bean
     */
    public Object getBean() {
        return bean;
    }

    /**
     * get bean wrapper associated with this entity with create on demand behavior
     * 
     * @return the beanWrapper
     */
    public BeanWrapper getBeanWrapper() {
        if ( beanWrapper == null )
            beanWrapper = BeanWrapper.create( bean, configuration.getConversionService() );
        return beanWrapper;
    }

    /**
     * get the {@link #getBean()} in serialized form. </p>
     * 
     * For remote communications remote entity already comes in
     * serialized form, so there is no need to do serialization job twice. </p>
     * 
     * @param objectBuffer
     * @return serialized version of {@link #getBean()}
     */
    public byte[] asSerializedData(final ObjectBuffer objectBuffer) {
        if ( beanAsBytes == null )
            beanAsBytes = configuration.getEntitySerializer().serialize( this, objectBuffer );
        return beanAsBytes;
    }

    /**
     * get the {@link #getBean()} in property values array. </p>
     * 
     * @return entity in form or property values array
     */
    public Object[] asPropertyValuesArray() {
        if ( propertyValues == null )
            propertyValues = persistentEntity.getBulkPropertyValues( this, configuration.getConversionService() );
        return propertyValues;
    }
}
