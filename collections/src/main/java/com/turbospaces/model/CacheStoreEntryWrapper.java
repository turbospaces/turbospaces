/**
 * Copyright (C) 2011-2012 Andrey Borisov <aandrey.borisov@gmail.com>
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
package com.turbospaces.model;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Preconditions;

/**
 * wrapper for writing-reading off-heap entities.
 * 
 * @since 0.1
 */
public final class CacheStoreEntryWrapper {
    /**
     * meta information about {@link #bean}'s class.
     */
    private BO persistentEntity;
    /**
     * id of the entity(can be derived from spring's data abstraction or passed directly).
     */
    private Object id;
    /**
     * optimistic lock version(optional).
     */
    private Integer version;
    /**
     * routing field for partitioned jspace(this is something that is not needed for local JVM caches).
     */
    private Object routing;
    /**
     * actual entity.
     */
    private Object bean;
    /**
     * array of property values extracted from {@link #bean}.
     */
    private Object[] propertyValues;
    /**
     * de-serialized form of {@link #bean}.
     */
    private byte[] beanAsBytes;

    /**
     * create new cache store instance for the given persistent class over actual bean.
     * 
     * @param persistentEntity
     *            class meta information placeholder
     * @param bean
     *            actual bean
     * @return new instance
     */
    public static CacheStoreEntryWrapper writeValueOf(final BO persistentEntity,
                                                      final Object bean) {
        CacheStoreEntryWrapper wrapper = new CacheStoreEntryWrapper();
        wrapper.persistentEntity = Preconditions.checkNotNull( persistentEntity );
        wrapper.bean = Preconditions.checkNotNull( bean );
        wrapper.persistentEntity.fillIdVersionRouting( wrapper );
        return wrapper;
    }

    /**
     * create new cache store instance for given persistent class and assign entity's key explicitly.
     * 
     * @param persistentEntity
     *            class meta information placeholder
     * @param id
     *            entity's key
     * @return new instance
     */
    public static CacheStoreEntryWrapper readByIdValueOf(final BO persistentEntity,
                                                         final Object id) {
        CacheStoreEntryWrapper wrapper = new CacheStoreEntryWrapper();
        wrapper.persistentEntity = Preconditions.checkNotNull( persistentEntity );
        wrapper.setId( id );
        return wrapper;
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
     *            bean's primary key
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
     * @return routing field value (if such routing field explicitly defined via {@code Routing} annotation otherwise
     *         id's
     *         field value)
     */
    public Object getRoutingOrId() {
        return routing != null ? routing : id;
    }

    /**
     * set the routing field's value
     * 
     * @param routing
     *            routing value
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
     *            byte array
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
     * get the {@link #getBean()} in serialized form. </p>
     * 
     * For remote communications remote entity already comes in
     * serialized form, so there is no need to do serialization job twice. </p>
     * 
     * @param objectBuffer
     *            object buffer for reading/writing entities to-from byte array
     * @return serialized version of {@link #getBean()}
     */
    public byte[] asSerializedData(final ObjectBuffer objectBuffer) {
        if ( beanAsBytes == null )
            beanAsBytes = objectBuffer.writeObjectData( this );
        return beanAsBytes;
    }

    /**
     * get the {@link #getBean()} as property values array.</p>
     * 
     * @return entity in form or property values array
     */
    public Object[] asPropertyValuesArray() {
        if ( propertyValues == null )
            propertyValues = persistentEntity.getBulkPropertyValues( this );
        return propertyValues;
    }

    private CacheStoreEntryWrapper() {}
}
