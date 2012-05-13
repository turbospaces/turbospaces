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
package com.turbospaces.offmemory;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mapping.model.MutablePersistentEntity;

import com.google.common.base.Objects;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.collections.OffHeapLinearProbingSet;
import com.turbospaces.model.BO;
import com.turbospaces.spaces.EntryKeyLockQuard;
import com.turbospaces.spaces.SpaceCapacityRestrictionHolder;

/**
 * index manager is responsible for managing indexes over space index fields for much faster data retrieve.
 * 
 * @since 0.1
 */
@ThreadSafe
@SuppressWarnings("rawtypes")
public class IndexManager implements DisposableBean, InitializingBean {
    private final SpaceCapacityRestrictionHolder capacityRestriction;
    private final OffHeapLinearProbingSet idCache;
    private final BO bo;

    @SuppressWarnings({ "javadoc" })
    public IndexManager(final MutablePersistentEntity mutablePersistentEntity, final SpaceConfiguration configuration) {
        bo = configuration.boFor( mutablePersistentEntity.getType() );
        capacityRestriction = new SpaceCapacityRestrictionHolder( bo.getCapacityRestriction() );
        idCache = new OffHeapLinearProbingSet( configuration, bo );
    }

    /**
     * put object into cache and index space index fields(properties).
     * 
     * @param obj
     * @param idGuard
     * @param pointer
     * @return how many bytes were previously occupied by key's value
     */
    public int add(final Object obj,
                   final EntryKeyLockQuard idGuard,
                   final ByteArrayPointer pointer) {
        capacityRestriction.ensureCapacity( pointer, obj );
        int bytesOccupied = idCache.put( idGuard.getKey(), pointer );
        capacityRestriction.add( pointer.bytesOccupied(), bytesOccupied );
        return bytesOccupied;
    }

    /**
     * check whether id cache contains <code>key=uniqueIdentifier</code>
     * 
     * @param id
     * @return true if contains unique identifier as key
     */
    public boolean containsUniqueIdentifier(final Object id) {
        return idCache.contains( id );
    }

    /**
     * retrieve byte array pointer(or as serialized data) by identifier
     * 
     * @param id
     * @param asPointer
     * @return byte array pointer if any
     */
    public Object getByUniqueIdentifier(final Object id,
                                        final boolean asPointer) {
        return asPointer ? idCache.getAsPointer( id ) : idCache.getAsSerializedData( id );
    }

    /**
     * remove byte array pointer by identifier it was added with.
     * 
     * @param idGuard
     *            unique identifier lock guard
     * @return how many bytes are freed now
     */
    public int takeByUniqueIdentifier(final EntryKeyLockQuard idGuard) {
        int bytesOccupied = idCache.remove( idGuard.getKey() );
        capacityRestriction.remove( bytesOccupied );
        return bytesOccupied;
    }

    /**
     * @return total number of entities stored by uniqueIdentifier index.
     */
    public long size() {
        return capacityRestriction.getItemsCount();
    }

    /**
     * @return number of bytes dedicated for storing off-heap memory entities.
     */
    public long offHeapBytesOccuiped() {
        return capacityRestriction.getMemoryUsed();
    }

    @Override
    public void destroy() {
        idCache.destroy();
    }

    @Override
    public void afterPropertiesSet() {
        idCache.afterPropertiesSet();
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper( this )
                .add( "memoryUsed", capacityRestriction.getMemoryUsed() )
                .add( "itemsCount", capacityRestriction.getItemsCount() )
                .add( "idCache", idCache )
                .toString();
    }
}
