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
package com.turbospaces.offmemory;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mapping.model.MutablePersistentEntity;

import com.google.common.base.Objects;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.collections.EvictableCache;
import com.turbospaces.collections.OffHeapLinearProbingSet;
import com.turbospaces.model.BO;
import com.turbospaces.serialization.PropertiesSerializer;
import com.turbospaces.spaces.EntryKeyLockQuard;

/**
 * index manager is responsible for managing indexes over space index fields for much faster data retrieve and primary
 * key itself.
 * 
 * @since 0.1
 */
@ThreadSafe
@SuppressWarnings("rawtypes")
public class IndexManager implements DisposableBean, InitializingBean, EvictableCache {
    private final OffHeapLinearProbingSet idCache;
    private final BO bo;
    private final SpaceConfiguration configuration;

    @SuppressWarnings({ "javadoc" })
    public IndexManager(final MutablePersistentEntity mutablePersistentEntity, final SpaceConfiguration configuration) {
        this.configuration = configuration;
        bo = configuration.boFor( mutablePersistentEntity.getType() );
        idCache = new OffHeapLinearProbingSet( configuration.getMemoryManager(), bo.getCapacityRestriction(), (PropertiesSerializer) configuration
                .getKryo()
                .getSerializer( mutablePersistentEntity.getType() ), configuration.getListeningExecutorService() );
    }

    /**
     * put object into cache and index space index fields(properties).
     * 
     * @param obj
     *            space object
     * @param idGuard
     *            key locker guardian
     * @param pointer
     *            byte array pointer
     * @return how many bytes were previously occupied by key's value
     */
    public int add(final Object obj,
                   final EntryKeyLockQuard idGuard,
                   final ByteArrayPointer pointer) {
        return idCache.put( idGuard.getKey(), pointer );
    }

    /**
     * check whether id cache contains <code>key=uniqueIdentifier</code>
     * 
     * @param id
     *            primary key
     * @return true if contains unique identifier as key
     */
    public boolean containsUniqueIdentifier(final Object id) {
        return idCache.contains( id );
    }

    /**
     * retrieve byte array pointer(or as serialized data) by identifier
     * 
     * @param id
     *            primary key
     * @param asPointer
     *            whether result must be returned as byte array pointer
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
        return idCache.remove( idGuard.getKey() );
    }

    /**
     * @return total number of entities stored by uniqueIdentifier index.
     */
    public long size() {
        return idCache.getCapacityMonitor().getItemsCount();
    }

    /**
     * @return number of bytes dedicated for storing off-heap memory entities.
     */
    public long offHeapBytesOccuiped() {
        return idCache.getCapacityMonitor().getMemoryUsed();
    }

    @Override
    public void destroy() {
        idCache.evictAll();
    }

    @Override
    public long evictAll() {
        return idCache.evictAll();
    }

    @Override
    public long evictPercentage(final int percentage) {
        return idCache.evictPercentage( percentage );
    }

    @Override
    public long evictElements(final long elements) {
        return idCache.evictElements( elements );
    }

    @Override
    public void afterPropertiesSet() {
        // schedule cleanup maintenance task
        configuration.getScheduledExecutorService().scheduleAtFixedRate( new Runnable() {
            @Override
            public void run() {
                idCache.cleanUp();
            }
        }, 0, configuration.getCacheCleanupPeriod(), TimeUnit.MILLISECONDS );
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper( this )
                .add( "memoryUsed", offHeapBytesOccuiped() )
                .add( "itemsCount", size() )
                .add( "idCache", idCache )
                .toString();
    }
}
