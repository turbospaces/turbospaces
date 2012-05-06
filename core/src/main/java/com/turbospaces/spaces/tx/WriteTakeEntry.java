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
package com.turbospaces.spaces.tx;

import javax.annotation.concurrent.Immutable;

import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.SpaceOperation;
import com.turbospaces.model.BO;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.spaces.EntryKeyLockQuard;

/**
 * wrapper around object which has been added to the space or removed. this is intermediate object(short living object)
 * and lives somewhere between space transaction begin and space transaction completition</p>
 * 
 * This class should be considered as implementation specific, however your can use this for more advanced thing (in
 * case you prefer low-level interaction with space via native interfaces).</p>
 * 
 * @since 0.1
 */
@SuppressWarnings("rawtypes")
@Immutable
public final class WriteTakeEntry {
    /**
     * jspace entity itself
     */
    private Object obj;
    /**
     * jspace entity in form of property values array.
     */
    private Object[] propertyValues;
    /**
     * primary key locker(guard)
     */
    private final EntryKeyLockQuard idQuard;
    /**
     * off-heap byte array pointer associated with entity
     */
    private final ByteArrayPointer pointer;
    /**
     * space operation caused this entity creation
     */
    private SpaceOperation spaceOperation;
    /**
     * business entity wrapper for obj field
     */
    private final BO bo;
    /**
     * jspace configuration
     */
    private final AbstractSpaceConfiguration configuration;

    /**
     * create(or get from pool) space operation wrapper for given object, id guard locker, entry pointer, property
     * values.
     * 
     * @param obj
     * @param propertyValues
     * @param idQuard
     * @param pointer
     * @param bo
     * @param spaceConfiguration
     */

    public WriteTakeEntry(final Object obj,
                          final Object[] propertyValues,
                          final EntryKeyLockQuard idQuard,
                          final ByteArrayPointer pointer,
                          final BO bo,
                          final AbstractSpaceConfiguration spaceConfiguration) {
        this.obj = obj;
        this.propertyValues = propertyValues;
        this.bo = bo;
        this.idQuard = idQuard;
        this.pointer = pointer;
        this.configuration = spaceConfiguration;
    }

    /**
     * creates(or get from pool) space operation wrapper for key, entry pointer.
     * 
     * @param idQuard
     * @param pointer
     * @param bo
     * @param spaceConfiguration
     */
    public WriteTakeEntry(final EntryKeyLockQuard idQuard,
                          final ByteArrayPointer pointer,
                          final BO bo,
                          final AbstractSpaceConfiguration spaceConfiguration) {
        this( null, null, idQuard, pointer, bo, spaceConfiguration );
    }

    /**
     * @return object which has been added/removed from space.
     */
    public Object getObj() {
        if ( obj == null )
            return configuration.getEntitySerializer().deserialize( getPointer().getSerializedBuffer(), getPersistentEntity().getType() );
        return obj;
    }

    /**
     * @return associated low-level byte array pointer
     */
    public ByteArrayPointer getPointer() {
        return pointer;
    }

    /**
     * @return key lock guard associated with entity's primary key
     */
    public EntryKeyLockQuard getIdLockQuard() {
        return idQuard;
    }

    /**
     * @return business entity class associated with {@link #getObj()} field
     */
    public BO getPersistentEntity() {
        return bo;
    }

    /**
     * @return object in form of property values array
     */
    public Object[] getPropertyValues() {
        return propertyValues;
    }

    /**
     * @return space operation caused this entry to be created for
     */
    public SpaceOperation getSpaceOperation() {
        return spaceOperation;
    }

    /**
     * set the actual space operation
     * 
     * @param spaceOperation
     */
    public void setSpaceOperation(final SpaceOperation spaceOperation) {
        this.spaceOperation = spaceOperation;
    }

    /**
     * associate write entry(as is) or de-serialized entry.
     * 
     * @param obj
     */
    public void setObj(final Object obj) {
        this.obj = obj;
    }

    /**
     * associate property values
     * 
     * @param propertyValues
     */
    public void setPropertyValues(final Object[] propertyValues) {
        this.propertyValues = propertyValues;
    }
}
