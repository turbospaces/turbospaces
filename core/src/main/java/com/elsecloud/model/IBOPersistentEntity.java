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
package com.elsecloud.model;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.model.MutablePersistentEntity;

/**
 * subclass of spring-data's persistent entity class which adds concept of optimistic locking which is quite important
 * for data consistency.
 * 
 * @since 0.1
 * @param <T>
 *            entity type
 * @param <P>
 *            primary key type
 * 
 * @since 0.1
 */
public interface IBOPersistentEntity<T, P extends PersistentProperty<P>> extends MutablePersistentEntity<T, P> {

    /**
     * Returns the optimistic locking property of the {@link PersistentEntity}. Basically the
     * optimistic locking mechanism is something missing by persistent entity (or optional), but it is absolutely
     * critical for data consistency, that's why optimistic locking field defined as a standalone property.
     * 
     * @return the id property of the {@link PersistentEntity}.
     */
    P getOptimisticLockVersionProperty();

    /**
     * Returns the routing property of the {@link PersistentEntity}. Basically the routing mechanism allows you to split
     * the data across different nodes of cluster. Currently the direct field markup supported.
     * 
     * @return the routing property of the {@link PersistentEntity}
     */
    P getRoutingProperty();
}
