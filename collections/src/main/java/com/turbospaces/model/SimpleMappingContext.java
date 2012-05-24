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

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mapping.model.MutablePersistentEntity;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

/**
 * simple mapping context to deal with POJO classes without any specific mapping context(like JPA, mongoDB).</p>
 * 
 * @since 0.1
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class SimpleMappingContext extends AbstractMappingContext {

    @Override
    protected MutablePersistentEntity createPersistentEntity(final TypeInformation typeInformation) {
        return new BasicPersistentEntity( typeInformation );
    }

    @Override
    protected PersistentProperty createPersistentProperty(final Field field,
                                                          final PropertyDescriptor descriptor,
                                                          final MutablePersistentEntity owner,
                                                          final SimpleTypeHolder simpleTypeHolder) {
        return new AnnotationBasedPersistentProperty( field, descriptor, owner, simpleTypeHolder ) {
            @Override
            protected Association createAssociation() {
                return null;
            }
        };
    }
}
