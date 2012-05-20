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
