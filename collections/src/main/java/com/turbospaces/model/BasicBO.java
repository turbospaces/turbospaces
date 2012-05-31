package com.turbospaces.model;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * basic persistent field container.
 * 
 * @since 0.1
 */
public final class BasicBO {
    private final Field[] persistentFields;
    private final Class<?> type;

    /**
     * create new BO wrapper over type.
     * 
     * @param type
     *            introspection class
     */
    public BasicBO(final Class<?> type) {
        this.type = type;
        Set<Field> allFields = Sets.newHashSet();
        Class<?> nextClass = type;
        while ( nextClass != Object.class ) {
            Collections.addAll( allFields, nextClass.getDeclaredFields() );
            nextClass = nextClass.getSuperclass();
        }

        for ( Iterator<Field> iterator = allFields.iterator(); iterator.hasNext(); ) {
            Field field = iterator.next();
            if ( Modifier.isTransient( field.getModifiers() ) || Modifier.isStatic( field.getModifiers() ) || field.isSynthetic() )
                iterator.remove();
        }

        persistentFields = allFields.toArray( new Field[allFields.size()] );
    }

    /**
     * @return actual type
     */
    public Class<?> getType() {
        return type;
    }

    /**
     * @return persistent fields associated with this class
     */
    public Field[] getPersistentFields() {
        return persistentFields;
    }
}
