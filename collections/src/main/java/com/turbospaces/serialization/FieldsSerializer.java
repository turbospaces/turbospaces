package com.turbospaces.serialization;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.serialize.SimpleSerializer;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.model.BasicBO;

/**
 * seams to be better implementation than kryo's FieldSerializer.
 * 
 * @param <T>
 *            entity type
 * @since 0.1
 */
public final class FieldsSerializer<T> extends SimpleSerializer<T> {
    private final DecoratedKryo kryo;
    private final Class<T> type;
    private final CachedSerializationProperty[] properties;

    /**
     * create new fields serializer for given BO class.
     * 
     * @param kryo
     *            serializer factory
     * @param bo
     *            meta-data
     */
    @SuppressWarnings("unchecked")
    public FieldsSerializer(final DecoratedKryo kryo, final BasicBO bo) {
        super();
        this.kryo = kryo;
        this.type = (Class<T>) bo.getType();

        Field[] persistentFields = bo.getPersistentFields();
        properties = new CachedSerializationProperty[persistentFields.length];
        for ( int i = 0; i < persistentFields.length; i++ ) {
            Field field = persistentFields[i];
            CachedSerializationProperty property = new CachedSerializationProperty( field.getType(), field );
            properties[i] = property;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(final ByteBuffer buffer) {
        T newInstance = kryo.newInstance( type );
        for ( int i = 0; i < properties.length; i++ ) {
            CachedSerializationProperty property = properties[i];
            Object propertyValue = DecoratedKryo.readPropertyValue( kryo, property, buffer );
            JVMUtil.setPropertyValueUnsafe( newInstance, propertyValue, property.getPropertyType(), property.getFieldOffset() );
        }
        return newInstance;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(final ByteBuffer buffer,
                      final T object) {
        for ( int i = 0; i < properties.length; i++ ) {
            CachedSerializationProperty property = properties[i];
            DecoratedKryo.writePropertyValue(
                    kryo,
                    property,
                    JVMUtil.getPropertyValueUnsafe( object, property.getPropertyType(), property.getFieldOffset() ),
                    buffer );
        }
    }
}
