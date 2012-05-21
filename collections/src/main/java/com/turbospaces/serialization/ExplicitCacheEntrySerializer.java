package com.turbospaces.serialization;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo.RegisteredClass;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.SimpleSerializer;
import com.google.common.base.Preconditions;
import com.turbospaces.model.ExplicitCacheEntry;

/**
 * kryo serializer for {@link ExplicitCacheEntry}
 * 
 * @since 0.1
 * @see PropertiesSerializer
 */
public class ExplicitCacheEntrySerializer extends SimpleSerializer<ExplicitCacheEntry> {
    private final DecoratedKryo kryo;

    /**
     * create new explicit serializer suitable to work with {@link ExplicitCacheEntry} beans.
     * 
     * @param kryo
     *            serialization provider
     */
    public ExplicitCacheEntrySerializer(final DecoratedKryo kryo) {
        this.kryo = Preconditions.checkNotNull( kryo );
    }

    @SuppressWarnings("unchecked")
    @Override
    public ExplicitCacheEntry read(final ByteBuffer buffer) {
        RegisteredClass registeredIdClass = kryo.readClass( buffer );
        RegisteredClass regiteredRoutingClass = kryo.readClass( buffer );
        RegisteredClass registeredBeanClass = kryo.readClass( buffer );
        Serializer versionSerializer = kryo.getSerializer( Integer.class );

        Object id = registeredIdClass.getSerializer().readObjectData( buffer, registeredIdClass.getType() );
        Object routing = regiteredRoutingClass != null ? regiteredRoutingClass.getSerializer().readObject( buffer, regiteredRoutingClass.getType() )
                : null;
        Object bean = registeredBeanClass.getSerializer().readObjectData( buffer, registeredBeanClass.getType() );
        Integer version = versionSerializer.readObject( buffer, Integer.class );

        return new ExplicitCacheEntry( id, bean ).withRouting( routing ).withVersion( version );
    }

    @Override
    public void write(final ByteBuffer buffer,
                      final ExplicitCacheEntry object) {
        Object id = object.getKey();
        Object routing = object.getRouting();
        Object bean = object.getBean();
        Integer version = object.getVersion();

        RegisteredClass idSerializer = kryo.writeClass( buffer, id.getClass() );
        RegisteredClass routingSerializer = kryo.writeClass( buffer, routing != null ? routing.getClass() : null );
        RegisteredClass beanSerializer = kryo.writeClass( buffer, bean.getClass() );
        RegisteredClass versionSerializer = kryo.getRegisteredClass( Integer.class );

        idSerializer.getSerializer().writeObjectData( buffer, id );
        if ( routingSerializer != null )
            routingSerializer.getSerializer().writeObject( buffer, routing );
        beanSerializer.getSerializer().writeObjectData( buffer, bean );
        versionSerializer.getSerializer().writeObject( buffer, version );
    }
}
