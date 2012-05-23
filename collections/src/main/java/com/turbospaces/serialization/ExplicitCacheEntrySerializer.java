package com.turbospaces.serialization;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo.RegisteredClass;
import com.esotericsoftware.kryo.Serializer;
import com.google.common.base.Preconditions;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.model.ExplicitCacheEntry;

/**
 * kryo serializer for {@link ExplicitCacheEntry}
 * 
 * @since 0.1
 * @see PropertiesSerializer
 */
public class ExplicitCacheEntrySerializer extends MatchingSerializer<ExplicitCacheEntry<?, ?>> {
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public ExplicitCacheEntry<?, ?> read(final ByteBuffer buffer) {
        RegisteredClass registeredIdClass = kryo.readClass( buffer );
        Object id = registeredIdClass.getSerializer().readObjectData( buffer, registeredIdClass.getType() );

        RegisteredClass regiteredRoutingClass = kryo.readClass( buffer );
        RegisteredClass registeredBeanClass = kryo.readClass( buffer );
        Serializer versionSerializer = kryo.getSerializer( Integer.class );

        Object routing = regiteredRoutingClass != null ? regiteredRoutingClass.getSerializer().readObject( buffer, regiteredRoutingClass.getType() )
                : null;
        Object bean = registeredBeanClass.getSerializer().readObjectData( buffer, registeredBeanClass.getType() );
        Integer version = versionSerializer.readObject( buffer, Integer.class );

        return new ExplicitCacheEntry( id, bean ).withRouting( routing ).withVersion( version );
    }

    @Override
    public void write(final ByteBuffer buffer,
                      final ExplicitCacheEntry<?, ?> object) {
        Object id = object.getKey();
        Object routing = object.getRouting();
        Object bean = object.getBean();
        Integer version = object.getVersion();

        RegisteredClass idSerializer = kryo.writeClass( buffer, id.getClass() );
        idSerializer.getSerializer().writeObjectData( buffer, id );

        RegisteredClass routingSerializer = kryo.writeClass( buffer, routing != null ? routing.getClass() : null );
        RegisteredClass beanSerializer = kryo.writeClass( buffer, bean.getClass() );
        RegisteredClass versionSerializer = kryo.getRegisteredClass( Integer.class );

        if ( routingSerializer != null )
            routingSerializer.getSerializer().writeObject( buffer, routing );
        beanSerializer.getSerializer().writeObjectData( buffer, bean );
        versionSerializer.getSerializer().writeObject( buffer, version );
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object readID(final ByteBuffer buffer) {
        buffer.clear();
        RegisteredClass registeredIdClass = kryo.readClass( buffer );
        Object id = registeredIdClass.getSerializer().readObjectData( buffer, registeredIdClass.getType() );
        buffer.clear();
        return id;
    }

    @Override
    public boolean match(final ByteBuffer buffer,
                         final CacheStoreEntryWrapper cacheEntryTemplate) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Class<?> getType() {
        // TODO Auto-generated method stub
        return null;
    }
}
