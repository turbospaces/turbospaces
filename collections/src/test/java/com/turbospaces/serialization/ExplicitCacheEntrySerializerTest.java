package com.turbospaces.serialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.math.RoundingMode;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowire;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.serialize.EnumSerializer;
import com.esotericsoftware.kryo.serialize.FieldSerializer;
import com.turbospaces.model.ExplicitCacheEntry;
import com.turbospaces.model.TestEntity1;

@SuppressWarnings("javadoc")
public class ExplicitCacheEntrySerializerTest {
    DecoratedKryo kryo;
    ExplicitCacheEntrySerializer serializer;

    @Before
    public void setup()
                       throws ClassNotFoundException {
        kryo = new DecoratedKryo();
        kryo.register( TestEntity1.class, new FieldSerializer( kryo, TestEntity1.class ) );

        Class<?> cl1 = Class.forName( "[L" + RoundingMode.class.getName() + ";" );
        Class<?> cl2 = Class.forName( "[L" + Autowire.class.getName() + ";" );
        SingleDimensionArraySerializer s1 = new SingleDimensionArraySerializer( cl1, kryo );
        SingleDimensionArraySerializer s2 = new SingleDimensionArraySerializer( cl2, kryo );
        kryo.register( cl1, s1 );
        kryo.register( cl2, s2 );
        kryo.register( Autowire.class, new EnumSerializer( Autowire.class ) );
        serializer = (ExplicitCacheEntrySerializer) kryo.getSerializer( ExplicitCacheEntry.class );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void able2SerializeDeserialize() {
        Long id = System.currentTimeMillis();
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        ExplicitCacheEntry<Long, TestEntity1> explicitCacheEntry1 = new ExplicitCacheEntry<Long, TestEntity1>( id, entity1 );
        explicitCacheEntry1.withRouting( "124" );

        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( explicitCacheEntry1 );
        ExplicitCacheEntry<Long, TestEntity1> explicitCacheEntry2 = objectBuffer.readObjectData( data, ExplicitCacheEntry.class );

        explicitCacheEntry1.getBean().assertMatch( explicitCacheEntry2.getBean() );
        assertThat( explicitCacheEntry1.getRouting(), is( explicitCacheEntry2.getRouting() ) );
        assertThat( explicitCacheEntry2.getVersion(), is( nullValue() ) );

        explicitCacheEntry1.withRouting( null );
        explicitCacheEntry1.withVersion( 896 );
        data = objectBuffer.writeObjectData( explicitCacheEntry1 );
        explicitCacheEntry2 = objectBuffer.readObjectData( data, ExplicitCacheEntry.class );
        explicitCacheEntry1.getBean().assertMatch( explicitCacheEntry2.getBean() );
        assertThat( explicitCacheEntry1.getVersion(), is( explicitCacheEntry2.getVersion() ) );
        assertThat( explicitCacheEntry2.getRouting(), is( nullValue() ) );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void canReadId() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        ExplicitCacheEntry<String, TestEntity1> explicitCacheEntry1 = new ExplicitCacheEntry<String, TestEntity1>(
                entity1.getUniqueIdentifier(),
                entity1 );

        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( explicitCacheEntry1 );
        assertThat( (String) serializer.readID( ByteBuffer.wrap( data ) ), is( entity1.getUniqueIdentifier() ) );
        ExplicitCacheEntry<Long, TestEntity1> explicitCacheEntry2 = objectBuffer.readObjectData( data, ExplicitCacheEntry.class );

        explicitCacheEntry1.getBean().assertMatch( explicitCacheEntry2.getBean() );

    }
}
