package com.turbospaces.offmemory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import org.junit.Test;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.turbospaces.core.EffectiveMemoryManager;
import com.turbospaces.core.UnsafeMemoryManager;
import com.turbospaces.model.BO;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.serialization.DecoratedKryo;

@SuppressWarnings("javadoc")
public class ByteArrayPointerTest {
    private final EffectiveMemoryManager memoryManager = new UnsafeMemoryManager();

    @Test
    public void verify()
                        throws SecurityException,
                        NoSuchMethodException,
                        ClassNotFoundException {
        DecoratedKryo kryo = new DecoratedKryo();
        BO bo = TestEntity1.getPersistentEntity();
        BO.registerPersistentClasses( kryo, bo.getOriginalPersistentEntity() );
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );

        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        byte[] serializedData = objectBuffer.writeObject( entity1 );
        ByteArrayPointer p = new ByteArrayPointer( memoryManager, serializedData, entity1, Integer.MAX_VALUE );
        p.dumpAndGetAddress();
        assertThat( p.bytesOccupied(), is( greaterThan( 0 ) ) );
        assertThat( ByteArrayPointer.getLastAccessTime( p.dumpAndGetAddress(), memoryManager ), is( lessThanOrEqualTo( System.currentTimeMillis() ) ) );
        assertThat(
                ByteArrayPointer.getCreationTimestamp( p.dumpAndGetAddress(), memoryManager ),
                is( lessThanOrEqualTo( System.currentTimeMillis() ) ) );
        long lastUpdateTmst = System.currentTimeMillis() + 123;
        ByteArrayPointer.updateLastAccessTime( p.dumpAndGetAddress(), lastUpdateTmst, memoryManager );
        assertThat( ByteArrayPointer.getLastAccessTime( p.dumpAndGetAddress(), memoryManager ), is( lastUpdateTmst ) );
        p.utilize();
    }
}
