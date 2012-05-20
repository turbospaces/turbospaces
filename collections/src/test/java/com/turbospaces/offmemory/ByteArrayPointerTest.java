package com.turbospaces.offmemory;

import org.junit.Test;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.turbospaces.model.BO;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.serialization.DecoratedKryo;

@SuppressWarnings("javadoc")
public class ByteArrayPointerTest {

    @Test
    public void occupiedBytes()
                               throws SecurityException,
                               NoSuchMethodException,
                               ClassNotFoundException {
        DecoratedKryo kryo = new DecoratedKryo();
        BO bo = TestEntity1.getPersistentEntity();
        kryo.registerPersistentClasses( bo.getOriginalPersistentEntity() );
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );

        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        byte[] serializedData = objectBuffer.writeObject( entity1 );
        ByteArrayPointer p = new ByteArrayPointer( serializedData, entity1, Integer.MAX_VALUE );
        p.dumpAndGetAddress();
        System.out.println( p.bytesOccupied() );
        p.utilize();
    }
}
