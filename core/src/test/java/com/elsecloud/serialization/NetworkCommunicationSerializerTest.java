package com.elsecloud.serialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;

@SuppressWarnings("javadoc")
public class NetworkCommunicationSerializerTest {
    Kryo kryo = new Kryo();

    {
        kryo.register( byte[][].class, new NetworkCommunicationSerializer() );
    }

    @Test
    public void canSerializeDeserialize() {
        byte[][] arr = new byte[3][];
        arr[0] = new byte[] { 1, 2, 3 };
        arr[1] = new byte[] { 4, 5, 6 };
        arr[2] = new byte[] { 7, 8, 9 };

        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[][] data = objectBuffer.readObjectData( objectBuffer.writeObjectData( arr ), arr.getClass() );

        assertThat( data[0], is( new byte[] { 1, 2, 3 } ) );
        assertThat( data[1], is( new byte[] { 4, 5, 6 } ) );
        assertThat( data[2], is( new byte[] { 7, 8, 9 } ) );

        byte[][] arr2 = new byte[1][];
        arr2[0] = arr[2];

        data = objectBuffer.readObjectData( objectBuffer.writeObjectData( arr2 ), arr.getClass() );
        assertThat( data[0], is( new byte[] { 7, 8, 9 } ) );
    }
}
