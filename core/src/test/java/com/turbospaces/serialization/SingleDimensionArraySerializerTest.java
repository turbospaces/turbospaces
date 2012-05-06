package com.turbospaces.serialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.turbospaces.api.SpaceOperation;
import com.turbospaces.serialization.SingleDimensionArraySerializer;

@SuppressWarnings("javadoc")
@RunWith(Parameterized.class)
public class SingleDimensionArraySerializerTest {
    SingleDimensionArraySerializer arraySerializer;
    Kryo kryo = new Kryo();
    Object array;

    {
        kryo.register( SpaceOperation.class );
    }

    public SingleDimensionArraySerializerTest(final Object array) {
        super();
        this.array = array;
    }

    @Parameters
    public static List<Object[]> data() {
        return Arrays.asList( new Object[][] { { new int[] { 1, 2, 3 } }, { new long[] { 1, 2, 3 } }, { new float[] { 1, 2, 3 } },
                { new double[] { 1, 2, 3 } }, { new boolean[] { true, false, true } }, { new char[] { 'a', 'b', 'c' } }, { new byte[] { 1, 2, 3 } },
                { SpaceOperation.values() } } );
    }

    @Before
    public void setUp() {
        arraySerializer = new SingleDimensionArraySerializer( array.getClass(), kryo );
        kryo.register( array.getClass(), arraySerializer );
    }

    @Test
    public void testSerializeDeserialize() {
        ObjectBuffer buffer = new ObjectBuffer( kryo );
        byte[] writeObjectData = buffer.writeObjectData( array );
        assertThat( writeObjectData.length, is( greaterThan( 1 ) ) );

        Object data = buffer.readObjectData( writeObjectData, array.getClass() );
        assertThat( data, is( array ) );
    }
}
