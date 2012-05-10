package com.turbospaces.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class MutableObjectTest {

    @Test
    public void test() {
        MutableObject<Integer> mutableObject = new MutableObject<Integer>();
        assertThat( mutableObject.get(), is( nullValue() ) );
        mutableObject.set( Integer.valueOf( 23 ) );
        assertThat( mutableObject.get(), is( Integer.valueOf( 23 ) ) );

        assertThat( mutableObject.get().hashCode(), is( Integer.valueOf( 23 ).hashCode() ) );
        Assert.assertTrue( mutableObject.get().equals( Integer.valueOf( 23 ) ) );
        assertThat( mutableObject.get().toString(), is( Integer.valueOf( 23 ).toString() ) );
    }
}
