package com.turbospaces.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

@SuppressWarnings("javadoc")
public class ConsistentHasherTest {
    ConsistentHasher<List<Integer>> consistentHasher = new ConsistentHasher<List<Integer>>( 16, 1024, new Supplier<List<Integer>>() {
        Random random = new Random();

        @Override
        public List<Integer> get() {
            return Lists.newArrayList( random.nextInt() );
        }
    } );

    @Test
    public void segmentsForKeysAreNotEmpty() {
        for ( int i = 0; i < 7234; i++ )
            assertThat( consistentHasher.segmentFor( i ), is( notNullValue() ) );
    }

    @Test
    public void segmentsConsistentForTheSameKey() {
        for ( int i = 0; i < 234; i++ ) {
            List<Integer> prev = null;
            for ( int j = 0; j < 128; j++ ) {
                List<Integer> segment = consistentHasher.segmentFor( i );
                assertThat( segment, is( notNullValue() ) );
                if ( prev == null )
                    prev = segment;
                Assert.assertTrue( String.format( " %s != %s", prev, segment ), prev == segment );
            }
        }
    }
}
