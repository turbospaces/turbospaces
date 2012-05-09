package com.turbospaces.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

@SuppressWarnings("javadoc")
public class ConsistentHasherTest {

    @Test
    public void test() {
        ConsistentHasher<List<Integer>> consistentHasher = new ConsistentHasher<List<Integer>>(
                (short) 1024,
                (short) 16,
                new Supplier<List<Integer>>() {
                    Random random = new Random();

                    @Override
                    public List<Integer> get() {
                        return Lists.newArrayList( random.nextInt() );
                    }
                } );
        for ( int i = 0; i < 7234; i++ )
            assertThat( consistentHasher.segmentFor( i ), is( notNullValue() ) );
    }
}
