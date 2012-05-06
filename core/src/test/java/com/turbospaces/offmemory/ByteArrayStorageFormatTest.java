/**
 * Copyright (C) 2011 Andrey Borisov <aandrey.borisov@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.turbospaces.offmemory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import java.nio.ByteBuffer;
import java.util.Date;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.turbospaces.model.TestEntity1;
import com.turbospaces.offmemory.ByteArrayPointer;

@SuppressWarnings("javadoc")
public class ByteArrayStorageFormatTest {
    Logger logger = LoggerFactory.getLogger( getClass() );
    TestEntity1 testEntity1;

    @Before
    public void before() {
        testEntity1 = new TestEntity1();
        testEntity1.afterPropertiesSet();
    }

    @Test
    public void storesCreationTimestampCorrectly() {
        ByteArrayPointer p = new ByteArrayPointer( new byte[] { 1, 2, 3 }, testEntity1, Long.MAX_VALUE );
        long address = p.dump();
        long creationTimestamp = ByteArrayPointer.getCreationTimestamp( address );

        logger.info( "created  : = {}", new Date( creationTimestamp ) );

        assertThat( creationTimestamp - 10000, is( lessThan( System.currentTimeMillis() ) ) );
        assertThat( creationTimestamp + 10000, is( greaterThan( System.currentTimeMillis() ) ) );
    }

    @Test
    public void storesTimeToLiveCorrectly() {
        ByteArrayPointer p = new ByteArrayPointer( new byte[] { 1, 2, 3 }, testEntity1, 634 );
        long address = p.dump();
        long ttl = ByteArrayPointer.getTimeToLive( address );

        logger.info( "ttl      : = {}", ttl );

        assertThat( ttl, is( 634L ) );
    }

    @Test
    public void storesInternalStateCorrectly() {
        ByteArrayPointer p = new ByteArrayPointer( new byte[] { 1, 2, 3 }, testEntity1, Long.MAX_VALUE );
        long address = p.dump();
        byte[] b = p.getSerializedData();
        assertThat( b, is( new byte[] { 1, 2, 3 } ) );

        ByteArrayPointer p1 = new ByteArrayPointer( address, ByteBuffer.wrap( b ) );
        b = p1.getSerializedData();
        Assert.assertFalse( p.isExpired() );
        assertThat( b, is( new byte[] { 1, 2, 3 } ) );

        assertThat( p1, is( p ) );
        assertThat( p1.hashCode(), is( p.hashCode() ) );
        assertThat( p1.toString(), is( p.toString() ) );

        assertThat( p1, is( p1 ) );
        assertThat( p1.equals( p ), is( true ) );
        assertThat( p1.equals( new Object() ), is( false ) );

        assertThat( (TestEntity1) p.getObject(), is( testEntity1 ) );

        ByteArrayPointer p2 = new ByteArrayPointer( new byte[] { 2, 3, 4 }, testEntity1, Long.MAX_VALUE );
        p2.dump();
        Assert.assertFalse( p2.equals( p1 ) );

        p.utilize();
        p2.utilize();
    }
}
