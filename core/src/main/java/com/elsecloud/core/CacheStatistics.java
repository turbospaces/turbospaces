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
package com.elsecloud.core;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Objects;

/**
 * cache statistic wrapper. contains hits/puts/takes count information.
 * 
 * @since 0.1
 */
public final class CacheStatistics implements Cloneable {
    private final AtomicLong hitsCount = new AtomicLong();
    private final AtomicLong putsCount = new AtomicLong();
    private final AtomicLong takesCount = new AtomicLong();
    private final AtomicLong exclusiveReadsCount = new AtomicLong();
    private volatile long offHeapBytesOccupied;

    @SuppressWarnings("javadoc")
    public long getOffHeapBytesOccupied() {
        return offHeapBytesOccupied;
    }

    @SuppressWarnings("javadoc")
    public void setOffHeapBytesOccupied(final long offHeapBytesOccupied) {
        this.offHeapBytesOccupied = offHeapBytesOccupied;
    }

    @SuppressWarnings("javadoc")
    public void increaseHitsCount() {
        hitsCount.incrementAndGet();
    }

    @SuppressWarnings("javadoc")
    public void increasePutsCount() {
        putsCount.incrementAndGet();
    }

    @SuppressWarnings("javadoc")
    public void increaseTakesCount() {
        takesCount.incrementAndGet();
    }

    @SuppressWarnings("javadoc")
    public void increaseExclusiveReadsCount() {
        exclusiveReadsCount.incrementAndGet();
    }

    @SuppressWarnings("javadoc")
    public long getHitsCount() {
        return hitsCount.get();
    }

    @SuppressWarnings("javadoc")
    public long getPutsCount() {
        return putsCount.get();
    }

    @SuppressWarnings("javadoc")
    public long getTakesCount() {
        return takesCount.get();
    }

    @SuppressWarnings("javadoc")
    public long getExclusiveReadsCount() {
        return exclusiveReadsCount.get();
    }

    @Override
    public synchronized CacheStatistics clone() {
        CacheStatistics statistics = new CacheStatistics();

        statistics.hitsCount.addAndGet( getHitsCount() );
        statistics.putsCount.addAndGet( getPutsCount() );
        statistics.takesCount.addAndGet( getTakesCount() );
        statistics.exclusiveReadsCount.addAndGet( getExclusiveReadsCount() );
        statistics.setOffHeapBytesOccupied( getOffHeapBytesOccupied() );

        return statistics;
    }

    /**
     * reset the cache statistics
     */
    public synchronized void reset() {
        hitsCount.set( 0 );
        putsCount.set( 0 );
        takesCount.set( 0 );
        exclusiveReadsCount.set( 0 );
        setOffHeapBytesOccupied( 0 );
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper( this )
                .add( "hitsCount", hitsCount )
                .add( "putsCount", putsCount )
                .add( "takesCount", takesCount )
                .add( "exclusiveReadsCount", exclusiveReadsCount )
                .add( "offHeapBytesOccupied", offHeapBytesOccupied )
                .toString();
    }
}
