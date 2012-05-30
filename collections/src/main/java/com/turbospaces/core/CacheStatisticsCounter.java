/**
 * Copyright (C) 2011-2012 Andrey Borisov <aandrey.borisov@gmail.com>
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
package com.turbospaces.core;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.AbstractCache.SimpleStatsCounter;
import com.google.common.cache.CacheStats;
import com.lmax.disruptor.Sequence;

/**
 * cache statistic wrapper. contains hits/puts/takes count information.
 * 
 * @since 0.1
 */
@SuppressWarnings("javadoc")
public final class CacheStatisticsCounter extends SimpleStatsCounter {
    private final Sequence putsCount = new Sequence( 0 );
    private final Sequence takesCount = new Sequence( 0 );
    private final Sequence exclusiveReadsCount = new Sequence( 0 );

    public void recordPuts(final int count) {
        putsCount.addAndGet( count );
    }

    public void recordTakes(final int count) {
        takesCount.addAndGet( count );
    }

    public void recordExclusiveReads(final int count) {
        exclusiveReadsCount.addAndGet( count );
    }

    @Override
    public String toString() {
        CacheStats snapshot = super.snapshot();

        return Objects
                .toStringHelper( this )
                .add( "putsCount", putsCount.get() )
                .add( "takesCount", takesCount.get() )
                .add( "exclusiveReadsCount", exclusiveReadsCount.get() )
                .add( "hitsCount", snapshot.hitCount() )
                .add( "missCount", snapshot.missCount() )
                .add( "loadSuccessCount", snapshot.loadSuccessCount() )
                .add( "loadExceptionCount", snapshot.loadExceptionCount() )
                .add( "loadSuccessRate", snapshot.loadExceptionRate() )
                .add( "loadExceptionRate", snapshot.loadExceptionRate() )
                .toString();
    }

    public CompleteCacheStats snapshotCompleteCacheStats() {
        return new CompleteCacheStats( putsCount.get(), takesCount.get(), exclusiveReadsCount.get(), snapshot() );
    }

    @Immutable
    public static class CompleteCacheStats {
        private final long putsCount;
        private final long takesCount;
        private final long exclusiveReadsCount;
        private final CacheStats readStats;

        private CompleteCacheStats(final long putsCount, final long takesCount, final long exclusiveReadsCount, final CacheStats readStats) {
            super();

            Preconditions.checkArgument( putsCount >= 0 );
            Preconditions.checkArgument( takesCount >= 0 );
            Preconditions.checkArgument( exclusiveReadsCount >= 0 );

            this.putsCount = putsCount;
            this.takesCount = takesCount;
            this.exclusiveReadsCount = exclusiveReadsCount;
            this.readStats = Preconditions.checkNotNull( readStats );
        }

        public long putsCount() {
            return putsCount;
        }

        public long takesCount() {
            return takesCount;
        }

        public long exclusiveReadsCount() {
            return exclusiveReadsCount;
        }

        public CacheStats readStats() {
            return readStats;
        }

        public CompleteCacheStats minus(final CompleteCacheStats other) {
            return new CompleteCacheStats( Math.max( 0, putsCount - other.putsCount ),//
                    Math.max( 0, takesCount - other.takesCount ),//
                    Math.max( 0, exclusiveReadsCount - other.exclusiveReadsCount ),//
                    readStats.minus( other.readStats ) );
        }
    }
}
