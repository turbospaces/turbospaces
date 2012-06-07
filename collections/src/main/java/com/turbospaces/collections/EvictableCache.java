package com.turbospaces.collections;

/**
 * means that the cache(or cache collection) can explicitly evict theirs entries.
 * 
 * @since 0.1
 */
public interface EvictableCache {
    /**
     * evict at maximum {@code size * percentage / 100} entries from collection.</p>
     * 
     * @param percentage - how many entries to evict in absolute percent value.
     * @return the number of evicted objects.
     */
    long evictPercentage(int percentage);

    /**
     * evict at maximum given elements count(at maximum means that probably there are no so much entries to remove due
     * to automatic eviction(in case of entry lease expiration) for example).</p>
     * 
     * @param elements - how many entries to evict from cache.
     * @return how many element evicted from cache(at maximum = given elements to evict, potentially smaller number).
     */
    long evictElements(long elements);

    /**
     * Evict all entities from jspace(without removal from External Data Source if configured).</p>
     * 
     * This method perfectly fits junit testing requirements and you can use this method to invalidate cache after each
     * user story(or even after each junit method run).
     * 
     * @return how many entities has been removed
     */
    long evictAll();
}
