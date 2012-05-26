package com.turbospaces.collections;

/**
 * means that the cache(or cache collection) can explicitly evict theirs entries.
 * 
 * @since 0.1
 */
public interface EvictableCache {

    /**
     * evict at maximum {@code size * percentage / 100} entries from collection.
     * 
     * @param percentage
     *            how many entries to evict in absolute percent value
     * @return the number of evicted objects
     */
    int evictPercentage(int percentage);

    /**
     * evict at maximum given elements
     * 
     * @param elements
     * @return how many element evicted from cache(at maximum = given elements to evict, potentially smaller number)
     */
    int evictElements(int elements);
}
