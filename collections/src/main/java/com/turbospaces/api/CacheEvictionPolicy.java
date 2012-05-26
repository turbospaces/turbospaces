package com.turbospaces.api;

/**
 * when the space store exceeds the maximum memory/capacity you may want to evict entries from cache or start rejecting
 * puts and raise {@link SpaceCapacityOverflowException} errors.
 * .
 * 
 * @since 0.1
 */
public enum CacheEvictionPolicy {
    /**
     * reject puts and throw {@link SpaceCapacityOverflowException} for each illegal attempt to add new cache entries.
     */
    REJECT,

    /**
     * Least Recently Used.</p>
     * Evict entries that haven't been used recently or very often.
     */
    LRU,

    /**
     * Least Frequently Used. </p>
     * Evict entries according to hits count.
     */
    LFU,

    /**
     * First In First Out. </p>
     * Evict entries according to "first in first out" principle.
     */
    FIFO,

    /**
     * Random eviction. </p>
     * Evict random entities.
     */
    RANDOM;

    /**
     * @return if this is {@link #LFU} or {@link #LRU} or {@link #FIFO} or {@link #RANDOM} eviction policy.
     */
    public boolean isEviction() {
        return this == LRU || this == LFU || this == FIFO || this == RANDOM;
    }
}
