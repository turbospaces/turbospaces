package com.turbospaces.model;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;

/**
 * explicit cache store entry value(when user passes both primary key and the associated value). The main purpose for
 * this class is facilitation of JDK's collections implementations and {@link Cache}
 * 
 * @param <K>
 *            ket type
 * @param <V>
 *            value type
 * 
 * @since 0.1
 */
public final class ExplicitCacheEntry<K, V> {
    private final K key;
    private Integer version;
    private Object routing;
    private final V bean;

    /**
     * create new instance of explicit cache entry with user supplied key and value
     * 
     * @param key
     *            space entry's key
     * @param bean
     *            space entry itself
     */
    public ExplicitCacheEntry(final K key, final V bean) {
        super();
        this.key = key;
        this.bean = bean;
    }

    /**
     * associate explicit routing value with this cache entry.
     * 
     * @param routing
     *            routing value
     * @return reference to this
     */
    public ExplicitCacheEntry<K, V> withRouting(final Object routing) {
        this.routing = routing;
        return this;
    }

    /**
     * associate explicit optimistic lock with this cache entry.
     * 
     * @param version
     *            optimistic lock version
     * @return reference to this
     */
    public ExplicitCacheEntry<K, V> withVersion(final Integer version) {
        this.version = version;
        return this;
    }

    /**
     * @return the key of the {@link #getBean()}
     */
    public K getKey() {
        return key;
    }

    /**
     * @return optional optimistic lock version
     */
    public Integer getVersion() {
        return version;
    }

    /**
     * @return the routing field
     */
    public Object getRouting() {
        return routing;
    }

    /**
     * @return the actual space entry
     */
    public V getBean() {
        return bean;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode( getKey(), getVersion(), getRouting(), getBean() );
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public boolean equals(final Object obj) {
        if ( obj instanceof ExplicitCacheEntry ) {
            ExplicitCacheEntry<K, V> another = (ExplicitCacheEntry) obj;
            return Objects.equal( getKey(), another.getKey() ) && Objects.equal( getVersion(), another.getVersion() )
                    && Objects.equal( getRouting(), another.getRouting() ) && Objects.equal( getBean(), another.getBean() );
        }
        return super.equals( obj );
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper( this )
                .add( "id", getKey() )
                .add( "version", getVersion() )
                .add( "routing", getRouting() )
                .add( "bean", getBean() )
                .toString();
    }
}
