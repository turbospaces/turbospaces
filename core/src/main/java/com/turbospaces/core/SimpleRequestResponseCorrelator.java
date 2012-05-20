package com.turbospaces.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import com.turbospaces.api.SpaceException;

/**
 * simple request-response matcher. this is generic class, so client is responsible for passing the same key(in term of
 * reference) in order to notify on proper object's monitor.</p>
 * 
 * internal mechanism is very simple: you write original request object and associate it with some id and return monitor
 * object to the client, then when response received -> update waiting client and cleanup resources.</p>
 * 
 * @param <K>
 *            key type
 * @param <V>
 *            value type
 * @since 0.1
 */
public class SimpleRequestResponseCorrelator<K, V> {
    private final ConcurrentMap<K, Entry<V>> responses = new ConcurrentHashMap<K, Entry<V>>();

    /**
     * remove key with associated value
     * 
     * @param key
     *            operation's key
     */
    public void clear(final K key) {
        responses.remove( key );
    }

    /**
     * put response and associate it with key. it is expected that client first call this method with <code>null</code>
     * response object, just to add key and get monitor(mutex object back) and second time with real response(at this
     * stage client thread waiting for response on mutex object will be notified).
     * 
     * @param key
     *            operation's key
     * @param response
     *            actual response from server
     * 
     * @return monitor(mutex) object that needs to be passed back with {@link #responseFor(Object, Object, long)} method
     */
    public Object put(final K key,
                      final V response) {
        Entry<V> entry = responses.get( key );
        if ( entry != null ) {
            final Object mutex = entry.monitor;
            synchronized ( mutex ) {
                entry.response = response;
                if ( entry.outOfOrder )
                    clear( key );
                else
                    mutex.notify();
            }
        }
        else {
            entry = new Entry<V>();
            responses.put( key, entry );
        }
        return entry.monitor;
    }

    /**
     * synchronously wait for server response. you would need to pass key, monitor object(mutex) which you will get from
     * {@link #put(Object, Object)} and specify timeout for operation.
     * 
     * @param key
     *            operation's key
     * @param monitor
     *            object's monitor
     * @param timeoutInMillis
     *            the maximum period of time to wait for response
     * @return response for the given key (or <code>null</code> meaning that response was not retrieved within timeout)
     */
    public V responseFor(final K key,
                         final Object monitor,
                         final long timeoutInMillis) {
        assert monitor != null;
        assert key != null;

        Entry<V> entry = responses.get( key );
        Preconditions.checkNotNull( entry, "wrong usage, call put first" );
        V response = entry.response;

        // entry is not null anyway, but response can be null
        if ( response == null )
            synchronized ( monitor ) {
                try {
                    monitor.wait( timeoutInMillis );
                    response = entry.response;
                    if ( response == null )
                        entry.outOfOrder = true;
                }
                catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                    throw new SpaceException( e.getMessage(), e );
                }

                if ( response != null )
                    clear( key );
            }
        return response;
    }

    ConcurrentMap<K, Entry<V>> rawResponseMap() {
        return responses;
    }

    private static class Entry<V> {
        private V response;
        private boolean outOfOrder;
        private final Object monitor = new Object();
    }
}
