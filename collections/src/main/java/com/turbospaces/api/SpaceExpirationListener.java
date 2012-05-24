package com.turbospaces.api;

import java.nio.ByteBuffer;
import java.util.EventListener;

/**
 * turbospaces support concept of writing entities with some lease context(meaning that entity has strict time-to-live
 * and can expire). this interface provided callback for such automatic expiration handling. </p>
 * 
 * NOTE: framework does not guarantee that expiration will happen immediately after time-to-live timeout, but it is
 * guaranteed that entity will be removed during read/take by id/template, so it is impossible to retrieve such expired
 * entity.
 * 
 * @param <K>
 *            expired entry's key type
 * @param <V>
 *            expired entry's value type
 * 
 * @since 0.1
 */
public abstract class SpaceExpirationListener<K, V> implements EventListener {
    private final boolean retrieveAsEntity;

    /**
     * create space expiration listener with default parameters.
     */
    public SpaceExpirationListener() {
        this( true );
    }

    /**
     * create expiration listener and specify where you want to handle expiration notification as byte array or
     * de-serialized entity
     * 
     * @param retrieveAsEntity
     *            how to return
     */
    public SpaceExpirationListener(final boolean retrieveAsEntity) {
        super();
        this.retrieveAsEntity = retrieveAsEntity;
    }

    /**
     * callback triggered when entity has been automatically removed from jspace in case of lease expiration.
     * 
     * @param entity
     *            expired space entry
     * @param id
     *            primary key of the entity
     * @param persistentClass
     *            actual persistent class(you may want to retrieve entity as byte's array, in this case it gives you
     *            information of actual type)
     * @param originalTimeToLive
     *            original time to live for the initial entry write(or last update)
     */
    public abstract void handleNotification(V entity,
                                            K id,
                                            Class<V> persistentClass,
                                            int originalTimeToLive);

    /**
     * @return true if you want to get entity as POJO, otherwise you will get as {@link ByteBuffer} back (also take care
     *         about proper casting to ByteBuffer in {@link #handleNotification(Object, Object, Class, int)} method in
     *         this
     *         case).
     */
    public final boolean retrieveAsEntity() {
        return retrieveAsEntity;
    }
}
