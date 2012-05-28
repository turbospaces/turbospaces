package com.turbospaces.collections;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.samples.jpetstore.domain.Account;

import com.google.common.cache.Cache;
import com.google.common.util.concurrent.MoreExecutors;
import com.turbospaces.api.SpaceExpirationListener;
import com.turbospaces.pool.ObjectFactory;

@SuppressWarnings("javadoc")
public class BasicGuavaCacheTest {
    PetStoreModelProvider petStoreModelProvider = new PetStoreModelProvider();

    @Test
    public void sunnyDayScenario() {
        ObjectFactory<Account> objectFactory = petStoreModelProvider.newAccountsObjectFactory();
        Cache<String, Account> cache = new GuavaOffHeapCacheBuilder<String, Account>().build( Account.class );

        for ( int i = 0; i < 1345; i++ ) {
            Account account = objectFactory.newInstance();
            cache.put( account.getUsername(), account );
            Assert.assertTrue( cache.getIfPresent( account.getUsername() ) != null );
            cache.invalidate( account.getUsername() );
            Assert.assertTrue( cache.getIfPresent( account.getUsername() ) == null );
        }
    }

    @Test
    public void automaticEvictionSunnyDayScenario()
                                                   throws InterruptedException {
        final AtomicInteger expired = new AtomicInteger();
        ExecutorService cachedThreadPool = MoreExecutors.sameThreadExecutor();
        ObjectFactory<Account> objectFactory = petStoreModelProvider.newAccountsObjectFactory();
        Cache<String, Account> cache = new GuavaOffHeapCacheBuilder<String, Account>()
                .executorService( cachedThreadPool )
                .expireAfterWrite( 1, TimeUnit.MILLISECONDS )
                .expirationListener( new SpaceExpirationListener<String, Account>() {
                    @Override
                    public void handleNotification(final Account entity,
                                                   final String id,
                                                   final Class<Account> persistentClass,
                                                   final int originalTimeToLive) {
                        expired.incrementAndGet();
                        Assert.assertTrue( persistentClass == Account.class );
                        Assert.assertEquals( id, entity.getUsername() );
                        Assert.assertEquals( 1, originalTimeToLive );
                    }
                } )
                .build( Account.class );
        Account[] entities = new Account[1345];
        for ( int i = 0; i < entities.length; i++ ) {
            Account account = objectFactory.newInstance();
            entities[i] = account;
            cache.put( account.getUsername(), account );
        }
        Thread.sleep( 2 );
        for ( int i = 0; i < entities.length; i++ )
            Assert.assertTrue( cache.getIfPresent( entities[i].getUsername() ) == null );

        Assert.assertEquals( cache.size(), 0 );
        Assert.assertEquals( expired.get(), entities.length );
    }
}
