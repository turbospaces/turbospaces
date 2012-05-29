package com.turbospaces.collections;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.samples.jpetstore.domain.Account;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.turbospaces.core.ObjectFactory;
import com.turbospaces.core.PerformanceMonitor;

@SuppressWarnings("javadoc")
public class PetStoreModelProvider {

    public ObjectFactory<Account> newAccountsObjectFactory() {
        return new ObjectFactory<Account>() {
            @Override
            public Account newInstance() {
                long now = System.nanoTime();
                Account account = new Account();
                account.setUsername( "username_" + now );
                account.setPassword( "password_" + now );
                account.setEmail( now + "@gmail.com" );
                account.setFirstName( "firstname_" + now );
                account.setLastName( "lastname_" + now );
                account.setStatus( "active" );
                account.setAddress1( "address_1_" + now );
                account.setAddress2( "address_2_" + now );
                account.setCity( "kiev_" + now );
                account.setState( "state_" + now );
                account.setZip( "zip_" + now );
                account.setCountry( "ukraine_" + now );
                account.setPhone( "+38093" + now );
                account.setFavouriteCategoryId( String.valueOf( now ) );
                account.setLanguagePreference( "ua" );
                account.setListOption( true );
                account.setBannerOption( true );
                account.setBannerName( "banner_" + now );
                return account;
            }

            @Override
            public void invalidate(final Account obj) {
                obj.setUsername( null );
                obj.setPassword( null );
                obj.setEmail( null );
                obj.setFirstName( null );
                obj.setLastName( null );
                obj.setStatus( null );
                obj.setAddress1( null );
                obj.setAddress2( null );
                obj.setCity( null );
                obj.setState( null );
                obj.setZip( null );
                obj.setCountry( null );
                obj.setPhone( null );
                obj.setFavouriteCategoryId( null );
                obj.setLanguagePreference( null );
                obj.setListOption( false );
                obj.setBannerOption( false );
                obj.setBannerName( null );
            }
        };
    }

    public PerformanceMonitor<Account> guavaMonitor(final Cache<String, Account> cache) {
        return new PerformanceMonitor<Account>( new Function<Map.Entry<String, Account>, Account>() {
            @Override
            public Account apply(final Entry<String, Account> input) {
                Account account = input.getValue();
                account.setUsername( "username_" + input.getKey() );
                cache.put( input.getKey(), account );
                return account;
            }
        }, new Function<String, Account>() {
            @Override
            public Account apply(final String input) {
                return cache.getIfPresent( input );
            }
        }, new Function<String, Account>() {
            @Override
            public Account apply(final String input) {
                cache.invalidate( input );
                return null;
            }
        }, newAccountsObjectFactory() ).withNumberOfIterations( 1 * 1000000 );
    }
}
