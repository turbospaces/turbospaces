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
package com.elsecloud.model;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.ThreadSafe;

import net.sf.cglib.beans.BulkBean;
import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.annotation.Routing;
import org.springframework.data.annotation.Version;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.TypeInformation;

import com.elsecloud.api.CapacityRestriction;
import com.elsecloud.api.SpaceErrors;
import com.elsecloud.core.SpaceUtility;
import com.elsecloud.spaces.CacheStoreEntryWrapper;
import com.google.common.base.Preconditions;

/**
 * trivial extension of {@link BasicPersistentEntity} which adds optimistic lock field explicitly. you should consider
 * this class as business object. </p>
 * 
 * This class designed to be used across different persistent storage adaptors(mongoDB, JPA etc) hidden over spring-data
 * abstraction layer, so prefer delegation instead of inheritance. </p>
 * 
 * @param <T>
 *            actual entity type
 * @param <P>
 *            actual persistent property
 * @since 0.1
 */
@ThreadSafe
public final class BO<T, P extends PersistentProperty<P>> implements IBOPersistentEntity<T, P>, SpaceErrors {
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final BasicPersistentEntity<T, P> delegate;
    private P optimisticLockVersionProperty, routingProperty;
    private final FastConstructor fastConstructor;
    private BulkBean bulkBean, idVersionRoutingBulkBean;
    private final Set<PersistentProperty<?>> brokenProperties = new HashSet<PersistentProperty<?>>();
    private PersistentProperty<P>[] orderedProperties;
    private CapacityRestriction capacityRestriction = new CapacityRestriction();
    private final Callable<Object> objectInstanciationCallback;

    /**
     * create business object over actual basic persistent entity
     * 
     * @param delegate
     */
    public BO(final BasicPersistentEntity<T, P> delegate) {
        this.delegate = delegate;

        fastConstructor = SpaceUtility.exceptionShouldNotHappen( new Callable<FastConstructor>() {
            @Override
            public FastConstructor call()
                                         throws Exception {
                return FastClass.create( delegate.getType() ).getConstructor( delegate.getType().getConstructor() );
            }
        } );

        objectInstanciationCallback = new Callable<Object>() {
            @Override
            public Object call()
                                throws Exception {
                return fastConstructor.newInstance();
            }
        };

        // find optimistic lock version field
        {
            final Collection<P> versionCandidates = new LinkedList<P>();
            final Collection<P> routingCandidates = new LinkedList<P>();
            doWithProperties( new PropertyHandler<P>() {

                @Override
                public void doWithPersistentProperty(final P persistentProperty) {
                    Field field = persistentProperty.getField();
                    Version annotation1 = field.getAnnotation( Version.class );
                    Routing annotation2 = field.getAnnotation( Routing.class );
                    if ( annotation1 != null )
                        versionCandidates.add( persistentProperty );
                    if ( annotation2 != null )
                        routingCandidates.add( persistentProperty );
                }
            } );
            Preconditions.checkArgument( versionCandidates.size() <= 1, "too many fields marked with Version annotation, candidates = "
                    + versionCandidates.toString() );
            Preconditions.checkArgument( routingCandidates.size() <= 1, "too many fields marked with Routing annotation, candidates = "
                    + routingCandidates.toString() );

            if ( !versionCandidates.isEmpty() )
                optimisticLockVersionProperty = versionCandidates.iterator().next();
            if ( !routingCandidates.isEmpty() )
                routingProperty = routingCandidates.iterator().next();
        }

        {
            // Java Beans convention marker
            AtomicBoolean propertyAccess = new AtomicBoolean( true );

            List<String> setters = new LinkedList<String>();
            List<String> getters = new LinkedList<String>();
            List<Class<?>> types = new LinkedList<Class<?>>();

            for ( PersistentProperty<?> persistentProperty : getOrderedProperties() ) {
                PropertyDescriptor propertyDescriptor = persistentProperty.getPropertyDescriptor();

                if ( propertyDescriptor != null ) {
                    if ( propertyDescriptor.getReadMethod() != null && propertyDescriptor.getWriteMethod() != null ) {
                        setters.add( propertyDescriptor.getWriteMethod().getName() );
                        getters.add( propertyDescriptor.getReadMethod().getName() );
                        types.add( persistentProperty.getType() );
                    }
                }
                else {
                    propertyAccess.set( false );
                    brokenProperties.add( persistentProperty );
                }
            }

            if ( propertyAccess.get() )
                // create properties extract for all persistent properties
                bulkBean = BulkBean.create(
                        getType(),
                        getters.toArray( new String[getters.size()] ),
                        setters.toArray( new String[setters.size()] ),
                        types.toArray( new Class[types.size()] ) );
            else
                logger.warn(
                        "PropetiesSerializer-{} unable to use getters-setters access optimization. Suspected/Corrupted properties = {}",
                        getType().getSimpleName(),
                        getBrokenProperties() );

            boolean canOptimizeIdProperty = getIdProperty().getPropertyDescriptor() != null
                    && getIdProperty().getPropertyDescriptor().getReadMethod() != null
                    && getIdProperty().getPropertyDescriptor().getWriteMethod() != null;

            boolean canOptimizeVersionProperty = getOptimisticLockVersionProperty() != null
                    && getOptimisticLockVersionProperty().getPropertyDescriptor() != null
                    && getOptimisticLockVersionProperty().getPropertyDescriptor().getReadMethod() != null
                    && getOptimisticLockVersionProperty().getPropertyDescriptor().getWriteMethod() != null;

            boolean canOptimizeRoutingProperty = getRoutingProperty() != null && getRoutingProperty().getPropertyDescriptor() != null
                    && getRoutingProperty().getPropertyDescriptor().getReadMethod() != null
                    && getRoutingProperty().getPropertyDescriptor().getWriteMethod() != null;

            // create id/version/routing bulk fields extractor
            if ( canOptimizeIdProperty && canOptimizeVersionProperty && canOptimizeRoutingProperty ) {
                String[] g = new String[] { getIdProperty().getPropertyDescriptor().getReadMethod().getName(),
                        getOptimisticLockVersionProperty().getPropertyDescriptor().getReadMethod().getName(),
                        getRoutingProperty().getPropertyDescriptor().getReadMethod().getName() };
                String[] s = new String[] { getIdProperty().getPropertyDescriptor().getWriteMethod().getName(),
                        getOptimisticLockVersionProperty().getPropertyDescriptor().getWriteMethod().getName(),
                        getRoutingProperty().getPropertyDescriptor().getWriteMethod().getName() };
                Class<?>[] c = new Class[] { getIdProperty().getType(), getOptimisticLockVersionProperty().getType(), getRoutingProperty().getType() };

                idVersionRoutingBulkBean = BulkBean.create( getType(), g, s, c );
            }
        }
    }

    /**
     * fill id and optimistic version information
     * 
     * @param borrowObject
     */
    public void fillIdVersionRouting(final CacheStoreEntryWrapper borrowObject) {
        if ( idVersionRoutingBulkBean != null ) {
            Object[] propertyValues = idVersionRoutingBulkBean.getPropertyValues( borrowObject.getBean() );

            borrowObject.setId( propertyValues[0] );
            borrowObject.setOptimisticLockVersion( (Integer) propertyValues[1] );
            borrowObject.setRouting( propertyValues[2] );
            return;
        }

        borrowObject.setId( SpaceUtility.getPropertyValue( borrowObject.getBeanWrapper(), getIdProperty() ) );
        if ( getOptimisticLockVersionProperty() != null )
            borrowObject.setOptimisticLockVersion( (Integer) SpaceUtility.getPropertyValue(
                    borrowObject.getBeanWrapper(),
                    getOptimisticLockVersionProperty() ) );
        if ( getRoutingProperty() != null )
            borrowObject.setRouting( SpaceUtility.getPropertyValue( borrowObject.getBeanWrapper(), getRoutingProperty() ) );
    }

    /**
     * set the bean property values using cglib if possible for faster access or reflection directly if cglib is not
     * available.
     * 
     * @param target
     *            actual bean to set properties
     * @param values
     *            array of actual values
     * @param conversionService
     *            type conversion service
     * @return target itself
     */
    @SuppressWarnings("rawtypes")
    public Object setBulkPropertyValues(final Object target,
                                        final Object[] values,
                                        final ConversionService conversionService) {
        if ( bulkBean == null ) {
            BeanWrapper beanWrapper = BeanWrapper.create( target, conversionService );
            for ( int i = 0, n = orderedProperties.length; i < n; i++ )
                SpaceUtility.setPropertyValue( beanWrapper, orderedProperties[i], values[i] );
        }
        else
            bulkBean.setPropertyValues( target, values );

        return target;
    }

    /**
     * get the bean property values using cglib if possible for faster access or reflection directly if cglib is not
     * available.
     * 
     * @param cacheEntry
     * @param conversionService
     * @return property values array
     */
    @SuppressWarnings("rawtypes")
    public Object[] getBulkPropertyValues(final CacheStoreEntryWrapper cacheEntry,
                                          final ConversionService conversionService) {

        if ( cacheEntry.getBean() == null ) {
            Object[] propertyValues = new Object[orderedProperties.length];
            propertyValues[getIdIndex()] = cacheEntry.getId();
            propertyValues[1] = cacheEntry.getOptimisticLockVersion();
            return propertyValues;
        }

        if ( bulkBean == null ) {
            BeanWrapper beanWrapper = cacheEntry.getBeanWrapper();
            Object[] propertyValues = new Object[orderedProperties.length];
            for ( int i = 0; i < orderedProperties.length; i++ )
                propertyValues[i] = SpaceUtility.getPropertyValue( beanWrapper, orderedProperties[i] );
            return propertyValues;
        }

        return bulkBean.getPropertyValues( cacheEntry.getBean() );
    }

    /**
     * create new instance of class via fast cglib constructor
     * 
     * @return new instance of {@link BO}'s underlying type
     */
    public Object newInstance() {
        return SpaceUtility.exceptionShouldNotHappen( objectInstanciationCallback );
    }

    /**
     * get the collection of broken properties - meaning those properties are being mismatched with Java Beans
     * specifications (actually read/write/or read-write methods missing for specific property).
     * <p>
     * 
     * you can use this method to test whether business object class follows Java Beans standards and can be optimized
     * with cglib for better performance.
     * 
     * @return the brokenProperties list or empty list
     */
    public List<String> getBrokenProperties() {
        List<String> l = new LinkedList<String>();
        for ( PersistentProperty<?> p : brokenProperties )
            l.add( p.getName() );
        return l;
    }

    @Override
    public P getOptimisticLockVersionProperty() {
        return optimisticLockVersionProperty;
    }

    @Override
    public P getRoutingProperty() {
        return routingProperty;
    }

    @Override
    public void setIdProperty(final P property) {
        delegate.setIdProperty( property );
    }

    @Override
    public void addPersistentProperty(final P property) {
        delegate.addPersistentProperty( property );
    }

    @Override
    public void addAssociation(final Association<P> association) {
        delegate.addAssociation( association );
    }

    @Override
    public void verify()
                        throws MappingException {
        delegate.verify();
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public PreferredConstructor<T> getPreferredConstructor() {
        return delegate.getPreferredConstructor();
    }

    @Override
    public P getIdProperty() {
        return delegate.getIdProperty();
    }

    @Override
    public P getPersistentProperty(final String name) {
        return delegate.getPersistentProperty( name );
    }

    @Override
    public Class<T> getType() {
        return delegate.getType();
    }

    @Override
    public Object getTypeAlias() {
        return delegate.getTypeAlias();
    }

    @Override
    public TypeInformation<T> getTypeInformation() {
        return delegate.getTypeInformation();
    }

    @Override
    public void doWithProperties(final PropertyHandler<P> handler) {
        delegate.doWithProperties( handler );
    }

    @Override
    public void doWithAssociations(final AssociationHandler<P> handler) {
        delegate.doWithAssociations( handler );
    }

    /**
     * associate space capacity restriction configuration on class level
     * 
     * @param capacityRestriction
     */
    public void setCapacityRestriction(final CapacityRestriction capacityRestriction) {
        Preconditions.checkNotNull( capacityRestriction );
        this.capacityRestriction = capacityRestriction;
    }

    /**
     * @return capacity restriction on class level associated with BO type
     */
    public CapacityRestriction getCapacityRestriction() {
        return capacityRestriction;
    }

    /**
     * @return collection of persistent properties sorted in alphabetical order
     */
    @SuppressWarnings("unchecked")
    public PersistentProperty<P>[] getOrderedProperties() {
        if ( orderedProperties == null ) {
            final List<PersistentProperty<P>> nonOrderedProperties = new LinkedList<PersistentProperty<P>>();

            // Potentially non-ordered properties, add to temporary set and then sort
            doWithProperties( new PropertyHandler<P>() {
                @Override
                public void doWithPersistentProperty(final P persistentProperty) {
                    if ( !persistentProperty.equals( getIdProperty() ) && !persistentProperty.equals( getOptimisticLockVersionProperty() ) )
                        nonOrderedProperties.add( persistentProperty );
                }
            } );

            // sort properties in alphabetical order
            Collections.sort( nonOrderedProperties, new Comparator<PersistentProperty<?>>() {

                @Override
                public int compare(final PersistentProperty<?> o1,
                                   final PersistentProperty<?> o2) {
                    return o1.getName().compareTo( o2.getName() );
                }
            } );

            // construct ordered properties lists where idProperty is first and version is second
            List<PersistentProperty<P>> ordered = new LinkedList<PersistentProperty<P>>();
            if ( getOptimisticLockVersionProperty() != null )
                ordered.add( getOptimisticLockVersionProperty() );
            if ( getRoutingProperty() != null )
                ordered.add( getRoutingProperty() );

            ordered.add( getIdIndex(), getIdProperty() );
            ordered.addAll( nonOrderedProperties );

            orderedProperties = ordered.toArray( new PersistentProperty[ordered.size()] );
        }
        return orderedProperties;
    }

    /**
     * @return the index of id property in {@link #getOrderedProperties()}
     */
    public static int getIdIndex() {
        return 0;
    }
}
