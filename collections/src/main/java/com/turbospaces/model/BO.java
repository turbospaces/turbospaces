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
package com.turbospaces.model;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.ThreadSafe;

import net.sf.cglib.beans.BulkBean;
import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastConstructor;

import org.springframework.data.annotation.Routing;
import org.springframework.data.annotation.Version;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mapping.model.BeanWrapper;

import com.esotericsoftware.kryo.serialize.EnumSerializer;
import com.esotericsoftware.minlog.Log;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.turbospaces.api.CapacityRestriction;
import com.turbospaces.serialization.DecoratedKryo;
import com.turbospaces.serialization.PropertiesSerializer;
import com.turbospaces.serialization.SingleDimensionArraySerializer;

/**
 * Business object wrapper around spring's data {@link PersistentEntity}.</p>
 * 
 * @since 0.1
 */
@ThreadSafe
@SuppressWarnings({ "rawtypes", "unchecked" })
public final class BO {

    private final BasicPersistentEntity delegate;
    private PersistentProperty optimisticLockVersionProperty, routingProperty;
    private final FastConstructor fastConstructor;
    private BulkBean bulkBean, idVersionRoutingBulkBean;
    private final Set<PersistentProperty> brokenProperties = new HashSet<PersistentProperty>();
    private PersistentProperty[] orderedProperties;
    private CapacityRestriction capacityRestriction = new CapacityRestriction();

    /**
     * create business object over actual basic persistent entity
     * 
     * @param delegate
     *            the actual persistent entity meta-data provider
     * @throws NoSuchMethodException
     *             re-throw cglib exception
     * @throws SecurityException
     *             re-throw cglib exception
     */
    public BO(final BasicPersistentEntity delegate) throws SecurityException, NoSuchMethodException {
        this.delegate = delegate;
        this.fastConstructor = FastClass.create( delegate.getType() ).getConstructor( delegate.getType().getConstructor() );

        // find optimistic lock version field
        {
            final Collection<PersistentProperty> versionCandidates = Lists.newLinkedList();
            final Collection<PersistentProperty> routingCandidates = Lists.newLinkedList();
            delegate.doWithProperties( new PropertyHandler() {

                @Override
                public void doWithPersistentProperty(final PersistentProperty persistentProperty) {
                    Field field = persistentProperty.getField();
                    Version annotation1 = field.getAnnotation( Version.class );
                    Routing annotation2 = field.getAnnotation( Routing.class );
                    if ( annotation1 != null )
                        versionCandidates.add( persistentProperty );
                    if ( annotation2 != null )
                        routingCandidates.add( persistentProperty );
                }
            } );
            Preconditions.checkArgument( versionCandidates.size() <= 1, "too many fields marked with @Version annotation, candidates = "
                    + versionCandidates.toString() );
            Preconditions.checkArgument( routingCandidates.size() <= 1, "too many fields marked with @Routing annotation, candidates = "
                    + routingCandidates.toString() );

            if ( !versionCandidates.isEmpty() )
                optimisticLockVersionProperty = versionCandidates.iterator().next();
            if ( !routingCandidates.isEmpty() )
                routingProperty = routingCandidates.iterator().next();
        }

        {
            // Java Beans convention marker
            AtomicBoolean propertyAccess = new AtomicBoolean( true );

            List<String> setters = Lists.newLinkedList();
            List<String> getters = Lists.newLinkedList();
            List<Class<?>> types = Lists.newLinkedList();

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
                        delegate.getType(),
                        getters.toArray( new String[getters.size()] ),
                        setters.toArray( new String[setters.size()] ),
                        types.toArray( new Class[types.size()] ) );
            else
                Log.warn( String.format(
                        "PropetiesSerializer-%s unable to use getters-setters access optimization. Suspected/Corrupted properties = %s",
                        delegate.getType().getSimpleName(),
                        getBrokenProperties() ) );

            boolean canOptimizeIdProperty = hasReadWriteMethods( delegate.getIdProperty() );
            boolean canOptimizeVersionProperty = hasReadWriteMethods( getOptimisticLockVersionProperty() );
            boolean canOptimizeRoutingProperty = hasReadWriteMethods( getRoutingProperty() );

            // create id/version/routing bulk fields extractor
            if ( canOptimizeIdProperty && canOptimizeVersionProperty && canOptimizeRoutingProperty ) {
                String[] g = new String[] { delegate.getIdProperty().getPropertyDescriptor().getReadMethod().getName(),
                        getOptimisticLockVersionProperty().getPropertyDescriptor().getReadMethod().getName(),
                        getRoutingProperty().getPropertyDescriptor().getReadMethod().getName() };
                String[] s = new String[] { delegate.getIdProperty().getPropertyDescriptor().getWriteMethod().getName(),
                        getOptimisticLockVersionProperty().getPropertyDescriptor().getWriteMethod().getName(),
                        getRoutingProperty().getPropertyDescriptor().getWriteMethod().getName() };
                Class<?>[] c = new Class[] { delegate.getIdProperty().getType(), getOptimisticLockVersionProperty().getType(),
                        getRoutingProperty().getType() };

                idVersionRoutingBulkBean = BulkBean.create( delegate.getType(), g, s, c );
            }
        }
    }

    /**
     * fill id and optimistic version property values.
     * 
     * @param cacheStoreEntry
     *            the target object and properties provider
     */
    public void fillIdVersionRouting(final CacheStoreEntryWrapper cacheStoreEntry) {
        if ( idVersionRoutingBulkBean != null ) {
            Object[] propertyValues = idVersionRoutingBulkBean.getPropertyValues( cacheStoreEntry.getBean() );

            cacheStoreEntry.setId( propertyValues[0] );
            cacheStoreEntry.setOptimisticLockVersion( (Integer) propertyValues[1] );
            cacheStoreEntry.setRouting( propertyValues[2] );
            return;
        }

        final BeanWrapper beanWrapper = BeanWrapper.create( cacheStoreEntry.getBean(), null );
        cacheStoreEntry.setId( beanWrapper.getProperty( delegate.getIdProperty(), delegate.getIdProperty().getType(), false ) );
        if ( getOptimisticLockVersionProperty() != null )
            cacheStoreEntry.setOptimisticLockVersion( (Integer) beanWrapper.getProperty(
                    getOptimisticLockVersionProperty(),
                    getOptimisticLockVersionProperty().getType(),
                    false ) );
        if ( getRoutingProperty() != null )
            cacheStoreEntry.setRouting( beanWrapper.getProperty( getRoutingProperty(), getRoutingProperty().getType(), false ) );
    }

    /**
     * set the bean property values using cglib if possible for faster access or reflection directly if cglib is not
     * available.
     * 
     * @param target
     *            actual bean to set properties
     * @param values
     *            array of actual values
     * @return target itself
     */
    public Object setBulkPropertyValues(final Object target,
                                        final Object[] values) {
        // if we can't use cglib for optimization
        if ( bulkBean == null ) {
            BeanWrapper beanWrapper = BeanWrapper.create( target, null );
            for ( int i = 0, n = orderedProperties.length; i < n; i++ )
                beanWrapper.setProperty( orderedProperties[i], values[i], false );
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
     *            space cache store entry wrapper
     * @return property values array
     */
    public Object[] getBulkPropertyValues(final CacheStoreEntryWrapper cacheEntry) {
        // if this is read operation and we don't have bean associated, read id/routing/version fields.
        if ( cacheEntry.getBean() == null ) {
            Object[] propertyValues = new Object[orderedProperties.length];
            propertyValues[getIdIndex()] = cacheEntry.getId();
            propertyValues[1] = cacheEntry.getOptimisticLockVersion();
            return propertyValues;
        }

        // if cglib can't be used for property extraction, us spring's data to read property values
        if ( bulkBean == null ) {
            final BeanWrapper beanWrapper = BeanWrapper.create( cacheEntry.getBean(), null );
            Object[] propertyValues = new Object[orderedProperties.length];
            for ( int i = 0; i < orderedProperties.length; i++ )
                propertyValues[i] = beanWrapper.getProperty( orderedProperties[i], orderedProperties[i].getType(), false );
            return propertyValues;
        }

        // otherwise try to read property values using cglib(if possible)
        return bulkBean.getPropertyValues( cacheEntry.getBean() );
    }

    /**
     * create new instance of class via fast cglib constructor
     * 
     * @return new instance of {@link BO}'s underlying type
     */
    public Object newInstance() {
        for ( ;; )
            try {
                return fastConstructor.newInstance();
            }
            catch ( InvocationTargetException e ) {
                Log.error( e.getMessage(), e );
                Throwables.propagate( e );
            }
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
        List<String> l = Lists.newLinkedList();
        for ( PersistentProperty<?> p : brokenProperties )
            l.add( p.getName() );
        return l;
    }

    /**
     * @return the optimistic lock version property(if any)
     */
    public PersistentProperty getOptimisticLockVersionProperty() {
        return optimisticLockVersionProperty;
    }

    /**
     * @return the routing property(if any)
     */
    public PersistentProperty getRoutingProperty() {
        return routingProperty;
    }

    /**
     * @return the id property
     */
    public PersistentProperty getIdProperty() {
        return Preconditions.checkNotNull( getOriginalPersistentEntity().getIdProperty() );
    }

    /**
     * get the persistent property by name
     * 
     * @param propertyName
     *            persistent property name
     * @return the persistent property
     */
    public Object getPersistentProperty(final String propertyName) {
        return Preconditions.checkNotNull( getOriginalPersistentEntity().getPersistentProperty( propertyName ) );
    }

    /**
     * @return spring's data persistent entity
     */
    public BasicPersistentEntity getOriginalPersistentEntity() {
        return delegate;
    }

    /**
     * associate space capacity restriction configuration on class level
     * 
     * @param capacityRestriction
     *            off-heap buffer capacity restriction configuration
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
    public PersistentProperty[] getOrderedProperties() {
        if ( orderedProperties == null ) {
            final List<PersistentProperty> nonOrderedProperties = Lists.newLinkedList();

            // Potentially non-ordered properties, add to temporary set and then sort
            getOriginalPersistentEntity().doWithProperties( new PropertyHandler() {
                @Override
                public void doWithPersistentProperty(final PersistentProperty persistentProperty) {
                    if ( !persistentProperty.equals( getOriginalPersistentEntity().getIdProperty() )
                            && !persistentProperty.equals( getOptimisticLockVersionProperty() ) && !persistentProperty.equals( getRoutingProperty() ) )
                        nonOrderedProperties.add( persistentProperty );
                }
            } );

            // sort properties in alphabetical order
            Collections.sort( nonOrderedProperties, new Comparator<PersistentProperty>() {
                @Override
                public int compare(final PersistentProperty o1,
                                   final PersistentProperty o2) {
                    return o1.getName().compareTo( o2.getName() );
                }
            } );

            // construct ordered properties lists where idProperty is first and version is second
            List<PersistentProperty> ordered = Lists.newLinkedList();
            if ( getOptimisticLockVersionProperty() != null )
                ordered.add( getOptimisticLockVersionProperty() );
            if ( getRoutingProperty() != null )
                ordered.add( getRoutingProperty() );

            // set id field first - we need this optimization to be fast in matchById reading
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

    private static boolean hasReadWriteMethods(final PersistentProperty p) {
        return p != null && p.getPropertyDescriptor() != null && p.getPropertyDescriptor().getReadMethod() != null
                && p.getPropertyDescriptor().getWriteMethod() != null;
    }

    /**
     * register the set of persistent classes and enrich kryo with some extract serialized related to persistent class.
     * 
     * @param kryo
     *            serialization provider
     * @param persistentEntities
     *            classes to register
     * @throws ClassNotFoundException
     *             re-throw conversion service
     * @throws NoSuchMethodException
     *             re-throw cglib's exception
     * @throws SecurityException
     *             re-throw cglib's exception
     */
    public static void registerPersistentClasses(final DecoratedKryo kryo,
                                                 final BasicPersistentEntity... persistentEntities)
                                                                                                   throws ClassNotFoundException,
                                                                                                   SecurityException,
                                                                                                   NoSuchMethodException {
        for ( BasicPersistentEntity<?, ?> e : persistentEntities ) {
            BO bo = new BO( e );
            bo.getOriginalPersistentEntity().doWithProperties( new PropertyHandler() {
                @Override
                public void doWithPersistentProperty(final PersistentProperty p) {
                    Class type = p.getType();
                    if ( type.isArray() && !kryo.isTypeRegistered( type ) ) {
                        SingleDimensionArraySerializer serializer = new SingleDimensionArraySerializer( type, kryo );
                        kryo.register( type, serializer );
                    }
                    else if ( type.isEnum() && !kryo.isTypeRegistered( type ) ) {
                        EnumSerializer enumSerializer = new EnumSerializer( type );
                        kryo.register( type, enumSerializer );
                    }
                }
            } );
            Class<?> arrayWrapperType = Class.forName( "[L" + e.getType().getName() + ";" );
            PropertiesSerializer serializer = new PropertiesSerializer( kryo, bo );
            SingleDimensionArraySerializer arraysSerializer = new SingleDimensionArraySerializer( arrayWrapperType, kryo );
            kryo.register( e.getType(), serializer );
            kryo.register( arrayWrapperType, arraysSerializer );
        }
    }
}
