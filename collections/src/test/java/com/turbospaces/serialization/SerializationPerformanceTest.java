package com.turbospaces.serialization;

import java.math.RoundingMode;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.data.mapping.model.BasicPersistentEntity;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.serialize.EnumSerializer;
import com.esotericsoftware.kryo.serialize.FieldSerializer;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.model.BO;
import com.turbospaces.model.SimpleMappingContext;
import com.turbospaces.model.TestEntity1;

@SuppressWarnings("javadoc")
public class SerializationPerformanceTest extends AbstractBenchmark {
    DecoratedKryo kryo;
    TestEntity1 entity1;

    @Before
    public void before() {
        kryo = new DecoratedKryo();
        entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
    }

    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 1)
    @Test
    public void runDefaultKryoSerialization()
                                             throws ClassNotFoundException {
        Class<?> cl1 = Class.forName( "[L" + RoundingMode.class.getName() + ";" );
        Class<?> cl2 = Class.forName( "[L" + Autowire.class.getName() + ";" );
        SingleDimensionArraySerializer s1 = new SingleDimensionArraySerializer( cl1, kryo );
        SingleDimensionArraySerializer s2 = new SingleDimensionArraySerializer( cl2, kryo );
        kryo.register( cl1, s1 );
        kryo.register( cl2, s2 );
        kryo.register( Autowire.class, new EnumSerializer( Autowire.class ) );
        kryo.register( TestEntity1.class, new FieldSerializer( kryo, TestEntity1.class ) );
        run( true );
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 1)
    @Test
    public void runTurbospaceKryoSerialization()
                                                throws SecurityException,
                                                NoSuchMethodException,
                                                ClassNotFoundException {
        SimpleMappingContext mappingContext = new SimpleMappingContext();
        mappingContext.setInitialEntitySet( Collections.singleton( TestEntity1.class ) );
        mappingContext.afterPropertiesSet();
        BO bo = new BO( (BasicPersistentEntity) mappingContext.getPersistentEntity( TestEntity1.class ) );
        BO.registerPersistentClasses( kryo, bo.getOriginalPersistentEntity() );
        run( false );
    }

    private void run(final boolean nativeAccess) {
        int iterations = 10 * 1000 * 1000;
        long now = System.currentTimeMillis();

        JVMUtil.repeatConcurrently( Runtime.getRuntime().availableProcessors(), iterations, new Runnable() {

            @Override
            public void run() {
                ObjectBuffer buffer = new ObjectBuffer( kryo );
                buffer.setKryo( kryo );
                buffer.writeObjectData( entity1 );
            }
        } );

        double seconds = ( (double) ( System.currentTimeMillis() - now ) / 1000 );
        System.out.println( ( nativeAccess ? "direct access:->" : "cglib access->" ) + "serialization TPS = " + ( (int) ( iterations / seconds ) ) );

        ObjectBuffer buffer = new ObjectBuffer( kryo );
        buffer.setKryo( kryo );
        final byte[] data = buffer.writeObjectData( entity1 );

        now = System.currentTimeMillis();
        JVMUtil.repeatConcurrently( Runtime.getRuntime().availableProcessors(), iterations, new Runnable() {

            @Override
            public void run() {
                ObjectBuffer buffer = new ObjectBuffer( kryo );
                buffer.setKryo( kryo );
                buffer.readObjectData( data, TestEntity1.class );
            }
        } );

        seconds = ( (double) ( System.currentTimeMillis() - now ) / 1000 );
        System.out
                .println( ( nativeAccess ? "direct access:->" : "cglib access->" ) + "de-serialization TPS = " + ( (int) ( iterations / seconds ) ) );
    }
}
