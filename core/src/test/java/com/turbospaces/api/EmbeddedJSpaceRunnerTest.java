package com.turbospaces.api;

import java.util.Collections;

import org.junit.Test;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.turbospaces.model.TestEntity1;

@SuppressWarnings("javadoc")
public class EmbeddedJSpaceRunnerTest {
    public static final String CLIENT_CONTEXT = "classpath*:META-INF/spring/jspace-client-test-context.xml";
    public static final String SERVER_CONTEXT = "classpath*:META-INF/spring/jspace-test-context.xml";

    public static SpaceConfiguration configurationFor()
                                                       throws Exception {
        SpaceConfiguration configuration = new SpaceConfiguration();
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setInitialEntitySet( Collections.singleton( TestEntity1.class ) );
        mappingContext.afterPropertiesSet();
        configuration.setMappingContext( mappingContext );
        configuration.afterPropertiesSet();
        return configuration;
    }

    public static ClientSpaceConfiguration clientConfigurationFor()
                                                                   throws Exception {
        ClientSpaceConfiguration configuration = new ClientSpaceConfiguration();
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setInitialEntitySet( Collections.singleton( TestEntity1.class ) );
        mappingContext.afterPropertiesSet();
        configuration.setMappingContext( mappingContext );
        configuration.afterPropertiesSet();
        return configuration;

    }

    @Test
    public void test()
                      throws Exception {
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    EmbeddedJSpaceRunner.main( SERVER_CONTEXT );
                }
                catch ( Exception e ) {}
            }
        };
        thread.start();
        EmbeddedJSpaceRunner.awaitNetworkJoin();
        Thread.sleep( 10 );
        thread.interrupt();
        thread.join();
    }
}
