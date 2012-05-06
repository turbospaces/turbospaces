package com.turbospaces.api;

import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.turbospaces.api.SpaceConfiguration;

@SuppressWarnings("javadoc")
public class NegativeConfigurationIntegrationTest {

    @Test(expected = BeanCreationException.class)
    public void cantInitializeWithMultipleMappingContext() {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
                "classpath*:META-INF/spring/negative/multiple-mapping-contexts.xml" );
        context.getBean( SpaceConfiguration.class );
    }
}
