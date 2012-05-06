package com.turbospaces.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.SpaceUtility;

@SuppressWarnings("javadoc")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:META-INF/spring/space-namespace-test-context.xml" })
@DirtiesContext
public class NamespaceParsingSpringIntegrationTest {
    @Autowired
    SpaceConfiguration configuration;

    @Test
    public void parsedConfiguration() {
        assertThat( configuration.getConversionService(), is( notNullValue() ) );
        assertThat( configuration.getJChannel(), is( notNullValue() ) );
        assertThat( configuration.getKryo(), is( notNullValue() ) );
        assertThat( configuration.getGroup(), is( "tech-v" + SpaceUtility.projectVersion() + "-jspace" ) );
    }
}
