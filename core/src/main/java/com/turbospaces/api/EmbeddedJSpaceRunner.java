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
package com.turbospaces.api;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.jgroups.Global;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.google.common.base.Throwables;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.logging.JGroupsCustomLoggerFactory;

/**
 * in-vm jspace runner.
 * 
 * @since 0.1
 */
public final class EmbeddedJSpaceRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger( EmbeddedJSpaceRunner.class );
    private static final Object joinNetworkMonitor = new Object();

    /**
     * launcher method
     * 
     * @param args
     *            [1 argument = application context path]
     * @throws Exception
     *             re-throw execution errors if any
     */
    public static void main(final String... args)
                                                 throws Exception {
        String appContextPath = args[0];
        if ( System.getProperty( Global.IPv4 ) == null && System.getProperty( Global.IPv6 ) == null )
            System.setProperty( Global.IPv4, Boolean.TRUE.toString() );
        System.setProperty( Global.CUSTOM_LOG_FACTORY, JGroupsCustomLoggerFactory.class.getName() );

        LOGGER.info( "Welcome to turbospaces:version = {}, build date = {}", SpaceUtility.projectVersion(), SpaceUtility.projecBuildTimestamp() );
        LOGGER.info( "{}: launching configuration {}", EmbeddedJSpaceRunner.class.getSimpleName(), appContextPath );
        AbstractXmlApplicationContext c = appContextPath.startsWith( "file" ) ? new FileSystemXmlApplicationContext( appContextPath )
                : new ClassPathXmlApplicationContext( appContextPath );
        c.getBeansOfType( JSpace.class );
        c.registerShutdownHook();
        Collection<SpaceConfiguration> configurations = c.getBeansOfType( SpaceConfiguration.class ).values();
        for ( SpaceConfiguration spaceConfiguration : configurations )
            spaceConfiguration.joinNetwork();
        LOGGER.info( "all jspaces joined network, notifying waiting threads..." );
        synchronized ( joinNetworkMonitor ) {
            joinNetworkMonitor.notifyAll();
        }

        while ( !Thread.currentThread().isInterrupted() )
            synchronized ( c ) {
                try {
                    c.wait( TimeUnit.SECONDS.toMillis( 1 ) );
                }
                catch ( InterruptedException e ) {
                    LOGGER.info( "got interruption signal, terminating jspaces... stack_trace = {}", Throwables.getStackTraceAsString( e ) );
                    Thread.currentThread().interrupt();
                }
            }

        c.destroy();
    }

    /**
     * await for network join event, useful for junit testing
     * 
     * @throws InterruptedException
     *             re-throw exception
     */
    public static void awaitNetworkJoin()
                                         throws InterruptedException {
        synchronized ( joinNetworkMonitor ) {
            joinNetworkMonitor.wait();
        }
    }

    private EmbeddedJSpaceRunner() {}
}
