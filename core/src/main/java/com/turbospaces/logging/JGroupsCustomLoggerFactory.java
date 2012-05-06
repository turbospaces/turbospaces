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
package com.turbospaces.logging;

import org.jgroups.logging.CustomLogFactory;
import org.jgroups.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * slf4j jgroups logging proxy
 * 
 * @since 0.1
 */
public class JGroupsCustomLoggerFactory implements CustomLogFactory {
    @Override
    @SuppressWarnings("rawtypes")
    public Log getLog(final Class clazz) {
        Logger logger = LoggerFactory.getLogger( clazz );
        return new JGroupsCustomLog( logger );
    }

    @Override
    public Log getLog(final String category) {
        Logger logger = LoggerFactory.getLogger( category );
        return new JGroupsCustomLog( logger );
    }
}
