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
package com.turbospaces.namespace;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * class is responsible for parsing jspace configurations from spring xml configuration files.
 * 
 * @since 0.1
 */
public class SpaceNamespaceHanlder extends NamespaceHandlerSupport {

    @Override
    public void init() {
        registerBeanDefinitionParser( "jspace-cfg", new SpaceConfigurationBeanDefinitionParser( true ) );
        registerBeanDefinitionParser( "client-jspace-cfg", new SpaceConfigurationBeanDefinitionParser( false ) );
    }
}
