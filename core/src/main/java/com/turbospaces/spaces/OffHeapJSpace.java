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
package com.turbospaces.spaces;

import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.eds.ExternalDataStorage;

/**
 * There are 3 possible types of Javaspace implementations:
 * <ul>
 * <li>local space - is the space where the code is currently running, it is local/embedded space. it does not know any
 * other members and all operation will be performed only on this cluster node regardless of deployment topology.
 * Technically if you will try to read the entity by ID which is not available on this cluster node, you will never
 * broadcast to other nodes. Important consideration is that behavior of course depends on deployment topology, in case
 * of data replication all data can be fetched from local node, for partitioned scenarios the story can change.</li>
 * 
 * <li>local clustered space - this is almost the same as 'local space', but the fundamental difference is that this
 * space has references to other cluster members and can perform cluster-wide operations. Technically if you are trying
 * to read entity by ID which is not available on this cluster node, you will broadcast to other nodes and fetch data
 * from remote cluster nodes.</li>
 * 
 * <li>remote space - actually just the same API for remote clients, but in-fact is just proxy to backend.</li>
 * </ul>
 * 
 * This one is in fact local space.
 * 
 * @since 0.1
 */
public class OffHeapJSpace extends AbstractJSpace {
    private ExternalDataStorage dataStorage;

    /**
     * create off-heap java space over configuration
     * 
     * @param configuration
     *            server side space configuration
     */
    public OffHeapJSpace(final SpaceConfiguration configuration) {
        super( configuration );
    }

    public void setDataStorage(final ExternalDataStorage dataStorage) {
        this.dataStorage = dataStorage;
    }
}
