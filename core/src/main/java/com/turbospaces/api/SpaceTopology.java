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

import org.springframework.data.annotation.Routing;

/**
 * definition of space deployment topologies.
 * 
 * @since 0.1
 */
public enum SpaceTopology {
    /**
     * the data is synchronously replicated between nodes upon transactions commit.
     * this topology guarantee data consistency between cluster nodes, however has some performance overhead for
     * data synchronization and replication. typically this one can be used for some kind of dictionaries or data
     * with low rate of updates(and does not require data partitioning because the size of data is not so big).</p>
     * 
     * Also <b>NOTE:</b> that from turbospaces experience typical application will have few spaces defined with
     * different deployment topologies (sync_repl and partitioned) where partitioned space is placeholder for 'big data'
     * and replicated is placeholder for reference data source(dictionaries).
     */
    SYNC_REPLICATED,
    /**
     * the data is partitioned between multiple cluster node according to {@link Routing} annotation and each node
     * contains unique subset of data. this is the most commonly used topology.
     */
    PARTITIONED,
    /**
     * remote client's local space is mechanism for caching data on client side rather than server side.
     */
    CLIENT_LOCAL;

    /**
     * @return true if space deployment topology is {@link #PARTITIONED}
     */
    public boolean isPartitioned() {
        return this == PARTITIONED;
    }

    /**
     * @return true if space deployment topology is {@link #SYNC_REPLICATED}
     */
    public boolean isSynchReplicated() {
        return this == SYNC_REPLICATED;
    }
}
