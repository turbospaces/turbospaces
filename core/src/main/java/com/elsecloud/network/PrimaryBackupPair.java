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
package com.elsecloud.network;

import org.jgroups.Address;

/**
 * in case on partitioned space and it order to guarantee failover, concept of primary-backup nodes has been introduced.
 */
public class PrimaryBackupPair {
    private final Address primaryNode;
    private Address backupNode;

    /**
     * construct new instance for given primary and backup nodes.
     * 
     * @param primaryNode
     * @param backupNode
     */
    public PrimaryBackupPair(final Address primaryNode, final Address backupNode) {
        super();
        this.primaryNode = primaryNode;
        this.backupNode = backupNode;
    }

    /**
     * construct primary-backup pair just for primary node
     * 
     * @param primaryNode
     */
    public PrimaryBackupPair(final Address primaryNode) {
        super();
        this.primaryNode = primaryNode;
    }

    /**
     * @return the primary node
     */
    public Address getPrimaryNode() {
        return primaryNode;
    }

    /**
     * @return the backup node
     */
    public Address getBackupNode() {
        return backupNode;
    }

    /**
     * @return true if the backup node associated with primary node
     */
    public boolean hasBackup() {
        return getBackupNode() != null;
    }
}
