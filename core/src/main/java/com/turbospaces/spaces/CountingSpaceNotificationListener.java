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

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.turbospaces.api.SpaceNotificationListener;
import com.turbospaces.api.SpaceOperation;

/**
 * simple counting space notification listener, track writes/updates/takes separately.
 * useful for unit/integration testing only.
 * 
 * @since 0.1
 */
public class CountingSpaceNotificationListener implements SpaceNotificationListener {
    private final Set<Object> writes = new HashSet<Object>();
    private final Set<Object> changes = new HashSet<Object>();
    private final Set<Object> takes = new HashSet<Object>();

    @Override
    public void handleNotification(final Object entity,
                                   final SpaceOperation operation) {
        Preconditions.checkNotNull( entity );
        switch ( operation ) {
            case WRITE:
                writes.add( entity );
                break;
            case UPDATE:
                changes.add( entity );
                break;
            case TAKE:
                takes.add( entity );
                break;
            case EXCLUSIVE_READ_LOCK:
            case WRITE_OR_UPDATE:
                break;
        }
    }

    /**
     * @return collection of written objects.
     */
    public Set<Object> getWrites() {
        return writes;
    }

    /**
     * @return collection of modified objects.
     */
    public Set<Object> getChanges() {
        return changes;
    }

    /**
     * @return collection of deleted objects.
     */
    public Set<Object> getTakes() {
        return takes;
    }
}
