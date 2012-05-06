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

import java.util.EventListener;
import java.util.Timer;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Jspace notification listener. You can subscribe for space entity modifications(writes/takes) and receive local/remote
 * events with this interface. This interface can be used to simulate JMS asynchronous behavior where publisher is just
 * space client(either remote/local really does matter) and receiver is again just space client(either local/remote)
 * listening for space write notifications.</p>
 * 
 * Concrete usage example would be: remote client send Entry[where jmsCorellationId=id], polling container(something
 * that periodically (via {@link ScheduledExecutorService} or {@link Timer}) takes entities by pattern) process messages
 * on server and publish response Entry[with original jmsCorrelationId], and finally client receive Entry[jms Message]
 * via notification listener.
 * 
 * @since 0.1
 */
public interface SpaceNotificationListener extends EventListener {

    /**
     * callback triggered when new/updated/deleted entity appears/disappears in/from space.
     * 
     * @param entity
     *            object which was updated/written/deleted in/from space
     * @param operation
     *            operation identifier
     */
    void handleNotification(Object entity,
                            SpaceOperation operation);
}
