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

import javax.annotation.concurrent.Immutable;

import com.turbospaces.api.SpaceNotificationListener;

/**
 * holder for space notifications (hold matching template, actual event listener and matching modifier).
 * this class needs to be considered as internal and should not be used by clients directly.
 * 
 * @since 0.1
 */
@Immutable
public class NotificationContext {
    private final SpaceNotificationListener listener;
    private final int modifier;
    private final CacheStoreEntryWrapper templateEntry;

    /**
     * create space notification context for given space template matching entity, listener and matching modifiers.
     * 
     * @param templateEntry
     * 
     * @param listener
     * @param modifier
     */
    public NotificationContext(final CacheStoreEntryWrapper templateEntry, final SpaceNotificationListener listener, final int modifier) {
        super();

        this.listener = listener;
        this.modifier = modifier;
        this.templateEntry = templateEntry;
    }

    /**
     * @return actual event listener
     */
    public SpaceNotificationListener getListener() {
        return listener;
    }

    /**
     * @return matching modifiers (match by id, or just by all fields)
     */
    public int getModifier() {
        return modifier;
    }

    /**
     * @return the templateEntry (matching object wrapper)
     */
    public CacheStoreEntryWrapper getTemplateEntry() {
        return templateEntry;
    }
}
