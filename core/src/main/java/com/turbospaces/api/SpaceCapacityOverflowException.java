/**
 * Copyright (C) 2011-2012 Andrey Borisov <aandrey.borisov@gmail.com>
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

import org.springframework.dao.DataAccessException;

/**
 * signals that cache exceeded it's max capacity and it is not possible to add more elements.
 * sometimes you would like to restrict how many entities can be added to the space and you can do this by setting
 * {@link SpaceConfiguration#setCapacityRestriction(CapacityRestriction)} bean.
 * 
 * @see SpaceConfiguration#setCapacityRestriction(CapacityRestriction)
 * @since 0.1
 */
public class SpaceCapacityOverflowException extends DataAccessException {
    private static final long serialVersionUID = 7570197171497419919L;

    private final long maxElements;
    private final Object obj;

    /**
     * create new space overflow exception and specify space's max capacity and object that attempted to be added to the
     * space.
     * 
     * @param maxElements
     *            space's capacity
     * @param obj
     *            object that attempted to be added.
     */
    public SpaceCapacityOverflowException(final long maxElements, final Object obj) {
        super( String.format( "Space exceeded it's capacity %s, attempt to add %s rejected.", maxElements, obj ) );
        this.maxElements = maxElements;
        this.obj = obj;
    }

    /**
     * @return maximum element that can be added to the space.
     */
    public long getMaxElements() {
        return maxElements;
    }

    /**
     * @return object failed to be added to the space.
     */
    public Object getObj() {
        return obj;
    }
}
