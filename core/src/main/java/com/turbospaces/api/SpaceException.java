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

import org.springframework.core.NestedRuntimeException;

/**
 * un-handled runtime exception reporting that there i something wrong with-in space(or with space configuration).
 * typically you wouldn't work with this class directly, rather you would interested in
 * {@link SpaceCapacityOverflowException} and {@link SpaceMemoryOverflowException} classes.
 * 
 * @since 0.1
 */
public class SpaceException extends NestedRuntimeException {
    private static final long serialVersionUID = 618436866438451880L;

    /**
     * Construct a <code>SpaceException</code> with the specified detail message.
     * 
     * @param msg
     *            the detail message
     */
    public SpaceException(final String msg) {
        super( msg );
    }

    /**
     * Construct a <code>SpaceException</code> with the specified detail message
     * and nested exception.
     * 
     * @param msg
     *            the detail message
     * @param cause
     *            the nested exception
     */
    public SpaceException(final String msg, final Throwable cause) {
        super( msg, cause );
    }
}
