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

import org.springframework.data.annotation.Id;

/**
 * definition of errors which could be produced by {@link JSpace}.
 * 
 * @since 0.1
 */
public interface SpaceErrors {
    /**
     * indicates that primary key is not set before writing entry to {@link JSpace}.
     * 
     * @see Id
     */
    String ID_IS_NOT_SET = "ID field is not provided, you must set primary key before interactions with jspace";

    /**
     * indicates that it is not possible to find entity for update using provided primary key. also this mean that
     * {@link JSpace#UPDATE_ONLY} modifier is used.
     */
    String ENTITY_IS_MISSING_FOR_UPDATE = "unable to update enitity by primary key %s, there is no such entity and update_only modifier specified";

    /**
     * indicates that the primary key unique constraint violated. also this means that {@link JSpace#WRITE_ONLY}
     * modifier is used (typically you would prefer {@link JSpace#WRITE_OR_UPDATE} modifier, but just in case).
     */
    String DUPLICATE_KEY_VIOLATION = "primary key violation exception: %s[%s] already exists";

    /**
     * indicates that jspace is unable to acquire write/or either exclusive-read lock for particular key within specific
     * timeout in milliseconds. this means that lock is held by concurrent(parallel) transaction.
     */
    String UNABLE_TO_ACQUIRE_LOCK = "unable to acquire %s lock for %s within %s millisecs, lock is held by concurrent transaction";

    /**
     * indicates that the timeout for space operation can't be negative.
     */
    String NEGATIVE_TIMEOUT = "timeout can't be negative";

    /**
     * indicates that the maximum operations can't be <=1
     */
    String NON_POSITIVE_MAX_RESULTS = "maxResults must be >=1 ";

    /**
     * indicates that the ttl(time-to-live) can't be negative
     */
    String NEGATIVE_TTL = "time-to-live can't be negative";

    /**
     * indicates that the actual mapping context can't be resolved in spring context and is not provided
     */
    String MAPPING_CONTEXT_IS_NOT_REGISTERED = "MappingContext can't be resolved and is not registered, please set the mappingContext via setMappingContext method";

    /**
     * indicates that due to network issues or wrong client side configuration remote server nodes can't be lookup-up
     */
    String LOOKUP_TIMEOUT = "unable to lookup at least one server node within %s milliseconds";

    /**
     * indicates that jgroups unable to send message to remote server members
     */
    String REMOTE_SEND_MESSAGE_FAULT = "unable to send message to destinations %s";

    /**
     * indicates that kryo unable to determine actual type of the entity based on byte array passed during remote
     * communications. This can be caused because of mismatch of kryo configuration on server and client.
     */
    String CLASS_IS_NOT_REGISTERED = "Class with id = %s is not registere. Typically this is result of mismatch in client and server kryo configuration.";
}
