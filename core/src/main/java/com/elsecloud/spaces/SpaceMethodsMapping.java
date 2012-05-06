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
package com.elsecloud.spaces;

import com.elsecloud.offmemory.OffHeapCacheStore;

/**
 * RPC jgroups method mapping.
 * 
 * @since 0.1
 * @see OffHeapCacheStore
 * @see RemoteJSpace
 */
public enum SpaceMethodsMapping {
    /**
     * remote write method identifier
     */
    WRITE,
    /**
     * remote fetch method identifier
     */
    FETCH,
    /**
     * remote notify method identifier
     */
    NOTIFY,
    /**
     * remote size method identifier
     */
    SIZE,
    /**
     * remote size method identifier
     */
    MB_USED,
    /**
     * remote space topology method identifier
     */
    SPACE_TOPOLOGY,
    /**
     * start remote space transaction method identifier
     */
    BEGIN_TRANSACTION,
    /**
     * commit remote space transaction method identifier
     */
    COMMIT_TRANSACTION,
    /**
     * rollback remote space transaction method identifier
     */
    ROLLBACK_TRANSACTION;
}
