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
package com.turbospaces.eds;

import java.util.List;

/**
 * External data storage allows data to be persisted outside space in order to survive after application restart.
 * Typically this can be database (either relation or NOSQL).
 */
public interface ExternalDataStorage {
    /**
     * persist collection of data modifications(this can be either deletes, updates, inserts). </p>
     * interesting thing would be the order of entities in case they are flushed with transaction commit - yes, order i
     * guaranteed(entities will be passed in the same order as modified in space/cloud).
     * 
     * @param bulkItems
     *            data modifications
     */
    void persist(List<BulkItem> bulkItems);
}
