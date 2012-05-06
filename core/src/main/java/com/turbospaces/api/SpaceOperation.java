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

/**
 * space operations enumeration definition. basically you can perform write(which is insert), update,
 * writeOrUpdate(merge), take(delete) or exclusiveRead(which is typically missing in RDBMS systems) operation with
 * JSpace.
 * 
 * @since 0.1
 */
public enum SpaceOperation {
    /**
     * RDBMS insert equivalent
     */
    WRITE,
    /**
     * RDBMS update equivalent
     */
    UPDATE,
    /**
     * RDBMS merge equivalent
     */
    WRITE_OR_UPDATE,
    /**
     * RDBMS take equivalent
     */
    TAKE,
    /**
     * synthetic operation - something that is not present in most databases. allows to get exclusive lock in such way
     * that no other concurrent readers able to read by the same id/template.
     */
    EXCLUSIVE_READ_LOCK
}
