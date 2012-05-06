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
package com.elsecloud.spaces.tx;

import org.springframework.transaction.support.SmartTransactionObject;

/**
 * Space transaction object, representing a {@link SpaceTransactionHolder}.
 * Used as transactional object by {@link SpaceTransactionManager}.
 * 
 * @since 0.1
 */
class SpaceTransactionObject implements SmartTransactionObject {
    private SpaceTransactionHolder spaceTransactionHolder;

    SpaceTransactionHolder getSpaceTransactionHolder() {
        return spaceTransactionHolder;
    }

    void setSpaceTransactionHolder(final SpaceTransactionHolder spaceTransactionHolder) {
        this.spaceTransactionHolder = spaceTransactionHolder;
    }

    void setRollbackOnly() {
        spaceTransactionHolder.setRollbackOnly();
    }

    @Override
    public boolean isRollbackOnly() {
        return spaceTransactionHolder.isRollbackOnly();
    }

    @Override
    public void flush() {}
}
