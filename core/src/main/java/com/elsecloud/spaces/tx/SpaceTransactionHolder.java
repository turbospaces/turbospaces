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

import org.springframework.transaction.support.ResourceHolderSupport;

/**
 * space transaction modification resource holder. used with spring transactions abstraction layer.
 * 
 * @since 0.1
 */
public class SpaceTransactionHolder extends ResourceHolderSupport {
    private Object modificationContext;

    /**
     * associate transaction modification object with space transaction holder.
     * 
     * @param modificationContext
     */
    public void setModificationContext(final Object modificationContext) {
        this.modificationContext = modificationContext;
    }

    /**
     * @return transaction modification context associated with the current transaction.
     */
    public Object getModificationContext() {
        return modificationContext;
    }
}
