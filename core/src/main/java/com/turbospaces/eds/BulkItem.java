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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.turbospaces.api.SpaceOperation;

/**
 * bulk data item encapsulates bulk operation (type, data item).
 */
@SuppressWarnings("javadoc")
public class BulkItem implements Externalizable {
    private SpaceOperation operation;
    private Object data;

    public BulkItem() {
        super();
    }

    public BulkItem(final SpaceOperation operation, final Object data) {
        this.operation = operation;
        this.data = data;
    }

    public SpaceOperation getOperation() {
        return operation;
    }

    public Object getData() {
        return data;
    }

    /**
     * @return true if underlying entity was updated.
     */
    public boolean isUpdate() {
        return SpaceOperation.UPDATE == getOperation();
    }

    /**
     * @return true if underlying entity was deleted.
     */
    public boolean isDelete() {
        return SpaceOperation.TAKE == getOperation();
    }

    /**
     * @return true if underlying entity was inserted.
     */
    public boolean isInsert() {
        return SpaceOperation.WRITE == getOperation();
    }

    @Override
    public void writeExternal(final ObjectOutput out)
                                                     throws IOException {
        out.writeByte( getOperation().ordinal() );
        out.writeObject( getData() );
    }

    @Override
    public void readExternal(final ObjectInput in)
                                                  throws IOException,
                                                  ClassNotFoundException {
        operation = SpaceOperation.values()[in.readByte()];
        data = in.readObject();
    }
}
