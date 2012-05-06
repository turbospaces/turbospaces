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
package com.elsecloud.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import com.elsecloud.api.SpaceOperation;
import com.elsecloud.eds.BulkItem;

@SuppressWarnings("javadoc")
public class BulkItemTest {

    @Test
    public void testInsert() {
        BulkItem bulkItem = new BulkItem( SpaceOperation.WRITE, getClass() );
        assertTrue( bulkItem.isInsert() );
        assertFalse( bulkItem.isDelete() );
        assertFalse( bulkItem.isUpdate() );
    }

    @Test
    public void testUpdate() {
        BulkItem bulkItem = new BulkItem( SpaceOperation.UPDATE, getClass() );
        assertTrue( bulkItem.isUpdate() );
        assertFalse( bulkItem.isInsert() );
        assertFalse( bulkItem.isDelete() );
    }

    @Test
    public void testDelete() {
        BulkItem bulkItem = new BulkItem( SpaceOperation.TAKE, getClass() );
        assertTrue( bulkItem.isDelete() );
        assertFalse( bulkItem.isInsert() );
        assertFalse( bulkItem.isUpdate() );
    }

    @Test
    public void canSerializedDeserialize()
                                          throws IOException,
                                          ClassNotFoundException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream stream = new ObjectOutputStream( outputStream );

        BulkItem bulkItem = new BulkItem( SpaceOperation.WRITE, getClass() );
        bulkItem.writeExternal( stream );

        ObjectInputStream inputStream = new ObjectInputStream( new ByteArrayInputStream( outputStream.toByteArray() ) );
        BulkItem another = new BulkItem();
        another.readExternal( inputStream );

        assertEquals( bulkItem.getOperation(), another.getOperation() );
        assertEquals( bulkItem.getData(), another.getData() );
    }
}
