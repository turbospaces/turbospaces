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
package com.turbospaces.runtime;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.turbospaces.core.Memory;

@SuppressWarnings("javadoc")
public class MemoryTest {

    @Test
    public void testGb() {
        assertEquals( Memory.kb( Long.SIZE ), (long) 64 * 1024 );
    }

    @Test
    public void testMb() {
        assertEquals( Memory.mb( Long.SIZE ), (long) 64 * 1024 * 1024 );
    }

    @Test
    public void testKb() {
        assertEquals( Memory.gb( Long.SIZE ), (long) 64 * 1024 * 1024 * 1024 );
    }

    @Test
    public void toKB() {
        assertEquals( Memory.toKb( 1024 ), 1 );
    }
}
