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
package com.turbospaces.serialization;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.SerializationException;
import com.turbospaces.spaces.CacheStoreEntryWrapper;

/**
 * abstraction around serialization process. currently preferable implementation is kryo serialization framework, but is
 * not limited to be only kryo(other considerable framework is google protobuff).
 * 
 * @since 0.1
 * @see DefaultEntitySerializer
 */
public interface EntitySerializer {

    /**
     * serialize the entitie's state into byte array for further off-memory persistence. The position of newly created
     * direct memory is set to zero.
     * 
     * @param cacheEntry
     * @param objectBuffer
     * @return byte array representation of the entity
     * 
     * @throws SerializationException
     *             IO exception if entity can't be serialized (direct memory can't be allocated)
     */
    byte[] serialize(CacheStoreEntryWrapper cacheEntry,
                     ObjectBuffer objectBuffer)
                                               throws SerializationException;

    /**
     * de-serialize the entitie's state from byte array to POJO state without changing position/limit/mark or any other
     * attributes of byte buffer.
     * 
     * @param source
     *            byte array representation of entity (byte buffer)
     * @param clazz
     *            persistent class
     * @return POJO state
     * @throws SerializationException
     *             IO exception if entity can't be de-serialized (for example due to file corruption or any other IO
     *             problems)
     */
    SerializationEntry deserialize(ByteBuffer source,
                                   Class<?> clazz)
                                                  throws SerializationException;

    /**
     * perform find-by-example operation over serialized object's byte array storage and given template class without
     * changing byte buffer's position/limit or any other attributes.
     * 
     * @param source
     *            byte array representation (byte buffer array)
     * @param cacheEntryTemplate
     *            find-by-example template
     * 
     * @return true if de-serialized state of the entity matched by template
     * @throws SerializationException
     *             if for some reason source's state can't be matched with template
     */
    boolean matchByTemplate(ByteBuffer source,
                            CacheStoreEntryWrapper cacheEntryTemplate)
                                                                      throws SerializationException;
}
