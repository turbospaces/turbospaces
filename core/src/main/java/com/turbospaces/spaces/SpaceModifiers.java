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
package com.turbospaces.spaces;

import com.turbospaces.api.JSpace;

/**
 * utility class to work with space modifiers (parse, validate).
 * 
 * @since 0.1
 */
@SuppressWarnings("javadoc")
public abstract class SpaceModifiers {

    public static boolean isWriteOnly(final int modifier) {
        return ( modifier & JSpace.WRITE_ONLY ) != 0;
    }

    public static boolean isWriteOrUpdate(final int modifier) {
        return ( modifier & JSpace.WRITE_OR_UPDATE ) != 0;
    }

    public static boolean isUpdateOnly(final int modifier) {
        return ( modifier & JSpace.UPDATE_ONLY ) != 0;
    }

    public static boolean isEvictOnly(final int modifier) {
        return ( modifier & JSpace.EVICT_ONLY ) != 0;
    }

    public static boolean isReadOnly(final int modifier) {
        return ( modifier & JSpace.READ_ONLY ) != 0;
    }

    public static boolean isTakeOnly(final int modifier) {
        return ( modifier & JSpace.TAKE_ONLY ) != 0;
    }

    public static boolean isMatchById(final int modifier) {
        return ( modifier & JSpace.MATCH_BY_ID ) != 0;
    }

    public static boolean isExclusiveRead(final int modifier) {
        return ( modifier & JSpace.EXCLUSIVE_READ_LOCK ) != 0;
    }

    public static boolean isReturnAsBytes(final int modifier) {
        return ( modifier & JSpace.RETURN_AS_BYTES ) != 0;
    }

    private SpaceModifiers() {}
}
