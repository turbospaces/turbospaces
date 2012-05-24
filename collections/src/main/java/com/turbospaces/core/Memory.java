/**
 * Copyright (C) 2011-2012 Andrey Borisov <aandrey.borisov@gmail.com>
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
package com.turbospaces.core;

/**
 * utility class representing memory
 * 
 * @since 0.1
 */
@SuppressWarnings("javadoc")
public abstract class Memory {
    public static long gb(final double gigabytes) {
        return (long) gigabytes * 1024 * 1024 * 1024;
    }

    public static long mb(final double megabytes) {
        return (long) megabytes * 1024 * 1024;
    }

    public static long kb(final double kilobytes) {
        return (long) kilobytes * 1024;
    }

    public static long toKb(final long bytes) {
        return bytes / 1024;
    }

    public static int toMb(final long bytes) {
        return (int) ( bytes / ( 1024 * 1024 ) );
    }

    private Memory() {}
}
