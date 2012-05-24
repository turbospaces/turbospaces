package com.turbospaces.collections;

import org.junit.Test;

/**
 * simple guava test class which allows to ensure that there is no unexpected issues(NoClassDefFound or any other
 * dependency issues).</p>
 * 
 * in theory we need only kryo library and guava of course - that's all, lets check this statement.
 * 
 * @since 0.1
 */
@SuppressWarnings("javadoc")
public class BasicGuavaCacheTest {

    @Test
    public void showBasicUsage() {
        GuavaOffHeapCacheBuilder<String, String> a;
    }
}
