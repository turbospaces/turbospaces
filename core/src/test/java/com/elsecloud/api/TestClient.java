package com.elsecloud.api;

import com.elsecloud.model.TestEntity1;

/**
 * for manual testing
 */
@SuppressWarnings("javadoc")
public class TestClient {

    public static void main(final String... args)
                                                 throws Exception {
        EmbeddedJSpaceRunner.main( TestEntity1.CLIENT_CONTEXT );
    }
}
