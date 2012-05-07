package com.turbospaces.api;

import org.junit.Test;

import com.turbospaces.model.TestEntity1;

@SuppressWarnings("javadoc")
public class EmbeddedJSpaceRunnerTest {

    @Test
    public void test()
                      throws Exception {
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    TestEntity1.main();
                }
                catch ( Exception e ) {}
            }
        };
        thread.start();
        EmbeddedJSpaceRunner.awaitNetworkJoin();
        Thread.sleep( 10 );
        thread.interrupt();
        thread.join();
    }
}
