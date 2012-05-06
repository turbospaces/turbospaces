package com.turbospaces.network;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.nio.ByteBuffer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.turbospaces.api.ClientSpaceConfiguration;
import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.api.SpaceTopology;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.spaces.OffHeapJSpace;
import com.turbospaces.spaces.RemoteJSpace;
import com.turbospaces.spaces.SimplisticJSpace;
import com.turbospaces.spaces.tx.SpaceTransactionManager;

@SuppressWarnings("javadoc")
public class BasicCommunicationTest {
    static ClientSpaceConfiguration clientConfiguration;
    static SpaceConfiguration configuration;

    static SimplisticJSpace simplisticJSpace;
    static OffHeapJSpace offHeapJSpace;
    static RemoteJSpace remoteJSpace;

    @BeforeClass
    public static void beforeClass()
                                    throws Exception {
        configuration = TestEntity1.configurationFor();
        clientConfiguration = TestEntity1.clientConfigurationFor();

        offHeapJSpace = new OffHeapJSpace( configuration );
        offHeapJSpace.afterPropertiesSet();
        simplisticJSpace = new SimplisticJSpace( offHeapJSpace );
        simplisticJSpace.afterPropertiesSet();

        remoteJSpace = new RemoteJSpace( clientConfiguration );
        remoteJSpace.afterPropertiesSet();

        configuration.joinNetwork();
        clientConfiguration.joinNetwork();
    }

    @AfterClass
    public static void afterClass()
                                   throws Exception {
        simplisticJSpace.destroy();
        offHeapJSpace.destroy();
        remoteJSpace.destroy();

        clientConfiguration.destroy();
        configuration.destroy();
    }

    @Test
    public void canGetTopologySizeAndMbUsedForEmptyRemoteSychReplicatedSpace() {
        assertThat( clientConfiguration.getReceiever().getServerNodes(), is( notNullValue() ) );
        assertThat( remoteJSpace.getSpaceTopology(), is( SpaceTopology.SYNC_REPLICATED ) );
        assertThat( remoteJSpace.size(), is( 0L ) );
        assertThat( remoteJSpace.mbUsed(), is( 0 ) );
    }

    @Test
    public void canSeeChangesInRemoteServerNode() {
        for ( int i = 0; i < 100; i++ ) {
            TestEntity1 entity1 = new TestEntity1();
            entity1.afterPropertiesSet();
            simplisticJSpace.write( entity1 );
        }

        assertThat( remoteJSpace.size(), is( 100L ) );
    }

    @Test
    public void canWriteSingleNonTransactionEntityRemotely() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        remoteJSpace.write( entity1, JSpace.LEASE_FOREVER, 0, JSpace.WRITE_ONLY );
        Object[] resp = remoteJSpace.fetch( entity1, 0, 1, JSpace.MATCH_BY_ID );
        entity1.assertMatch( (TestEntity1) resp[0] );
        resp = remoteJSpace.fetch( entity1, 0, 1, JSpace.MATCH_BY_ID | JSpace.RETURN_AS_BYTES );
        entity1.assertMatch( (TestEntity1) remoteJSpace
                .getSpaceConfiguration()
                .getEntitySerializer()
                .deserialize( (ByteBuffer) resp[0], TestEntity1.class )
                .getObject() );
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void canDoSomethingTransactionally() {
        final TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        SpaceTransactionManager remoteTxManager = new SpaceTransactionManager();
        remoteTxManager.setjSpace( remoteJSpace );
        TransactionTemplate transactionTemplate = new TransactionTemplate( remoteTxManager );
        transactionTemplate.setTimeout( 10000 );
        transactionTemplate.execute( new TransactionCallback() {
            @Override
            public Object doInTransaction(final TransactionStatus status) {
                remoteJSpace.write( entity1, JSpace.LEASE_FOREVER, 0, JSpace.WRITE_ONLY );
                Object[] fetch = remoteJSpace.fetch( entity1, 0, 1, JSpace.MATCH_BY_ID );
                ( (TestEntity1) fetch[0] ).assertMatch( entity1 );
                status.setRollbackOnly();
                return null;
            }
        } );
        Object[] fetch = remoteJSpace.fetch( entity1, 0, 1, JSpace.MATCH_BY_ID );
        assertThat( fetch.length, is( 0 ) );
    }
}
