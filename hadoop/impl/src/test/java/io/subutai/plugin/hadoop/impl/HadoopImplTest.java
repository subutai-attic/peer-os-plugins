package io.subutai.plugin.hadoop.impl;


import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.collections.Sets;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.LocalPeer;
import io.subutai.common.peer.PeerException;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.strategy.api.StrategyManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class HadoopImplTest
{
    private static final String LOCAL_PEER_ID = UUID.randomUUID().toString();
    private HadoopImpl hadoopImpl;
    private String id;

    @Mock
    ExecutorService executorService;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Tracker tracker;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    Commands commands;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    Environment environment;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    Monitor monitor;

    @Mock
    HadoopWebModule webModule;

    @Mock
    EnvironmentContainerHost containerHost;

    @Mock
    PeerManager peerManager;

    @Mock
    LocalPeer localPeer;

    @Mock
    private StrategyManager strategyManager;


    @Before
    public void setUp() throws PeerException
    {
        //when(dataSource.getConnection()).thenReturn(connection);
        //when(connection.prepareStatement(any(String.class))).thenReturn(preparedStatement);
        //when(preparedStatement.executeQuery()).thenReturn(resultSet);
        //when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        //when(resultSetMetaData.getColumnCount()).thenReturn(1);
        //when(preparedStatement.executeUpdate()).thenReturn(5);

        when( localPeer.getId() ).thenReturn( LOCAL_PEER_ID );
        when( peerManager.getLocalPeer() ).thenReturn( localPeer );
        when( peerManager.getPeer(anyString()) ).thenReturn( localPeer );
        hadoopImpl = new HadoopImpl( strategyManager, monitor, pluginDAO, webModule );
        //        hadoopImpl.init();
        hadoopImpl.setPeerManager( peerManager );
        hadoopImpl.setExecutor( executorService );
        hadoopImpl.setTracker( tracker );
        hadoopImpl.setPluginDAO( pluginDAO );
        hadoopImpl.setEnvironmentManager( environmentManager );
        id = new UUID( 50, 50 ).toString();

        // mock ClusterOperationHandler
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( hadoopClusterConfig.getClusterName() ).thenReturn( "test" );

        when( hadoopClusterConfig.getEnvironmentId() ).thenReturn( id );

        when( hadoopClusterConfig.getAllNodes() ).thenReturn( Arrays.asList( id ) );

        when( environment.toString() ).thenReturn( "Environment" );
        when( environment.getId() ).thenReturn( id );
        when( pluginDAO.getInfo( HadoopClusterConfig.PRODUCT_KEY, HadoopClusterConfig.class ) )
                .thenReturn( Arrays.asList( hadoopClusterConfig ) );

        when( containerHost.getHostname() ).thenReturn( String.format( "host%d", 1 ) );
        //        when( containerHost.hashCode() ).thenReturn( 1, 1 );
        //        when( containerHost.equals( anyObject() ) ).thenReturn( false );
    }


    @Test
    public void testInit()
    {
        //hadoopImpl.init();
    }


    @Test
    public void testDestroy()
    {
        hadoopImpl.destroy();
    }


    @Test
    public void testGetTracker()
    {
        hadoopImpl.getTracker();

        // assertions
        assertEquals( tracker, hadoopImpl.getTracker() );
        assertNotNull( hadoopImpl.getTracker() );
    }


    @Test
    public void testSetTracker()
    {
        hadoopImpl.setTracker( tracker );

        // assertions
        assertEquals( tracker, hadoopImpl.getTracker() );
    }


    @Test
    public void testGetExecutor()
    {
        hadoopImpl.getExecutor();

        // assertions
        assertEquals( executorService, hadoopImpl.getExecutor() );
        assertNotNull( hadoopImpl.getExecutor() );
    }


    @Test
    public void testSetExecutor()
    {
        hadoopImpl.setExecutor( executorService );
        hadoopImpl.getExecutor();

        // assertions
        assertEquals( executorService, hadoopImpl.getExecutor() );
    }


    @Test
    public void testGetEnvironmentManager()
    {
        hadoopImpl.getEnvironmentManager();

        // assertions
        assertEquals( environmentManager, hadoopImpl.getEnvironmentManager() );
        assertNotNull( hadoopImpl.getEnvironmentManager() );
    }


    @Test
    public void testSetEnvironmentManager()
    {
        hadoopImpl.setEnvironmentManager( environmentManager );
        hadoopImpl.getEnvironmentManager();

        // assertions
        assertEquals( environmentManager, hadoopImpl.getEnvironmentManager() );
    }


    @Test
    public void testGetPluginDAO()
    {
        hadoopImpl.getPluginDAO();

        // assertions
        assertNotNull( hadoopImpl.getPluginDAO() );
    }


    @Test
    public void testInstallCluster()
    {
        hadoopImpl.installCluster( hadoopClusterConfig );

        assertNotNull( hadoopImpl.installCluster( hadoopClusterConfig ) );
    }


    @Test
    public void testUninstallCluster()
    {
        hadoopImpl.uninstallCluster( hadoopClusterConfig );

        assertNotNull( hadoopImpl.uninstallCluster( hadoopClusterConfig ) );
    }


    //    @Test
    //    public void testStartNameNode()
    //    {
    //        hadoopImpl.startNameNode( hadoopClusterConfig );
    //
    //        assertNotNull( hadoopImpl.startNameNode( hadoopClusterConfig ) );
    //    }


    //    @Test
    //    public void testStopNameNode()
    //    {
    //        hadoopImpl.stopNameNode( hadoopClusterConfig );
    //
    //        assertNotNull( hadoopImpl.stopNameNode( hadoopClusterConfig ) );
    //    }


    //    @Test
    //    public void testStatusNameNode()
    //    {
    //        hadoopImpl.statusNameNode( hadoopClusterConfig );
    //
    //        assertNotNull( hadoopImpl.statusNameNode( hadoopClusterConfig ) );
    //    }


    //    @Test
    //    public void testStatusSecondaryNameNode()
    //    {
    //        hadoopImpl.statusSecondaryNameNode( hadoopClusterConfig );
    //
    //        assertNotNull( hadoopImpl.statusSecondaryNameNode( hadoopClusterConfig ) );
    //    }


    @Test
    public void testStartDataNode()
    {
        String hostname = "test";
        hadoopImpl.startDataNode( hadoopClusterConfig, hostname );

        assertNotNull( hadoopImpl.startDataNode( hadoopClusterConfig, hostname ) );
    }


    @Test
    public void testStopDataNode()
    {
        String hostname = "test";
        hadoopImpl.stopDataNode( hadoopClusterConfig, hostname );

        assertNotNull( hadoopImpl.stopDataNode( hadoopClusterConfig, hostname ) );
    }


    @Ignore
    @Test
    public void testAddNode1()
    {
        String clusterName = "test";
        hadoopImpl.addNode( clusterName, 5 );

        assertNotNull( hadoopImpl.addNode( clusterName, 5 ) );
    }


    @Ignore
    @Test
    public void testDestroyNode()
    {
        String hostname = "test";
        hadoopImpl.destroyNode( hadoopClusterConfig, hostname );

        assertNotNull( hadoopImpl.destroyNode( hadoopClusterConfig, hostname ) );
    }


    @Test
    public void testExcludeNode()
    {
        String hostname = "test";
        hadoopImpl.excludeNode( hadoopClusterConfig, hostname );

        assertNotNull( hadoopImpl.excludeNode( hadoopClusterConfig, hostname ) );
    }


    @Test
    public void testIncludeNode()
    {
        String hostname = "test";
        hadoopImpl.includeNode( hadoopClusterConfig, hostname );

        assertNotNull( hadoopImpl.includeNode( hadoopClusterConfig, hostname ) );
    }


    @Test
    public void testGetClusters()
    {
        hadoopImpl.getClusters();

        // assertions
        assertNotNull( hadoopImpl.getClusters() );
    }


    @Test
    public void testGetCluster()
    {
        hadoopImpl.getCluster( "test" );
    }


    @Test
    public void testAddNode()
    {
        hadoopImpl.addNode( "test", "test" );
    }


    @Ignore
    @Test
    public void testUninstallCluster1()
    {
        hadoopImpl.uninstallCluster( "test" );
    }


    @Test
    public void testOnEnvironmentCreated() throws Exception
    {
        hadoopImpl.onEnvironmentCreated( environment );
    }


    @Test
    public void testOnEnvironmentGrown() throws Exception
    {
        hadoopImpl.onEnvironmentGrown( environment, Sets.newSet( containerHost ) );
        verify( containerHost ).getHostname();
    }


    @Test
    public void testOnContainerDestroyed() throws Exception
    {
        hadoopImpl.onContainerDestroyed( environment, id );
        verify( pluginDAO ).saveInfo( HadoopClusterConfig.PRODUCT_KEY, "test", hadoopClusterConfig );
    }


    @Test
    public void testOnEnvironmentDestroyed() throws Exception
    {
        hadoopImpl.onEnvironmentDestroyed( id );
        verify( pluginDAO ).deleteInfo( HadoopClusterConfig.PRODUCT_KEY, "test" );
    }
}