package io.subutai.plugin.mysql.impl.handler;


import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.NodeGroup;
import io.subutai.common.environment.Topology;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.network.api.NetworkManager;
import io.subutai.core.peer.api.LocalPeer;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.NodeType;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.common.impl.PluginDataService;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;
import io.subutai.plugin.mysql.impl.ClusterConfig;
import io.subutai.plugin.mysql.impl.MySQLCImpl;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ClusterOperationHandlerTest
{
    //@formatter:off
    @Mock
    MySQLClusterConfig config;
    @Mock
    MySQLCImpl impl;
    @Mock
    Environment environment;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    Set<EnvironmentContainerHost> containerHosts;
    @Mock
    Set<String> mySet;
    @Mock
    CommandResult commandResult;
    @Mock
    ExecutorService executor;
    @Mock
    Monitor monitor;
    @Mock
    NetworkManager networkManager;
    @Mock
    PeerManager peerManager;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    QuotaManager quotaManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    PluginDataService dataService;
    @Mock
    ClusterConfig clusterConfig;
    @Mock
    Topology topology;
    @Mock
    LocalPeer localPeer;
    @Mock
    NodeGroup nodeGroup;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    EnvironmentContainerHost containerHost2;
    @Mock
    Iterator<EnvironmentContainerHost> iterator;
    @Mock
    Set<EnvironmentContainerHost> hostSet;
    private String clusterName;
    private String hostName;
    private String id;

    ClusterOperationHandler clusterOperationHandler;
    //@formatter:on


    @Before
    public void setUp() throws Exception
    {
        impl = new MySQLCImpl( monitor, pluginDAO );
        id = UUID.randomUUID().toString();
        impl.setEnvironmentManager( environmentManager );
        impl.setExecutor( executor );
        impl.setNetworkManager( networkManager );
        impl.setPeerManager( peerManager );
        impl.setPluginDAO( pluginDAO );
        impl.setQuotaManager( quotaManager );
        impl.setTracker( tracker );
        clusterName = "test";

        mySet.add( id );
        mySet.add( id );
        containerHosts.add( containerHost );
        containerHosts.add( containerHost2 );
        hostSet.add( containerHost );

        when( pluginDAO.saveInfo( MySQLClusterConfig.PRODUCT_KEY, clusterName, config ) ).thenReturn( true );

        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );

        when( config.getClusterName() ).thenReturn( clusterName );
        when( config.getEnvironmentId() ).thenReturn( id );

        when( iterator.hasNext() ).thenReturn( true ).thenReturn( true );
        when( iterator.next() ).thenReturn( containerHost ).thenReturn( containerHost );

        when( containerHosts.iterator() ).thenReturn( iterator );

        when( impl.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() ) ).thenReturn( environment );
        when( impl.getCluster( clusterName ) ).thenReturn( config );
        when( impl.getPeerManager().getLocalPeer() ).thenReturn( localPeer );
        when( environmentManager.growEnvironment( config.getEnvironmentId(), topology, false ) )
                .thenReturn( containerHosts );
    }


    @Test
    public void testSetUpCluster() throws Exception
    {

        clusterOperationHandler = new ClusterOperationHandler( impl, config, ClusterOperationType.INSTALL, null );
        clusterOperationHandler.run();

        verify( trackerOperation ).addLog( "Setting up cluster ..." );
    }


    @Test
    public void testStartCluster() throws Exception
    {
        clusterOperationHandler = new ClusterOperationHandler( impl, config, ClusterOperationType.START_ALL, null );
        clusterOperationHandler.run();
        verify( trackerOperation )
                .addLogDone( String.format( "MySQL Cluster %s", config.getClusterName() + " has been started" ) );
    }


    @Test
    public void testDestroyCluster() throws Exception
    {
        clusterOperationHandler = new ClusterOperationHandler( impl, config, ClusterOperationType.DESTROY, null );
        clusterOperationHandler.run();
        verify( trackerOperation ).addLogDone( "Cluster removed from database" );
    }


    @Test
    public void testStopCluster() throws Exception
    {
        clusterOperationHandler = new ClusterOperationHandler( impl, config, ClusterOperationType.STOP_ALL, null );
        clusterOperationHandler.run();
        verify( trackerOperation )
                .addLogDone( ( String.format( "MySQL Cluster %s", config.getClusterName() + " has been stopped" ) ) );
    }


    //@Test
    public void testAddDataNode()
    {
        clusterOperationHandler =
                new ClusterOperationHandler( impl, config, ClusterOperationType.ADD, NodeType.DATANODE );
        clusterOperationHandler.run();
        verify( trackerOperation ).addLog( "Node added" );
    }
}
