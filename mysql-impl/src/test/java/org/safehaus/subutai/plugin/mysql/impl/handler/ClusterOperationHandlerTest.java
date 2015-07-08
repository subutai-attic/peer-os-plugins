package org.safehaus.subutai.plugin.mysql.impl.handler;


import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.NodeGroup;
import org.safehaus.subutai.common.environment.Topology;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.lxc.quota.api.QuotaManager;
import org.safehaus.subutai.core.metric.api.Monitor;
import org.safehaus.subutai.core.network.api.NetworkManager;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.core.peer.api.PeerManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.common.impl.PluginDataService;
import org.safehaus.subutai.plugin.mysql.impl.ClusterConfig;
import org.safehaus.subutai.plugin.mysql.impl.MySQLCImpl;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLClusterConfig;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Created by tkila on 6/3/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class ClusterOperationHandlerTest
{
    //@formatter:off
    @Mock MySQLClusterConfig config;
    @Mock MySQLCImpl         impl;
    @Mock Environment        environment;
    @Mock Tracker            tracker;
    @Mock EnvironmentManager environmentManager;
    @Mock Set<ContainerHost> containerHosts;
    @Mock Set<UUID>          mySet;
    @Mock CommandResult      commandResult;
    @Mock ExecutorService    executor;
    @Mock Monitor            monitor;
    @Mock NetworkManager     networkManager;
    @Mock PeerManager        peerManager;
    @Mock PluginDAO          pluginDAO;
    @Mock QuotaManager       quotaManager;
    @Mock TrackerOperation   trackerOperation;
    @Mock PluginDataService  dataService;
    @Mock ClusterConfig      clusterConfig;
    @Mock Topology           topology;
    @Mock LocalPeer          localPeer;
    @Mock NodeGroup          nodeGroup;
    @Mock ContainerHost      containerHost;
    @Mock ContainerHost      containerHost2;
    @Mock Iterator<ContainerHost>  iterator;
    @Mock Set<ContainerHost> hostSet;
    private String           clusterName;
    private String           hostName;
    private UUID             uuid;

    ClusterOperationHandler  clusterOperationHandler;
    //@formatter:on


    @Before
    public void setUp() throws Exception
    {
        impl = new MySQLCImpl( monitor );
        uuid = UUID.randomUUID();
        impl.setEnvironmentManager( environmentManager );
        impl.setExecutor( executor );
        impl.setNetworkManager( networkManager );
        impl.setPeerManager( peerManager );
        impl.setPluginDAO( pluginDAO );
        impl.setQuotaManager( quotaManager );
        impl.setTracker( tracker );
        clusterName = "test";

        mySet.add( uuid );
        mySet.add( uuid );
        containerHosts.add( containerHost );
        containerHosts.add( containerHost2 );
        hostSet.add( containerHost );

        when( pluginDAO.saveInfo( MySQLClusterConfig.PRODUCT_KEY, clusterName, config ) ).thenReturn( true );

        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );

        when( config.getClusterName() ).thenReturn( clusterName );
        when( config.getEnvironmentId() ).thenReturn( uuid );

        when(iterator.hasNext()).thenReturn( true ).thenReturn( true );
        when(iterator.next()).thenReturn( containerHost ).thenReturn( containerHost );

        when(containerHosts.iterator()).thenReturn( iterator );

        when( impl.getEnvironmentManager().findEnvironment( config.getEnvironmentId() ) ).thenReturn( environment );
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
