package org.safehaus.subutai.plugin.storm.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.peer.api.PeerManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.storm.impl.StormImpl;
import org.safehaus.subutai.plugin.zookeeper.api.CommandType;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class StormNodeOperationHandlerTest
{
    private StormNodeOperationHandler stormNodeOperationHandler;
    private StormNodeOperationHandler stormNodeOperationHandler2;
    private StormNodeOperationHandler stormNodeOperationHandler3;
    private StormNodeOperationHandler stormNodeOperationHandler4;
    private UUID uuid;
    private Set<ContainerHost> mySet;
    private Set<UUID> myUUID;
    @Mock
    CommandResult commandResult;
    @Mock
    ContainerHost containerHost;
    @Mock
    StormImpl stormImpl;
    @Mock
    StormClusterConfiguration stormClusterConfiguration;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    Zookeeper zookeeper;
    @Mock
    ZookeeperClusterConfig zookeeperClusterConfig;
    @Mock
    PeerManager peerManager;


    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID( 50, 50 );
        mySet = new HashSet<>();
        mySet.add( containerHost );

        myUUID = new HashSet<>();
        myUUID.add( uuid );

        // mock constructor
        when( stormImpl.getCluster( "testClusterName" ) ).thenReturn( stormClusterConfiguration );
        when( stormImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        stormNodeOperationHandler =
                new StormNodeOperationHandler( stormImpl, "testClusterName", "testHostName", NodeOperationType.START );
        stormNodeOperationHandler2 =
                new StormNodeOperationHandler( stormImpl, "testClusterName", "testHostName", NodeOperationType.STOP );
        stormNodeOperationHandler3 =
                new StormNodeOperationHandler( stormImpl, "testClusterName", "testHostName", NodeOperationType.STATUS );
        stormNodeOperationHandler4 =
                new StormNodeOperationHandler( stormImpl, "testClusterName", "testHostName", NodeOperationType.DESTROY );

        // mock run method
        when( stormImpl.getCluster( anyString() ) ).thenReturn( stormClusterConfiguration );
        when( stormImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( uuid );
        when( stormImpl.getZookeeperManager() ).thenReturn( zookeeper );
        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( uuid );
        when( containerHost.getId() ).thenReturn( uuid );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( zookeeper.getCommand( any( CommandType.class) ) ).thenReturn( "testCommand" );
    }


    @Test
    public void testRunClusterDoesNotExist() throws Exception
    {
        when( stormImpl.getCluster( anyString() ) ).thenReturn( null );
        stormNodeOperationHandler.run();

        // assertions
        verify( trackerOperation )
                .addLogFailed( String.format( "Cluster with name %s does not exist", "testClusterName" ) );
    }


    @Test
    public void testRunNoEnvironment() throws Exception
    {
        when( stormImpl.getEnvironmentManager() ).thenThrow( EnvironmentNotFoundException.class );

        stormNodeOperationHandler.run();
    }


    @Test
    public void testRunErrorGettingContainer() throws Exception
    {
        when( environment.getContainerHostByHostname( anyString() ) ).thenThrow( ContainerHostNotFoundException.class );

        stormNodeOperationHandler.run();
    }


    @Test
    public void testRunExternalEnvironmentNoContainer() throws Exception
    {
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( null );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );

        stormNodeOperationHandler.run();

        // assertions
        verify( trackerOperation ).addLogFailed( String.format( "No Container with ID %s", "testHostName" ) );
    }


    @Test
    public void testRunOperationTypeStart() throws Exception
    {
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( null );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );

        stormNodeOperationHandler.run();
    }


    @Test
    public void testRunOperationTypeStart2() throws Exception
    {
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( null );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( UUID.randomUUID() );
        when( stormClusterConfiguration.getSupervisors() ).thenReturn( myUUID );

        stormNodeOperationHandler.run();
    }



    @Test
    public void testRunOperationTypeStop() throws Exception
    {
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( null );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );

        stormNodeOperationHandler2.run();
    }


    @Test
    public void testRunOperationTypeStop2() throws Exception
    {
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( null );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( UUID.randomUUID() );
        when( stormClusterConfiguration.getSupervisors() ).thenReturn( myUUID );

        stormNodeOperationHandler2.run();
    }


    @Test
    public void testRunOperationTypeStatus() throws Exception
    {
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( null );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );

        stormNodeOperationHandler3.run();
    }


    @Test
    public void testRunOperationTypeStatus2() throws Exception
    {
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( null );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( UUID.randomUUID() );
        when( stormClusterConfiguration.getSupervisors() ).thenReturn( myUUID );

        stormNodeOperationHandler3.run();
    }


    @Test
    public void testRunOperationTypeDestroy() throws ContainerHostNotFoundException
    {
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( stormImpl.getPluginDAO() ).thenReturn( pluginDAO );
        stormNodeOperationHandler4.run();

        // assertions
        verify( trackerOperation ).addLog( "Destroying " + "testHostName" + " node." );
        verify( trackerOperation ).addLogDone( "Container " + "testHostName" + " is destroyed!" );
    }

    @Test
    public void testDestroyNode() throws Exception
    {

    }


    @Test
    public void testLogResults() throws Exception
    {

    }
}