package io.subutai.plugin.storm.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.env.api.exception.EnvironmentDestructionException;
import io.subutai.core.peer.api.LocalPeer;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.PluginDAO;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.impl.StormImpl;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class StormClusterOperationHandlerTest
{
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
    @Mock
    LocalPeer localPeer;
    private StormClusterOperationHandler stormClusterOperationHandler;
    private StormClusterOperationHandler stormClusterOperationHandler2;
    private StormClusterOperationHandler stormClusterOperationHandler3;
    private StormClusterOperationHandler stormClusterOperationHandler4;
    private StormClusterOperationHandler stormClusterOperationHandler5;
    private StormClusterOperationHandler stormClusterOperationHandler6;
    private StormClusterOperationHandler stormClusterOperationHandler7;
    private UUID uuid;
    private Set<ContainerHost> mySet;
    private Set<UUID> myUUID;


    @Before
    public void setUp() throws Exception
    {
        uuid = UUID.randomUUID();
        mySet = new HashSet<>();
        mySet.add( containerHost );

        myUUID = new HashSet<>();
        myUUID.add( uuid );

        // mock constructor
        when( stormImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        stormClusterOperationHandler =
                new StormClusterOperationHandler( stormImpl, stormClusterConfiguration, ClusterOperationType.INSTALL );
        stormClusterOperationHandler2 = new StormClusterOperationHandler( stormImpl, stormClusterConfiguration,
                ClusterOperationType.UNINSTALL );
        stormClusterOperationHandler3 = new StormClusterOperationHandler( stormImpl, stormClusterConfiguration,
                ClusterOperationType.START_ALL );
        stormClusterOperationHandler4 =
                new StormClusterOperationHandler( stormImpl, stormClusterConfiguration, ClusterOperationType.STOP_ALL );
        stormClusterOperationHandler5 = new StormClusterOperationHandler( stormImpl, stormClusterConfiguration,
                ClusterOperationType.STATUS_ALL );
        stormClusterOperationHandler6 =
                new StormClusterOperationHandler( stormImpl, stormClusterConfiguration, ClusterOperationType.ADD );
        stormClusterOperationHandler7 =
                new StormClusterOperationHandler( stormImpl, stormClusterConfiguration, ClusterOperationType.REMOVE );

        when( stormImpl.getClusterSetupStrategy( stormClusterConfiguration, trackerOperation ) )
                .thenReturn( clusterSetupStrategy );
        when( stormImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( stormImpl.getZookeeperManager() ).thenReturn( zookeeper );
        //        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );
        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( uuid );
        when( zookeeperClusterConfig.getEnvironmentId() ).thenReturn( uuid );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( uuid );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( stormImpl.getPluginDAO() ).thenReturn( pluginDAO );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( stormImpl.getPeerManager() ).thenReturn( peerManager );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( mySet );
        when( peerManager.getLocalPeer() ).thenReturn( localPeer );
    }


    @Test
    public void testRunMalformedConfiguration() throws Exception
    {
        stormClusterOperationHandler.run();

        //assertions
        verify( trackerOperation ).addLogFailed( "Malformed configuration" );
    }


    @Test
    public void testRunClusterAlreadyExist() throws Exception
    {
        when( stormClusterConfiguration.getClusterName() ).thenReturn( "testClusterName" );
        when( stormImpl.getCluster( anyString() ) ).thenReturn( stormClusterConfiguration );

        stormClusterOperationHandler.run();

        //assertions
        verify( trackerOperation ).addLogFailed( String.format( "Cluster with name '%s' already exists", null ) );
    }


    @Test
    public void testRunFailedToSetupCluster() throws Exception
    {
        when( stormClusterConfiguration.getClusterName() ).thenReturn( "testClusterName" );
        when( clusterSetupStrategy.setup() ).thenThrow( ClusterSetupException.class );

        stormClusterOperationHandler.run();
    }


    @Test
    public void testRunOperationTypeInstall() throws Exception
    {
        when( stormClusterConfiguration.getClusterName() ).thenReturn( "testClusterName" );

        stormClusterOperationHandler.run();

        //assertions
        verify( trackerOperation ).addLogDone( String.format( "Cluster %s set up successfully", null ) );
    }


    @Test
    @Ignore
    public void testRunOperationTypeUninstallClusterDoesNotExist() throws Exception
    {
        stormClusterOperationHandler2.run();

        //assertions
        verify( trackerOperation )
                .addLogFailed( String.format( "Cluster with name %s does not exist. Operation aborted", null ) );
    }


    @Test
    @Ignore
    public void testRunOperationTypeUninstallErrorWhileDestroying() throws Exception
    {
        when( stormImpl.getCluster( anyString() ) ).thenReturn( stormClusterConfiguration );
        when( stormImpl.getEnvironmentManager() ).thenThrow( EnvironmentDestructionException.class );

        stormClusterOperationHandler2.run();
    }


    @Test
    public void testRunOperationTypeUninstallNoEnvironment() throws Exception
    {
        when( stormImpl.getCluster( anyString() ) ).thenReturn( stormClusterConfiguration );
        when( stormImpl.getEnvironmentManager() ).thenThrow( EnvironmentNotFoundException.class );

        stormClusterOperationHandler2.run();
    }


    @Test
    public void testRunOperationTypeUninstallNoEnvironment2() throws Exception
    {
        when( stormImpl.getCluster( anyString() ) ).thenReturn( stormClusterConfiguration );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenThrow( EnvironmentNotFoundException.class );
        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );

        stormClusterOperationHandler2.run();
    }


    @Test
    public void testRunOperationTypeUninstallContainerHostNotFound() throws Exception
    {
        when( stormImpl.getCluster( anyString() ) ).thenReturn( stormClusterConfiguration );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenThrow( ContainerHostNotFoundException.class );
        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );

        stormClusterOperationHandler2.run();
    }


    @Test
    @Ignore
    public void testRunOperationTypeUninstall() throws Exception
    {
        when( stormImpl.getCluster( anyString() ) ).thenReturn( stormClusterConfiguration );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );

        stormClusterOperationHandler2.run();

        // assertions
        verify( trackerOperation ).addLog( "Destroying environment..." );
        verify( trackerOperation ).addLogDone( "Cluster destroyed" );
    }


    @Test
    public void testRunOperationTypeStartAllNoEnvironment() throws Exception
    {
        when( stormImpl.getEnvironmentManager() ).thenThrow( EnvironmentNotFoundException.class );

        stormClusterOperationHandler3.run();
    }


    @Test
    public void testRunOperationTypeStartAll() throws Exception
    {
        when( containerHost.getId() ).thenReturn( uuid );

        stormClusterOperationHandler3.run();
    }


    @Test
    public void testRunOperationTypeStartAll2() throws Exception
    {
        when( stormClusterConfiguration.getNimbus() ).thenReturn( UUID.randomUUID() );
        when( containerHost.getId() ).thenReturn( uuid );
        when( stormClusterConfiguration.getSupervisors() ).thenReturn( myUUID );

        stormClusterOperationHandler3.run();
    }


    @Test
    public void testRunOperationTypeStopAllNoEnvironment() throws Exception
    {
        when( stormImpl.getEnvironmentManager() ).thenThrow( EnvironmentNotFoundException.class );

        stormClusterOperationHandler4.run();
    }


    @Test
    public void testRunOperationTypeStopAll() throws Exception
    {
        when( containerHost.getId() ).thenReturn( uuid );

        stormClusterOperationHandler4.run();
    }


    @Test
    public void testRunOperationTypeStopAll2() throws Exception
    {
        when( stormClusterConfiguration.getNimbus() ).thenReturn( UUID.randomUUID() );
        when( containerHost.getId() ).thenReturn( uuid );
        when( stormClusterConfiguration.getSupervisors() ).thenReturn( myUUID );

        stormClusterOperationHandler4.run();
    }


    @Test
    public void testRunOperationTypeStatusAllNoEnvironment() throws Exception
    {
        when( stormImpl.getEnvironmentManager() ).thenThrow( EnvironmentNotFoundException.class );

        stormClusterOperationHandler5.run();
    }


    @Test
    public void testRunOperationTypeStatusAll() throws Exception
    {
        when( containerHost.getId() ).thenReturn( uuid );

        stormClusterOperationHandler5.run();
    }


    @Test
    public void testRunOperationTypeStatusAll2() throws Exception
    {
        when( stormClusterConfiguration.getNimbus() ).thenReturn( UUID.randomUUID() );
        when( containerHost.getId() ).thenReturn( uuid );
        when( stormClusterConfiguration.getSupervisors() ).thenReturn( myUUID );

        stormClusterOperationHandler5.run();
    }


    @Test
    public void testRunOperationTypeAddNoEnvironment() throws Exception
    {
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenThrow( EnvironmentNotFoundException.class );

        stormClusterOperationHandler6.run();
    }


    @Test
    @Ignore
    public void testRunOperationTypeAdd() throws Exception
    {
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( containerHost.getIpByInterfaceName( "eth0" ) ).thenReturn( "test" );
        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );

        stormClusterOperationHandler6.run();
    }


    @Test
    @Ignore
    public void testRunOperationTypeAdd2() throws Exception
    {
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( false );
        when( containerHost.getIpByInterfaceName( "eth0" ) ).thenReturn( "test" );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( UUID.randomUUID() );
        when( zookeeper.getCluster( anyString() ) ).thenReturn( null );

        stormClusterOperationHandler6.run();

        // assertions
        verify( trackerOperation ).addLogDone( "Finished." );
    }
}