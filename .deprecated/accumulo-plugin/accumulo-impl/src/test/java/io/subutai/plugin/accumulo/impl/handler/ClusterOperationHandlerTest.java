package io.subutai.plugin.accumulo.impl.handler;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;
import io.subutai.plugin.accumulo.impl.Commands;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@Ignore
@RunWith( MockitoJUnitRunner.class )
public class ClusterOperationHandlerTest
{
    @Mock
    AccumuloImpl accumuloImpl;
    @Mock
    AccumuloClusterConfig accumuloClusterConfig;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    CommandResult commandResult;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    ZookeeperClusterConfig zookeeperClusterConfig;
    @Mock
    Hadoop hadoop;
    @Mock
    Zookeeper zookeeper;
    @Mock
    PluginDAO pluginDAO;
    private ClusterOperationHandler clusterOperationHandler;
    private ClusterOperationHandler clusterOperationHandler2;
    private ClusterOperationHandler clusterOperationHandler3;
    private ClusterOperationHandler clusterOperationHandler4;
    private ClusterOperationHandler clusterOperationHandler5;
    private String id;


    @Before
    public void setUp() throws CommandException, EnvironmentNotFoundException, ContainerHostNotFoundException
    {
        // mock constructor
        id = UUID.randomUUID().toString();
        when( accumuloImpl.getCluster( anyString() ) ).thenReturn( accumuloClusterConfig );
        when( accumuloImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );

        // mock runOperationOnContainers method
        when( accumuloImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );

        clusterOperationHandler = new ClusterOperationHandler( accumuloImpl, accumuloClusterConfig, hadoopClusterConfig,
                zookeeperClusterConfig, ClusterOperationType.INSTALL );
        clusterOperationHandler2 =
                new ClusterOperationHandler( accumuloImpl, accumuloClusterConfig, hadoopClusterConfig,
                        zookeeperClusterConfig, ClusterOperationType.UNINSTALL );
        clusterOperationHandler3 =
                new ClusterOperationHandler( accumuloImpl, accumuloClusterConfig, hadoopClusterConfig,
                        zookeeperClusterConfig, ClusterOperationType.START_ALL );
        clusterOperationHandler4 =
                new ClusterOperationHandler( accumuloImpl, accumuloClusterConfig, hadoopClusterConfig,
                        zookeeperClusterConfig, ClusterOperationType.STOP_ALL );
        clusterOperationHandler5 =
                new ClusterOperationHandler( accumuloImpl, accumuloClusterConfig, hadoopClusterConfig,
                        zookeeperClusterConfig, ClusterOperationType.STATUS_ALL );

        // mock setup method
        when( accumuloClusterConfig.getMasterNode() ).thenReturn( UUID.randomUUID().toString() );
        when( accumuloClusterConfig.getGcNode() ).thenReturn( UUID.randomUUID().toString() );
        when( accumuloClusterConfig.getMonitor() ).thenReturn( UUID.randomUUID().toString() );
        when( accumuloClusterConfig.getClusterName() ).thenReturn( "test-cluster" );
        when( accumuloClusterConfig.getInstanceName() ).thenReturn( "test-instance" );
        when( accumuloClusterConfig.getPassword() ).thenReturn( "test-password" );
        when( accumuloImpl.getHadoopManager() ).thenReturn( hadoop );


        List<String> myList = new ArrayList<>();
        myList.add( id );

        Set<String> myUUID = new HashSet<>();
        myUUID.add( id );
        when( accumuloClusterConfig.getTracers() ).thenReturn( myUUID );
        when( accumuloClusterConfig.getSlaves() ).thenReturn( myUUID );

        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( accumuloImpl.getZkManager() ).thenReturn( zookeeper );
        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );
        when( zookeeperClusterConfig.getNodes() ).thenReturn( myUUID );
        when( zookeeperClusterConfig.getClusterName() ).thenReturn( "testClusterName" );
        when( containerHost.getHostname() ).thenReturn( "testHostName" );
        when( zookeeper.startNode( anyString(), anyString() ) ).thenReturn( UUID.randomUUID() );

        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( accumuloClusterConfig.getAllNodes() ).thenReturn( myUUID );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( commandResult.getStdOut() ).thenReturn( "Hadoop install ok installed" );


        // mock clusterConfiguration
        when( accumuloImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( zookeeperClusterConfig.getEnvironmentId() ).thenReturn( id );
        when( environmentManager.loadEnvironment( id ) ).thenReturn( environment );
        when( accumuloImpl.getPluginDAO() ).thenReturn( pluginDAO );
        when( accumuloImpl.getCluster( anyString() ) ).thenReturn( null );

        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( environment.getContainerHosts() ).thenReturn( mySet );
    }


    @Test
    public void testRunWithClusterOperationTypeInstall() throws CommandException, EnvironmentNotFoundException
    {
        //clusterOperationHandler.run();

        // assertions
        assertEquals( environment, accumuloImpl.getEnvironmentManager().loadEnvironment( any( String.class ) ) );
        //verify( trackerOperation ).addLogDone( "Accumulo cluster data saved into database" );
    }


    @Test
    public void testRunWithClusterOperationTypeUninstall() throws CommandException
    {
        Set<String> mySet = new HashSet<>();
        mySet.add( id );
        when( accumuloImpl.getCluster( anyString() ) ).thenReturn( accumuloClusterConfig );
        when( accumuloClusterConfig.getAllNodes() ).thenReturn( mySet );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( accumuloImpl.getPluginDAO() ).thenReturn( pluginDAO );

        clusterOperationHandler2.run();

        // assertions
        assertNotNull( accumuloImpl.getCluster( anyString() ) );
        //verify( containerHost ).execute( new RequestBuilder(
        //      Commands.uninstallCommand + Common.PACKAGE_PREFIX + AccumuloClusterConfig.PRODUCT_KEY.toLowerCase() ) );
        assertTrue( commandResult.hasSucceeded() );
        verify( trackerOperation ).addLog(
                AccumuloClusterConfig.PRODUCT_KEY + " is uninstalled from node " + containerHost.getHostname()
                        + " successfully." );
        verify( trackerOperation ).addLog( AccumuloClusterConfig.PRODUCT_KEY + " cluster info removed from HDFS." );
        verify( accumuloImpl ).getPluginDAO();
    }


    @Test
    public void testRunWithClusterOperationTypeStartAll() throws CommandException, EnvironmentNotFoundException
    {
        clusterOperationHandler3.run();

        // assertions
        assertEquals( environment, accumuloImpl.getEnvironmentManager().loadEnvironment( any( String.class ) ) );
        assertTrue( commandResult.hasSucceeded() );
    }


    @Test
    public void testRunWithClusterOperationTypeStopAll() throws EnvironmentNotFoundException
    {
        clusterOperationHandler4.run();

        // assertions
        assertEquals( environment, accumuloImpl.getEnvironmentManager().loadEnvironment( any( String.class ) ) );
        assertTrue( commandResult.hasSucceeded() );
    }


    @Test
    public void testRunWithClusterOperationTypeStatusAll() throws EnvironmentNotFoundException
    {

        clusterOperationHandler5.run();

        // assertions
        assertEquals( environment, accumuloImpl.getEnvironmentManager().loadEnvironment( any( String.class ) ) );
        assertTrue( commandResult.hasSucceeded() );
    }


    @Test
    public void testRunShouldThrowClusterSetupException() throws CommandException
    {
        when( hadoop.getCluster( anyString() ) ).thenThrow( ClusterSetupException.class );
        when( accumuloImpl.getCluster( anyString() ) ).thenReturn( null );

        clusterOperationHandler.run();
    }


    @Test
    public void testRunWhenCommandResultHasNotSucceeded() throws CommandException
    {
        when( accumuloImpl.getCluster( anyString() ) ).thenReturn( accumuloClusterConfig );
        when( commandResult.hasSucceeded() ).thenReturn( false );

        clusterOperationHandler2.run();

        // assertions
        assertNotNull( accumuloImpl.getCluster( anyString() ) );
        verify( containerHost ).execute( new RequestBuilder(
                Commands.uninstallCommand + Common.PACKAGE_PREFIX + AccumuloClusterConfig.PRODUCT_KEY.toLowerCase() ) );
        assertFalse( commandResult.hasSucceeded() );
        verify( trackerOperation ).addLogFailed(
                "Could not uninstall " + AccumuloClusterConfig.PRODUCT_KEY + " from node " + containerHost
                        .getHostname() );
    }


    @Test
    public void testRunShouldThrowsCommandException() throws CommandException
    {
        when( accumuloImpl.getCluster( anyString() ) ).thenReturn( accumuloClusterConfig );
        when( containerHost.execute( new RequestBuilder(
                Commands.uninstallCommand + Common.PACKAGE_PREFIX + AccumuloClusterConfig.PRODUCT_KEY
                        .toLowerCase() ) ) ).thenThrow( CommandException.class );
        when( commandResult.hasSucceeded() ).thenReturn( false );

        clusterOperationHandler2.run();

        // assertions
        assertNotNull( accumuloImpl.getCluster( anyString() ) );
    }

/*
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    HadoopClusterConfig hadoopClusterConfig;
    AccumuloClusterConfig accumuloClusterConfig;
    AccumuloImpl accumuloMock;
    ZookeeperClusterConfig zookeeperClusterConfig;


    @Before
    public void setUp()
    {
        hadoopClusterConfig = mock( HadoopClusterConfig.class );
        when( hadoopClusterConfig.getEnvironmentId() ).thenReturn( UUID.randomUUID() );

        Environment environmentMock = mock( Environment.class );
        when( environmentMock.getId() ).thenReturn( UUID.randomUUID() );

        accumuloClusterConfig = mock( AccumuloClusterConfig.class );
        when( accumuloClusterConfig.getMasterNode() ).thenReturn( UUID.randomUUID() );
        when( accumuloClusterConfig.getGcNode() ).thenReturn( UUID.randomUUID() );
        when( accumuloClusterConfig.getMonitor() ).thenReturn( UUID.randomUUID() );
        when( accumuloClusterConfig.getClusterName() ).thenReturn( "test-cluster" );
        when( accumuloClusterConfig.getInstanceName() ).thenReturn( "test-instance" );
        when( accumuloClusterConfig.getPassword() ).thenReturn( "test-password" );

        accumuloMock = mock( AccumuloImpl.class );
        when( accumuloMock.getTracker() ).thenReturn( new TrackerMock() );
        when( accumuloMock.getEnvironmentManager() ).thenReturn( mock( EnvironmentManager.class ) );
        when( accumuloMock.getEnvironmentManager().getEnvironmentByUUID( hadoopClusterConfig.getEnvironmentId() ) )
                .thenReturn( environmentMock );

        Set<UUID> set = new HashSet<>();
        set.add( UUID.randomUUID() );
        set.add( UUID.randomUUID() );
        when( accumuloClusterConfig.getTracers() ).thenReturn( set );
        when( accumuloClusterConfig.getSlaves() ).thenReturn( set );

        zookeeperClusterConfig = mock( ZookeeperClusterConfig.class );
    }


    @Test
    public void testSetupClusterWithExistingClusterName()
    {
        when( accumuloMock.getCluster( anyString() ) ).thenReturn( accumuloClusterConfig );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( accumuloMock, accumuloClusterConfig, hadoopClusterConfig,
                        zookeeperClusterConfig, ClusterOperationType.INSTALL );

//        operationHandler.run();

//        assertTrue( operationHandler.getTrackerOperation().getLog().contains( "already a cluster" ) );
//        assertEquals( operationHandler.getTrackerOperation().getState(), OperationState.FAILED );
    }


    @Test
    public void testDestroyCluster()
    {
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( accumuloMock, accumuloClusterConfig, hadoopClusterConfig,
                        zookeeperClusterConfig, ClusterOperationType.UNINSTALL );
//        operationHandler.run();
//        assertTrue( operationHandler.getTrackerOperation().getLog().contains( "not exist" ) );
//        assertEquals( operationHandler.getTrackerOperation().getState(), OperationState.FAILED );
    }
*/
}
