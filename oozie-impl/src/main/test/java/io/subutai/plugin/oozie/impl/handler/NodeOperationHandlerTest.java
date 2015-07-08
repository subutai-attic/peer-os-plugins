package io.subutai.plugin.oozie.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.PluginDAO;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.Commands;
import io.subutai.plugin.oozie.impl.OozieImpl;
import io.subutai.plugin.oozie.impl.handler.NodeOperationHandler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class NodeOperationHandlerTest
{
    private NodeOperationHandler nodeOperationHandler;
    private NodeOperationHandler nodeOperationHandler2;
    private NodeOperationHandler nodeOperationHandler3;
    private NodeOperationHandler nodeOperationHandler4;
    private NodeOperationHandler nodeOperationHandler5;
    private UUID uuid;
    @Mock
    Commands commands;
    @Mock
    OozieImpl oozieImpl;
    @Mock
    OozieClusterConfig oozieClusterConfig;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    ContainerHost containerHost;
    @Mock
    CommandResult commandResult;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    Hadoop hadoop;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    RequestBuilder requestBuilder;


    @Before
    public void setUp() throws Exception
    {
        // mock constructor
        uuid = new UUID( 50, 50 );
        when( oozieImpl.getCluster( "testClusterName" ) ).thenReturn( oozieClusterConfig );
        when( oozieImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        nodeOperationHandler =
                new NodeOperationHandler( oozieImpl, "testClusterName", "testHostName", NodeOperationType.START );
        nodeOperationHandler2 =
                new NodeOperationHandler( oozieImpl, "testClusterName", "testHostName", NodeOperationType.STOP );
        nodeOperationHandler3 =
                new NodeOperationHandler( oozieImpl, "testClusterName", "testHostName", NodeOperationType.STATUS );
        nodeOperationHandler4 =
                new NodeOperationHandler( oozieImpl, "testClusterName", "testHostName", NodeOperationType.INSTALL );
        nodeOperationHandler5 =
                new NodeOperationHandler( oozieImpl, "testClusterName", "testHostName", NodeOperationType.UNINSTALL );

        // mock run method
        Set<ContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( containerHost.getHostname() ).thenReturn( "testHostName" );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );

        // mock installProductOnNode
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );

        when( oozieImpl.getPluginDao() ).thenReturn( pluginDAO );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
    }


    @Test
    public void testRunOperationTypeStart() throws Exception
    {
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );

        nodeOperationHandler.run();

        // assertions
        assertNotNull( oozieImpl.getCluster( "testClusterName" ) );
        assertTrue( containerHost.isConnected() );
    }


    @Test
    public void testRunOperationTypeStartRunning() throws Exception
    {
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );
        when( commandResult.getStdOut() ).thenReturn( "Oozie Server is running" );

        nodeOperationHandler.run();

        // assertions
        assertNotNull( oozieImpl.getCluster( "testClusterName" ) );
    }


    @Test
    public void testRunOperationTypeStartNotRunning() throws Exception
    {
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );
        when( commandResult.getStdOut() ).thenReturn( "Oozie Server is not running" );

        nodeOperationHandler.run();

        // assertions
        assertNotNull( oozieImpl.getCluster( "testClusterName" ) );
    }


    @Test
    public void testRunOperationTypeStop() throws CommandException
    {
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );

        nodeOperationHandler2.run();

        // assertions
        assertNotNull( oozieImpl.getCluster( "testClusterName" ) );
    }


    @Test
    public void testRunOperationTypeStatus() throws CommandException
    {
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );

        nodeOperationHandler3.run();

        // assertions
        assertNotNull( oozieImpl.getCluster( "testClusterName" ) );
    }


    @Test
    public void testRunOperationTypeInstall() throws CommandException
    {
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );

        nodeOperationHandler4.run();

        // assertions
        assertNotNull( oozieImpl.getCluster( "testClusterName" ) );
        assertTrue( commandResult.hasSucceeded() );
        verify( oozieImpl ).getPluginDao();
    }


    @Test
    public void testRunOperationTypeInstallHasNotSucceeded() throws CommandException
    {
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );
        when( commandResult.hasSucceeded() ).thenReturn( false );

        nodeOperationHandler4.run();

        // assertions
        assertNotNull( oozieImpl.getCluster( "testClusterName" ) );
        assertFalse( commandResult.hasSucceeded() );
        verify( trackerOperation ).addLogFailed( "Could not install " + OozieClusterConfig.PRODUCT_KEY + " to node " + "testHostName" );
    }


    @Test
    public void testRunOperationTypeUninstall() throws CommandException
    {
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        nodeOperationHandler5.run();

        // assertions
        assertNotNull( oozieImpl.getCluster( "testClusterName" ) );
        assertTrue( commandResult.hasSucceeded() );
        verify( oozieImpl ).getPluginDao();
    }


    @Test
    public void testRunOperationTypeUninstallHasNotSucceeded() throws CommandException
    {
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );
        when( commandResult.hasSucceeded() ).thenReturn( false );

        nodeOperationHandler5.run();

        // assertions
        assertNotNull( oozieImpl.getCluster( "testClusterName" ) );
        assertFalse( commandResult.hasSucceeded() );
        verify( trackerOperation ).addLogFailed( "Could not uninstall " + OozieClusterConfig.PRODUCT_KEY + " from node " + "testHostName" );
    }


    @Test
    public void testRunClusterNotExist()
    {
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );

        nodeOperationHandler.run();
    }


    @Test
    public void testRunClusterNoEnvironment() throws EnvironmentNotFoundException
    {
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( oozieClusterConfig );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( null );

        nodeOperationHandler.run();
    }
}