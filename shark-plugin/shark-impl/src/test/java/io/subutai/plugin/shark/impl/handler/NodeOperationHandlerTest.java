package io.subutai.plugin.shark.impl.handler;


import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.OperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.shark.api.SharkClusterConfig;
import io.subutai.plugin.shark.impl.Commands;
import io.subutai.plugin.shark.impl.SharkImpl;
import io.subutai.plugin.spark.api.Spark;
import io.subutai.plugin.spark.api.SparkClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class NodeOperationHandlerTest
{
    NodeOperationHandler nodeOperationHandler;
    NodeOperationHandler nodeOperationHandler2;
    private SharkImpl sharkImpl;
    private SharkClusterConfig sharkClusterConfig;
    private Tracker tracker;
    private TrackerOperation trackerOperation;
    private EnvironmentManager environmentManager;
    private Environment environment;
    private EnvironmentContainerHost containerHost;
    private RequestBuilder requestBuilder;
    private CommandResult commandResult;
    private String id;
    private Spark spark;
    private SparkClusterConfig sparkClusterConfig;
    private Commands commands;
    private PluginDAO pluginDAO;


    @Before
    public void setUp()
    {
        pluginDAO = mock( PluginDAO.class );
        commands = mock( Commands.class );
        sparkClusterConfig = mock( SparkClusterConfig.class );
        spark = mock( Spark.class );
        id = UUID.randomUUID().toString();
        commandResult = mock( CommandResult.class );
        requestBuilder = mock( RequestBuilder.class );
        containerHost = mock( EnvironmentContainerHost.class );
        environment = mock( Environment.class );
        environmentManager = mock( EnvironmentManager.class );
        trackerOperation = mock( TrackerOperation.class );
        tracker = mock( Tracker.class );
        sharkImpl = mock( SharkImpl.class );
        sharkClusterConfig = mock( SharkClusterConfig.class );
        when( sharkImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );

        nodeOperationHandler = new NodeOperationHandler( sharkImpl, sharkClusterConfig, "test", OperationType.INCLUDE );
        nodeOperationHandler2 =
                new NodeOperationHandler( sharkImpl, sharkClusterConfig, "test", OperationType.EXCLUDE );
    }


    @Test
    public void testRunWithOperationTypeInclude() throws Exception
    {
        // mock run method
        when( sharkImpl.getCluster( any( String.class ) ) ).thenReturn( sharkClusterConfig );
        when( sharkImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( "test" ) ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );

        // mock addNode method
        List<String> myList = mock( List.class );
        myList.add( id );
        when( containerHost.getId() ).thenReturn( id );
        when( sharkImpl.getSparkManager() ).thenReturn( spark );
        when( spark.getCluster( any( String.class ) ) ).thenReturn( sparkClusterConfig );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( sparkClusterConfig.getAllNodesIds() ).thenReturn( myList );
        when( sparkClusterConfig.getAllNodesIds().contains( id ) ).thenReturn( true );

        when( commands.getCheckInstalledCommand() ).thenReturn( requestBuilder );
        when( commandResult.getStdOut() ).thenReturn( "test" );
        when( commands.getInstallCommand() ).thenReturn( requestBuilder );
        when( commands.getSetMasterIPCommand( containerHost ) ).thenReturn( requestBuilder );
        when( sharkImpl.getPluginDao() ).thenReturn( pluginDAO );
        when( pluginDAO.saveInfo( anyString(), anyString(), any() ) ).thenReturn( true );

        // mock executeCommand method
        when( sharkImpl.getCommands() ).thenReturn( commands );
        when( containerHost.execute( requestBuilder ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        nodeOperationHandler.executeCommand( containerHost, requestBuilder );

        nodeOperationHandler.run();

        assertNotNull( environment );
        assertNotNull( containerHost );
        assertNotNull( sparkClusterConfig );
        assertNotNull( commandResult );
        assertEquals( commandResult,
                nodeOperationHandler.executeCommand( containerHost, sharkImpl.getCommands().getInstallCommand() ) );
        assertEquals( id, containerHost.getId() );
        assertTrue( containerHost.isConnected() );
        assertTrue( pluginDAO.saveInfo( anyString(), anyString(), any() ) );
    }


    @Test
    public void testRunWithOperationTypeExclude() throws Exception
    {
        // mock run method
        when( sharkImpl.getCluster( any( String.class ) ) ).thenReturn( sharkClusterConfig );
        when( sharkImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( "test" ) ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );

        // mock removeNode method
        Set<String> mySet = mock( Set.class );
        mySet.add( id );
        when( containerHost.getId() ).thenReturn( id );
        when( sharkClusterConfig.getNodeIds() ).thenReturn( mySet );
        when( sharkClusterConfig.getNodeIds().contains( anyString() ) ).thenReturn( true );
        when( sharkClusterConfig.getNodeIds().remove( any( String.class ) ) ).thenReturn( true );
        when( sharkImpl.getPluginDao() ).thenReturn( pluginDAO );
        when( pluginDAO.saveInfo( anyString(), anyString(), any() ) ).thenReturn( true );


        // mock executeCommand method
        when( sharkImpl.getCommands() ).thenReturn( commands );
        when( commands.getUninstallCommand() ).thenReturn( requestBuilder );

        when( containerHost.execute( requestBuilder ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        nodeOperationHandler.executeCommand( containerHost, requestBuilder );


        nodeOperationHandler2.run();

        assertNotNull( environment );
        assertNotNull( containerHost );
        assertNotNull( sparkClusterConfig );
        assertNotNull( commandResult );
        assertEquals( commandResult,
                nodeOperationHandler.executeCommand( containerHost, sharkImpl.getCommands().getUninstallCommand() ) );
        assertEquals( id, containerHost.getId() );
        assertTrue( containerHost.isConnected() );
        assertTrue( pluginDAO.saveInfo( anyString(), anyString(), any() ) );
        assertTrue( sharkClusterConfig.getNodeIds().remove( any( String.class ) ) );
    }


    @Test
    public void testExecuteCommand() throws CommandException, ClusterException
    {
        when( containerHost.execute( requestBuilder ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        nodeOperationHandler.executeCommand( containerHost, requestBuilder );

        assertTrue( commandResult.hasSucceeded() );
        assertEquals( commandResult, nodeOperationHandler.executeCommand( containerHost, requestBuilder ) );
        assertEquals( commandResult, containerHost.execute( requestBuilder ) );
        assertNotNull( nodeOperationHandler.executeCommand( containerHost, requestBuilder ) );
    }


    @Test( expected = IllegalArgumentException.class )
    public void testRunWhenClusterNameIsNull()
    {
        when( sharkImpl.getTracker() ).thenReturn( tracker );
        nodeOperationHandler = new NodeOperationHandler( sharkImpl, sharkClusterConfig, null, OperationType.INSTALL );
    }


    @Test( expected = ClusterException.class )
    public void shouldThrowsClusterExceptionInExecuteCommand() throws CommandException, ClusterException
    {
        when( containerHost.execute( requestBuilder ) ).thenReturn( commandResult );
        nodeOperationHandler.executeCommand( containerHost, requestBuilder );
    }
}