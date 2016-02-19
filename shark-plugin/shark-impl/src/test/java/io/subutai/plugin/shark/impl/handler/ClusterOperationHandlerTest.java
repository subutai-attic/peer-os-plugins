package io.subutai.plugin.shark.impl.handler;


import java.util.Iterator;
import java.util.Set;

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
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ClusterOperationHandlerTest
{
    private ClusterOperationHandler clusterOperationHandler;
    private ClusterOperationHandler clusterOperationHandler2;
    private ClusterOperationHandler clusterOperationHandler3;
    private SharkImpl sharkImpl;
    private SharkClusterConfig sharkClusterConfig;
    private TrackerOperation trackerOperation;
    private EnvironmentManager environmentManager;
    private Environment environment;
    private EnvironmentContainerHost containerHost;
    private RequestBuilder requestBuilder;
    private CommandResult commandResult;
    private Spark spark;
    private SparkClusterConfig sparkClusterConfig;
    private Commands commands;
    private PluginDAO pluginDAO;
    private ClusterSetupStrategy clusterSetupStrategy;


    @Before
    public void setUp()
    {
        clusterSetupStrategy = mock( ClusterSetupStrategy.class );
        pluginDAO = mock( PluginDAO.class );
        commands = mock( Commands.class );
        sparkClusterConfig = mock( SparkClusterConfig.class );
        spark = mock( Spark.class );
        commandResult = mock( CommandResult.class );
        requestBuilder = mock( RequestBuilder.class );
        containerHost = mock( EnvironmentContainerHost.class );
        environment = mock( Environment.class );
        environmentManager = mock( EnvironmentManager.class );
        trackerOperation = mock( TrackerOperation.class );
        final Tracker tracker = mock( Tracker.class );
        sharkImpl = mock( SharkImpl.class );
        sharkClusterConfig = mock( SharkClusterConfig.class );
        when( sharkImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        clusterOperationHandler =
                new ClusterOperationHandler( sharkImpl, sharkClusterConfig, ClusterOperationType.INSTALL );
        clusterOperationHandler2 =
                new ClusterOperationHandler( sharkImpl, sharkClusterConfig, ClusterOperationType.UNINSTALL );
        clusterOperationHandler3 =
                new ClusterOperationHandler( sharkImpl, sharkClusterConfig, ClusterOperationType.CUSTOM );
    }


    @Test
    public void testRunWithOperationTypeInstall() throws Exception
    {
        // mock setupCluster method
        when( sharkImpl.getSparkManager() ).thenReturn( spark );
        when( spark.getCluster( any( String.class ) ) ).thenReturn( sparkClusterConfig );
        when( sharkImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( sharkImpl.getClusterSetupStrategy( trackerOperation, sharkClusterConfig, environment ) )
                .thenReturn( clusterSetupStrategy );

        clusterOperationHandler.run();

        // asserts
        assertEquals( spark, sharkImpl.getSparkManager() );
        assertNotNull( sparkClusterConfig );
        assertEquals( sparkClusterConfig, sharkImpl.getSparkManager().getCluster( anyString() ) );
        assertNotNull( environment );
        assertEquals( environment, sharkImpl.getEnvironmentManager().loadEnvironment( any( String.class ) ) );
        verify( sharkImpl ).getClusterSetupStrategy( trackerOperation, sharkClusterConfig, environment );
    }


    @Test
    public void testRunWithOperationTypeUninstall() throws Exception
    {
        // mock destroyCluster method
        Set<EnvironmentContainerHost> mySet = mock( Set.class );
        mySet.add( containerHost );
        when( sharkImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostsByIds( any( Set.class ) ) ).thenReturn( mySet ).thenReturn( mySet );
        Iterator<EnvironmentContainerHost> iterator = mock( Iterator.class );
        when( mySet.iterator() ).thenReturn( iterator ).thenReturn( iterator );
        when( iterator.hasNext() ).thenReturn( true ).thenReturn( false ).thenReturn( true ).thenReturn( false );
        when( iterator.next() ).thenReturn( containerHost ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );
        when( mySet.size() ).thenReturn( 1 );
        when( sharkImpl.getCommands() ).thenReturn( commands );
        when( commands.getUninstallCommand() ).thenReturn( requestBuilder );
        when( sharkImpl.getPluginDao() ).thenReturn( pluginDAO );
        when( pluginDAO.deleteInfo( anyString(), anyString() ) ).thenReturn( true );

        // mock executeCommand
        when( containerHost.execute( requestBuilder ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        clusterOperationHandler.executeCommand( containerHost, requestBuilder );

        clusterOperationHandler2.run();


        // asserts
        assertNotNull( environment );
        assertEquals( environment, sharkImpl.getEnvironmentManager().loadEnvironment( any( String.class ) ) );
        assertTrue( containerHost.isConnected() );
        assertEquals( requestBuilder, sharkImpl.getCommands().getUninstallCommand() );
        assertTrue( sharkImpl.getPluginDao().deleteInfo( anyString(), anyString() ) );
    }


    @Test
    public void testRunWithOperationTypeCustom() throws Exception
    {
        // mock actualizeMasterIP method
        when( sharkImpl.getSparkManager() ).thenReturn( spark );
        when( spark.getCluster( anyString() ) ).thenReturn( sparkClusterConfig );
        when( sharkImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        Set<EnvironmentContainerHost> mySet = mock( Set.class );
        mySet.add( containerHost );
        when( environment.getContainerHostsByIds( any( Set.class ) ) ).thenReturn( mySet ).thenReturn( mySet );
        Iterator<EnvironmentContainerHost> iterator = mock( Iterator.class );
        when( mySet.iterator() ).thenReturn( iterator ).thenReturn( iterator );
        when( iterator.hasNext() ).thenReturn( true ).thenReturn( false ).thenReturn( true ).thenReturn( false );
        when( iterator.next() ).thenReturn( containerHost ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );
        when( mySet.size() ).thenReturn( 1 );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( sharkImpl.getCommands() ).thenReturn( commands );
        when( commands.getSetMasterIPCommand( containerHost ) ).thenReturn( requestBuilder );

        //mock executeCommand method
        when( containerHost.execute( requestBuilder ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        clusterOperationHandler.executeCommand( containerHost, requestBuilder );

        clusterOperationHandler3.run();

        // asserts
        assertEquals( sparkClusterConfig, sharkImpl.getSparkManager().getCluster( anyString() ) );
        assertNotNull( sparkClusterConfig );
        assertNotNull( environment );
        assertEquals( environment, sharkImpl.getEnvironmentManager().loadEnvironment( any( String.class ) ) );
        assertTrue( containerHost.isConnected() );
        assertNotNull( containerHost );
        assertEquals( containerHost, environment.getContainerHostById( any( String.class ) ) );
        assertEquals( commandResult, clusterOperationHandler3.executeCommand( containerHost, requestBuilder ) );
    }


    @Test
    public void testExecuteCommand() throws CommandException, ClusterException
    {
        when( containerHost.execute( requestBuilder ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        clusterOperationHandler.executeCommand( containerHost, requestBuilder );

        // asserts
        assertTrue( commandResult.hasSucceeded() );
        assertEquals( commandResult, clusterOperationHandler.executeCommand( containerHost, requestBuilder ) );
        assertEquals( commandResult, containerHost.execute( requestBuilder ) );
        assertNotNull( clusterOperationHandler.executeCommand( containerHost, requestBuilder ) );
    }
}