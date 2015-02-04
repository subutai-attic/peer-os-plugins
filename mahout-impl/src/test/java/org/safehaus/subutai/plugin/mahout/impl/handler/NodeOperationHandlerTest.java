package org.safehaus.subutai.plugin.mahout.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;
import org.safehaus.subutai.plugin.mahout.impl.Commands;
import org.safehaus.subutai.plugin.mahout.impl.MahoutImpl;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class NodeOperationHandlerTest
{
    private NodeOperationHandler nodeOperationHandler;
    private NodeOperationHandler nodeOperationHandler2;
    private UUID uuid;
    @Mock
    Commands commands;
    @Mock
    MahoutImpl mahoutImpl;
    @Mock
    MahoutClusterConfig mahoutClusterConfig;
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
        when( mahoutImpl.getCluster( "testClusterName" ) ).thenReturn( mahoutClusterConfig );
        when( mahoutImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        nodeOperationHandler =
                new NodeOperationHandler( mahoutImpl, "testClusterName", "testHostName", NodeOperationType.INSTALL );
        nodeOperationHandler2 =
                new NodeOperationHandler( mahoutImpl, "testClusterName", "testHostName", NodeOperationType.UNINSTALL );

        // mock run method
        Set<ContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( containerHost.getHostname() ).thenReturn( "testHostName" );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );


        // mock installProductOnNode
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );

        when( mahoutImpl.getPluginDAO() ).thenReturn( pluginDAO );
        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );

        when( mahoutImpl.getCommands() ).thenReturn( commands );
    }


    @Test
    public void testRunOperationTypeInstall() throws CommandException
    {
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );

        nodeOperationHandler.run();

        // assertions
        assertNotNull( mahoutImpl.getCluster( "testClusterName" ) );
    }


    @Test
    public void testRunOperationTypeInstallHasNotSucceeded() throws CommandException
    {
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );
        when( commandResult.hasSucceeded() ).thenReturn( false );

        nodeOperationHandler.run();

        // assertions
        assertNotNull( mahoutImpl.getCluster( "testClusterName" ) );
    }


    @Test
    public void testRunOperationTypeUninstall() throws CommandException
    {
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        nodeOperationHandler2.run();

        // assertions
        assertNotNull( mahoutImpl.getCluster( "testClusterName" ) );
    }


    @Test
    public void testRunOperationTypeUninstallHasNotSucceeded() throws CommandException
    {
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.getExitCode() ).thenReturn( 0 );
        when( containerHost.isConnected() ).thenReturn( true );
        when( commandResult.hasSucceeded() ).thenReturn( false );
        when( commands.getUninstallCommand() ).thenReturn( requestBuilder );

        nodeOperationHandler2.run();

        // assertions
        assertNotNull( mahoutImpl.getCluster( "testClusterName" ) );
    }


    @Test
    public void testRunClusterNotExist()
    {
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );

        nodeOperationHandler.run();
    }
}