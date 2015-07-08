package io.subutai.plugin.hipi.impl.handler;


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
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hipi.api.HipiConfig;
import io.subutai.plugin.hipi.impl.HipiImpl;
import io.subutai.plugin.hipi.impl.handler.NodeOperationHandler;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class NodeOperationHandlerTest
{
    @Mock CommandResult commandResult;
    @Mock ContainerHost containerHost;
    @Mock HipiImpl hipiImpl;
    @Mock HipiConfig hipiConfig;
    @Mock Tracker tracker;
    @Mock EnvironmentManager environmentManager;
    @Mock TrackerOperation trackerOperation;
    @Mock Environment environment;
    private NodeOperationHandler nodeOperationHandler;
    private NodeOperationHandler nodeOperationHandler2;
    private UUID uuid;


    @Before
    public void setUp() throws Exception
    {
        // mock constructor
        uuid = new UUID( 50, 50 );
        when( hipiImpl.getCluster( "testClusterName" ) ).thenReturn( hipiConfig );
        when( hipiImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        nodeOperationHandler =
                new NodeOperationHandler( hipiImpl, "testClusterName", "testHostName", NodeOperationType.INCLUDE );
        nodeOperationHandler2 =
                new NodeOperationHandler( hipiImpl, "testClusterName", "testHostName", NodeOperationType.EXCLUDE );
    }


    @Test
    public void testRunEnvironmentNotFound() throws Exception
    {
        when( hipiImpl.getEnvironmentManager() ).thenThrow( EnvironmentNotFoundException.class );

        nodeOperationHandler.run();
    }


    @Test
    public void testRunContainerNotFound() throws Exception
    {
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( anyString() ) ).thenThrow( ContainerHostNotFoundException.class );

        nodeOperationHandler.run();
    }


    @Test
    public void testRunContainerHostNotConnected() throws Exception
    {
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( false );

        nodeOperationHandler.run();
    }


    @Test
    public void testRunAlreadyHasHipi() throws Exception
    {
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasCompleted() ).thenReturn( true );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( commandResult.getStdOut() ).thenReturn( HipiConfig.PRODUCT_PACKAGE );

        nodeOperationHandler.run();
    }


    @Test
    public void testRunCommandResultNotCompleted() throws Exception
    {
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasCompleted() ).thenReturn( false );

        nodeOperationHandler.run();
    }


    @Test
    public void testRunNodeTypeInclude() throws Exception
    {
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasCompleted() ).thenReturn( true );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( commandResult.getStdOut() ).thenReturn( Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME );

        nodeOperationHandler.run();
    }


    @Test
    public void testRunExcludeLastSlave() throws EnvironmentNotFoundException, ContainerHostNotFoundException
    {
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );
        Set<UUID> mySet = new HashSet<>();
        mySet.add( uuid );
        when( hipiConfig.getNodes() ).thenReturn( mySet );

        nodeOperationHandler2.run();
    }


    @Test
    public void testRunExcludeNotBelongToCluster() throws EnvironmentNotFoundException, ContainerHostNotFoundException
    {
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );
        Set<UUID> mySet = new HashSet<>();
        mySet.add( uuid );
        mySet.add( new UUID( 5, 5 ) );
        when( hipiConfig.getNodes() ).thenReturn( mySet );

        nodeOperationHandler2.run();
    }


    @Test
    public void testRunExcludeCommandResultNotSucceeded()
            throws EnvironmentNotFoundException, ContainerHostNotFoundException, CommandException
    {
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );
        Set<UUID> mySet = new HashSet<>();
        mySet.add( uuid );
        mySet.add( new UUID( 5, 5 ) );
        when( hipiConfig.getNodes() ).thenReturn( mySet );
        when( containerHost.getId() ).thenReturn( uuid );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );

        nodeOperationHandler2.run();
    }


    @Test
    public void testRunExclude()
            throws EnvironmentNotFoundException, ContainerHostNotFoundException, CommandException, ClusterException
    {
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( containerHost.isConnected() ).thenReturn( true );
        Set<UUID> mySet = new HashSet<>();
        mySet.add( uuid );
        mySet.add( new UUID( 5, 5 ) );
        when( hipiConfig.getNodes() ).thenReturn( mySet );
        when( containerHost.getId() ).thenReturn( uuid );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( containerHost.getHostname() ).thenReturn( "HostName" );

        nodeOperationHandler2.run();

        // assertions
        verify( trackerOperation ).addLog( HipiConfig.PRODUCT_KEY + " removed from " + containerHost.getHostname() );
        verify( hipiImpl ).saveConfig( hipiConfig );
        verify( trackerOperation ).addLogDone( "Cluster info updated in DB\nDone" );
    }
}