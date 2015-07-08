package io.subutai.plugin.accumulo.impl.handler;


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
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;
import io.subutai.plugin.accumulo.impl.Commands;
import io.subutai.plugin.accumulo.impl.handler.RemovePropertyOperationHandler;
import io.subutai.plugin.common.api.ClusterSetupStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RemovePropertyOperationHandlerTest
{
    @Mock AccumuloImpl accumuloImpl;
    @Mock AccumuloClusterConfig accumuloClusterConfig;
    @Mock Tracker tracker;
    @Mock EnvironmentManager environmentManager;
    @Mock TrackerOperation trackerOperation;
    @Mock Environment environment;
    @Mock ContainerHost containerHost;
    @Mock CommandResult commandResult;
    @Mock ClusterSetupStrategy clusterSetupStrategy;
    private RemovePropertyOperationHandler removePropertyOperationHandler;
    private UUID uuid;


    @Before
    public void setUp() throws Exception
    {
        // mock constructor
        uuid = UUID.randomUUID();
        when( accumuloImpl.getCluster( anyString() ) ).thenReturn( accumuloClusterConfig );
        when( accumuloImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        removePropertyOperationHandler =
                new RemovePropertyOperationHandler( accumuloImpl, "testClusterName", "testPropertyName" );
    }


    @Test
    public void testGetTrackerId() throws Exception
    {
        UUID uuid1 = removePropertyOperationHandler.getTrackerId();

        // assertions
        assertNotNull( uuid1 );
        assertEquals( uuid, uuid1 );
    }


    @Test
    public void testRun() throws Exception
    {
        // mock run method
        Set<ContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( accumuloImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( mySet );
        when( containerHost.execute( new RequestBuilder( Commands.getRemovePropertyCommand( "testPropertyName" ) ) ) )
                .thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );

        removePropertyOperationHandler.run();

        // assertions
        assertNotNull( accumuloImpl.getCluster( anyString() ) );
        verify( containerHost )
                .execute( new RequestBuilder( Commands.getRemovePropertyCommand( "testPropertyName" ) ) );
        verify( trackerOperation ).addLog( "Property removed successfully to node " + containerHost.getHostname() );
        verify( containerHost ).execute( Commands.stopCommand );
        verify( containerHost ).execute( Commands.startCommand );
    }


    @Test
    public void testRunWhenCommandResultHasNotSucceeded() throws Exception
    {
        // mock run method
        Set<ContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( accumuloImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( mySet );
        when( containerHost.execute( new RequestBuilder( Commands.getRemovePropertyCommand( "testPropertyName" ) ) ) )
                .thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( false );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );

        removePropertyOperationHandler.run();

        // assertions
        assertNotNull( accumuloImpl.getCluster( anyString() ) );
        verify( containerHost )
                .execute( new RequestBuilder( Commands.getRemovePropertyCommand( "testPropertyName" ) ) );
    }


    @Test( expected = NullPointerException.class )
    public void testRunWhenCommandResultHasNotSucceeded2() throws Exception
    {
        // mock run method
        Set<ContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( accumuloImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( mySet );
        when( containerHost.execute( new RequestBuilder( Commands.getRemovePropertyCommand( "testPropertyName" ) ) ) )
                .thenThrow( CommandException.class );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );

        removePropertyOperationHandler.run();

        // assertions
        assertNotNull( accumuloImpl.getCluster( anyString() ) );
        verify( containerHost )
                .execute( new RequestBuilder( Commands.getRemovePropertyCommand( "testPropertyName" ) ) );
    }


    @Test
    public void testRunShouldThrowsCommandException() throws Exception
    {
        // mock run method
        Set<ContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( accumuloImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( mySet );
        when( containerHost.execute( new RequestBuilder( Commands.getRemovePropertyCommand( "testPropertyName" ) ) ) )
                .thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( Commands.stopCommand ) ).thenThrow( CommandException.class );

        removePropertyOperationHandler.run();

        // assertions
        assertNotNull( accumuloImpl.getCluster( anyString() ) );
        verify( containerHost )
                .execute( new RequestBuilder( Commands.getRemovePropertyCommand( "testPropertyName" ) ) );
        verify( trackerOperation ).addLog( "Property removed successfully to node " + containerHost.getHostname() );
    }
}