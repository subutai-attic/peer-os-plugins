package io.subutai.plugin.cassandra.impl.handler;


import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.plugin.common.api.NodeOperationType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class CheckNodeTest
{
    private NodeOperationHandler checkNodeHandler;
    private UUID uuid;
    @Mock
    CassandraImpl cassandraImpl;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    CassandraClusterConfig cassandraClusterConfig;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    Environment environment;
    @Mock
    ContainerHost containerHost;
    @Mock
    Iterator<ContainerHost> iterator;
    @Mock
    Set<ContainerHost> mySet;
    @Mock
    CommandResult commandResult;


    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID( 50, 50 );
        when( cassandraImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );

        checkNodeHandler = new NodeOperationHandler( cassandraImpl, "test", "test", NodeOperationType.STATUS );
    }


    @Test
    public void testRun() throws Exception
    {
        // mock run method
        when( cassandraImpl.getCluster( "test" ) ).thenReturn( cassandraClusterConfig );
        when( cassandraImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( mySet.iterator() ).thenReturn( iterator );
        when( iterator.hasNext() ).thenReturn( true ).thenReturn( false );
        when( iterator.next() ).thenReturn( containerHost );
        when( containerHost.getId() ).thenReturn( uuid );
        when( containerHost.getHostname() ).thenReturn( "test" );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );


        checkNodeHandler.run();

        assertNotNull( cassandraImpl.getCluster( "test" ) );
        assertEquals( environment, environmentManager.findEnvironment( any( UUID.class ) ) );
        assertTrue( commandResult.hasSucceeded() );
    }


    @Test
    public void testRunWhenCommandResultNotSucceeded() throws Exception
    {
        // mock run method
        when( cassandraImpl.getCluster( "test" ) ).thenReturn( cassandraClusterConfig );
        when( cassandraImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( mySet.iterator() ).thenReturn( iterator );
        when( iterator.hasNext() ).thenReturn( true ).thenReturn( false );
        when( iterator.next() ).thenReturn( containerHost );
        when( containerHost.getId() ).thenReturn( uuid );
        when( containerHost.getHostname() ).thenReturn( "test" );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( false );

        checkNodeHandler.run();
    }


    @Test
    public void testRunWhenClusterDoesNotExist()
    {
        when( cassandraImpl.getCluster( "test" ) ).thenReturn( null );

        checkNodeHandler.run();
    }
}