package io.subutai.plugin.cassandra.impl.handler;


import java.util.Iterator;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.plugin.common.api.ClusterOperationType;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class StopClusterTest
{
    private ClusterOperationHandler stopClusterHandler;
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
    EnvironmentContainerHost containerHost;
    @Mock
    Iterator<EnvironmentContainerHost> iterator;
    @Mock
    Set<EnvironmentContainerHost> mySet;
    @Mock
    CommandResult commandResult;


    @Before
    public void setup()
    {
        when( cassandraImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( cassandraClusterConfig.getClusterName() ).thenReturn( "test" );
        stopClusterHandler =
                new ClusterOperationHandler( cassandraImpl, cassandraClusterConfig, ClusterOperationType.STOP_ALL );
    }


    @Test
    public void testRun() throws Exception
    {
        // mock run method
        when( cassandraImpl.getCluster( "test" ) ).thenReturn( cassandraClusterConfig );
        when( cassandraImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( mySet.iterator() ).thenReturn( iterator );
        when( iterator.hasNext() ).thenReturn( true ).thenReturn( false );
        when( iterator.next() ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        stopClusterHandler.run();

        // asserts
        assertNotNull( cassandraImpl.getCluster( "test" ) );
        assertEquals( environment, environmentManager.loadEnvironment( any( String.class ) ) );
        assertTrue( commandResult.hasSucceeded() );
    }


    @Test
    public void testRunWhenClusterDoesNotExist()
    {
        when( cassandraImpl.getCluster( "test" ) ).thenReturn( null );

        stopClusterHandler.run();
    }


    @Test
    public void testRunWhenCommandResultNotSucceeded() throws Exception
    {
        // mock run method
        when( cassandraImpl.getCluster( "test" ) ).thenReturn( cassandraClusterConfig );
        when( cassandraImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( mySet.iterator() ).thenReturn( iterator );
        when( iterator.hasNext() ).thenReturn( true ).thenReturn( false );
        when( iterator.next() ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( false );

        stopClusterHandler.run();
    }
}
