package org.safehaus.subutai.plugin.cassandra.impl.handler;


import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.cassandra.api.CassandraClusterConfig;
import org.safehaus.subutai.plugin.cassandra.impl.CassandraImpl;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class InstallClusterTest
{
    private ClusterOperationHandler installClusterHandler;
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
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;


    @Before
    public void setUp()
    {
        uuid = new UUID( 50, 50 );
        when( cassandraImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( cassandraClusterConfig.getClusterName() ).thenReturn( "test" );

        installClusterHandler =
                new ClusterOperationHandler( cassandraImpl, cassandraClusterConfig, ClusterOperationType.INSTALL );
    }


    @Test
    public void testGetTrackerId()
    {
        when( trackerOperation.getId() ).thenReturn( uuid );

        installClusterHandler.getTrackerId();

        // asserts
        assertNotNull( installClusterHandler.getTrackerId() );
        assertEquals( uuid, installClusterHandler.getTrackerId() );
    }


//    @Test
    //    public void testRun() throws Exception
    //    {
    //        // mock run method
    //        when( cassandraImpl.getEnvironmentManager() ).thenReturn( environmentManager );
    //        when( environmentManager.createEnvironment( anyString(), any( Topology.class ), anyBoolean() ) )
    //                .thenReturn( environment );
    //        when( cassandraImpl.getClusterSetupStrategy( environment, cassandraClusterConfig, trackerOperation ) )
    //                .thenReturn( clusterSetupStrategy );
    //
    //        installClusterHandler.run();
    //
    //        // asserts
    //        assertEquals( clusterSetupStrategy,
    //                cassandraImpl.getClusterSetupStrategy( environment, cassandraClusterConfig, trackerOperation ) );
    //    }
}