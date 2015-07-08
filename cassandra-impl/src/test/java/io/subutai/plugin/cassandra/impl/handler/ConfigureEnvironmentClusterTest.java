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
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.plugin.cassandra.impl.handler.ConfigureEnvironmentClusterHandler;
import io.subutai.plugin.common.PluginDAO;
import io.subutai.plugin.common.api.ClusterSetupStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ConfigureEnvironmentClusterTest
{
    private ConfigureEnvironmentClusterHandler configureEnvironmentClusterHandler;
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

        configureEnvironmentClusterHandler =
                new ConfigureEnvironmentClusterHandler( cassandraImpl, cassandraClusterConfig );
    }


    @Test
    public void testGetTrackerId()
    {
        when( trackerOperation.getId() ).thenReturn( uuid );

        configureEnvironmentClusterHandler.getTrackerId();

        // asserts
        assertNotNull( configureEnvironmentClusterHandler.getTrackerId() );
        assertEquals( uuid, configureEnvironmentClusterHandler.getTrackerId() );
    }


    @Test
    public void testRun() throws EnvironmentNotFoundException
    {
        // mock run method
        when( cassandraImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( cassandraImpl.getClusterSetupStrategy( environment, cassandraClusterConfig, trackerOperation ) )
                .thenReturn( clusterSetupStrategy );
        when( environment.getId() ).thenReturn( UUID.randomUUID() );
        when( cassandraImpl.getPluginDAO() ).thenReturn( mock( PluginDAO.class ) );
        configureEnvironmentClusterHandler.run();

        // asserts
        verify( trackerOperation ).addLog( "Configuring environment..." );
        assertEquals( environment, environmentManager.findEnvironment( any( UUID.class ) ) );
        assertEquals( clusterSetupStrategy,
                cassandraImpl.getClusterSetupStrategy( environment, cassandraClusterConfig, trackerOperation ) );
    }
}