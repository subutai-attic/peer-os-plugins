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
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.template.api.TemplateManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class InstallClusterTest
{
    private ClusterOperationHandler installClusterHandler;
    private UUID id;
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
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    TemplateManager templateManager;


    @Before
    public void setUp()
    {
        id = UUID.randomUUID();
        when( cassandraImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( cassandraClusterConfig.getClusterName() ).thenReturn( "test" );

        installClusterHandler = new ClusterOperationHandler( cassandraImpl, templateManager, cassandraClusterConfig,
                ClusterOperationType.INSTALL );
    }


    @Test
    public void testGetTrackerId()
    {
        when( trackerOperation.getId() ).thenReturn( id );

        installClusterHandler.getTrackerId();

        // asserts
        assertNotNull( installClusterHandler.getTrackerId() );
        assertEquals( id, installClusterHandler.getTrackerId() );
    }
}