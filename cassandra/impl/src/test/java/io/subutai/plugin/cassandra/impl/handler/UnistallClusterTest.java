package io.subutai.plugin.cassandra.impl.handler;


import java.util.Iterator;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.template.api.TemplateManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class UnistallClusterTest
{
    private ClusterOperationHandler uninstallClusterHandler;
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
    PluginDAO pluginDAO;
    @Mock
    TemplateManager templateManager;


    @Before
    public void setup()
    {
        when( cassandraImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( cassandraClusterConfig.getClusterName() ).thenReturn( "test" );

        uninstallClusterHandler =
                new ClusterOperationHandler( cassandraImpl, templateManager, cassandraClusterConfig, ClusterOperationType.UNINSTALL );
    }


    @Test
    public void testRun() throws Exception
    {
        // mock run method
        when( cassandraImpl.getCluster( anyString() ) ).thenReturn( cassandraClusterConfig );
        when( cassandraImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        //        when(environmentManager.destroyEnvironment(any(UUID.class))).thenReturn(true);
        when( cassandraImpl.getPluginDAO() ).thenReturn( pluginDAO );
        when( pluginDAO.deleteInfo( anyString(), anyString() ) ).thenReturn( true );

        uninstallClusterHandler.run();

        // asserts
        //        assertTrue(environmentManager.destroyEnvironment( any( UUID.class ) ));
        assertTrue( pluginDAO.deleteInfo( anyString(), anyString() ) );
    }


    @Test
    public void testRunWhenCassandraClusterConfigIsNull()
    {
        when( cassandraImpl.getCluster( anyString() ) ).thenReturn( null );

        uninstallClusterHandler.run();
    }
}
