package org.safehaus.subutai.plugin.solr.impl.handler;


import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.Topology;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;
import org.safehaus.subutai.plugin.solr.impl.SolrImpl;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ClusterOperationHandlerTest
{
    private ClusterOperationHandler clusterOperationHandler;
    private ClusterOperationHandler clusterOperationHandler2;
    private UUID uuid;
    @Mock
    SolrImpl solrImpl;
    @Mock
    SolrClusterConfig solrClusterConfig;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    Topology topology;


    @Before
    public void setUp() throws Exception
    {
        // mock constructor
        uuid = UUID.randomUUID();
        when( solrImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        clusterOperationHandler =
                new ClusterOperationHandler( solrImpl, solrClusterConfig, ClusterOperationType.INSTALL_OVER_ENV );
        clusterOperationHandler2 =
                new ClusterOperationHandler( solrImpl, solrClusterConfig, ClusterOperationType.UNINSTALL );

        // mock setupCluster
        when( solrImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( solrImpl.getClusterSetupStrategy( environment, solrClusterConfig, trackerOperation ) )
                .thenReturn( clusterSetupStrategy );
    }


    @Test
    public void testRunOperationOnContainers() throws Exception
    {
//        clusterOperationHandler.runOperationOnContainers( ClusterOperationType.INSTALL_OVER_ENV );
    }


    @Test
    public void testRunOperationTypeInstall() throws Exception
    {
//        clusterOperationHandler.run();

        // assertions
//        verify( trackerOperation ).addLog( "Building environment..." );
//        verify( clusterSetupStrategy ).setup();
//        verify( trackerOperation ).addLogDone( String.format( "Cluster %s set up successfully", null ) );
    }


    @Test//( expected = RuntimeException.class )
    public void testRunOperationTypeInstallSetupFailed() throws Exception
    {
        when( clusterSetupStrategy.setup() ).thenThrow( ClusterSetupException.class );

        clusterOperationHandler.run();
    }


    @Test
    public void testRunOpertaionTypeUninstall()
    {
        when( solrImpl.getCluster( anyString() ) ).thenReturn( solrClusterConfig );
        when( solrImpl.getPluginDAO() ).thenReturn( pluginDAO );

        clusterOperationHandler2.run();

        // assertions
        verify( solrImpl ).getPluginDAO();
        verify( trackerOperation ).addLogDone( "Cluster removed from database" );
        assertNotNull( solrImpl.getCluster( anyString() ) );
    }


    @Test
    public void testRunOpertaionTypeUninstallSolrClusterConfigNull()
    {
        when( solrImpl.getCluster( anyString() ) ).thenReturn( null );

        clusterOperationHandler2.run();

        // assertions
        verify( trackerOperation )
                .addLogFailed( String.format( "Cluster with name %s does not exist. Operation aborted", null ) );
    }
}