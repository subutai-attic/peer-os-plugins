package org.safehaus.subutai.plugin.solr.impl.handler;


import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;
import org.safehaus.subutai.plugin.solr.impl.SolrImpl;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class EnvConfigOperationHandlerTest
{
    private EnvConfigOperationHandler envConfigOperationHandler;
    private UUID uuid;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    SolrImpl solrImpl;
    @Mock
    SolrClusterConfig solrClusterConfig;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    Environment environment;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;


    @Before
    public void setUp() throws Exception
    {
        // mock constructor
        uuid = new UUID( 50, 50 );
        when( solrImpl.getCluster( "testClusterName" ) ).thenReturn( solrClusterConfig );
        when( solrImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );


        envConfigOperationHandler = new EnvConfigOperationHandler( solrImpl, solrClusterConfig );

        // mock run
        when( solrImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( solrImpl.getClusterSetupStrategy( environment, solrClusterConfig, trackerOperation ) )
                .thenReturn( clusterSetupStrategy );
    }


    @Test
    public void testRun() throws Exception
    {
        envConfigOperationHandler.run();

        // assertions
        assertNotNull( solrImpl.getClusterSetupStrategy( environment, solrClusterConfig, trackerOperation ) );
        verify( clusterSetupStrategy).setup();
        verify( trackerOperation ).addLogDone( String.format( "Cluster %s set up successfully", null ));
    }

    @Test(expected = RuntimeException.class)
    public void testRunSetupFailed() throws ClusterSetupException
    {
        when( clusterSetupStrategy.setup() ).thenThrow( ClusterSetupException.class );

        envConfigOperationHandler.run();
    }
}