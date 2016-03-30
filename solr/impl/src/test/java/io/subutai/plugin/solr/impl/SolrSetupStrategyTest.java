package io.subutai.plugin.solr.impl;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.solr.api.SolrClusterConfig;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class SolrSetupStrategyTest
{
    private SolrSetupStrategy solrSetupStrategy;
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
    EnvironmentContainerHost containerHost;
    @Mock
    PluginDAO pluginDAO;


    @Before
    public void setUp() throws Exception
    {
        solrSetupStrategy = new SolrSetupStrategy( solrImpl, trackerOperation, solrClusterConfig, environment );
        when( solrImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupMalformedConfiguration() throws Exception
    {
        solrSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterAlreadyExist() throws Exception
    {
        when( solrClusterConfig.getClusterName() ).thenReturn( "test" );
        when( solrClusterConfig.getNumberOfNodes() ).thenReturn( 5 );
        when( solrImpl.getCluster( anyString() ) ).thenReturn( solrClusterConfig );

        solrSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupEnvironmentNoNodes() throws Exception
    {
        when( solrClusterConfig.getClusterName() ).thenReturn( "test" );
        when( solrClusterConfig.getNumberOfNodes() ).thenReturn( 5 );
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        when( environment.getContainerHosts() ).thenReturn( mySet );

        solrSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupNodesRequired() throws Exception
    {
        when( solrClusterConfig.getClusterName() ).thenReturn( "test" );
        when( solrClusterConfig.getNumberOfNodes() ).thenReturn( 5 );
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( environment.getContainerHosts() ).thenReturn( mySet );

        solrSetupStrategy.setup();
    }


    @Test
    public void testSetup() throws Exception
    {
        when( solrClusterConfig.getClusterName() ).thenReturn( "test" );
        when( solrClusterConfig.getNumberOfNodes() ).thenReturn( 1 );
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        mySet.add( containerHost );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( containerHost.getTemplateName() ).thenReturn( SolrClusterConfig.TEMPLATE_NAME );
        when( containerHost.getId() ).thenReturn( UUID.randomUUID().toString() );
        when( solrImpl.getPluginDAO() ).thenReturn( pluginDAO );

        solrSetupStrategy.setup();

        // assertions
        verify( trackerOperation ).addLog( "Saving cluster information to database..." );
        assertNotNull( solrImpl.getPluginDAO() );
        verify( trackerOperation ).addLog( "Cluster information saved to database" );
    }
}