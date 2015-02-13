package org.safehaus.subutai.plugin.solr.impl;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;

import static org.junit.Assert.assertNotNull;
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
    ContainerHost containerHost;
    @Mock
    PluginDAO pluginDAO;

    @Before
    public void setUp() throws Exception
    {
        solrSetupStrategy = new SolrSetupStrategy( solrImpl, trackerOperation, solrClusterConfig, environment );
    }


    @Test(expected = ClusterSetupException.class)
    public void testSetupMalformedConfiguration() throws Exception
    {
        solrSetupStrategy.setup();
    }


    @Test(expected = ClusterSetupException.class)
    public void testSetupClusterAlreadyExist() throws Exception
    {
        when(solrClusterConfig.getClusterName()).thenReturn( "test" );
        when( solrClusterConfig.getNumberOfNodes() ).thenReturn( 5 );
        when( solrImpl.getCluster( anyString() ) ).thenReturn( solrClusterConfig );

        solrSetupStrategy.setup();
    }


    @Test(expected = ClusterSetupException.class)
    public void testSetupEnvironmentNoNodes() throws Exception
    {
        when(solrClusterConfig.getClusterName()).thenReturn( "test" );
        when( solrClusterConfig.getNumberOfNodes() ).thenReturn( 5 );
        Set<ContainerHost> mySet = new HashSet<>(  );
        when( environment.getContainerHosts() ).thenReturn( mySet );

        solrSetupStrategy.setup();
    }


    @Test(expected = ClusterSetupException.class)
    public void testSetupNodesRequired() throws Exception
    {
        when(solrClusterConfig.getClusterName()).thenReturn( "test" );
        when( solrClusterConfig.getNumberOfNodes() ).thenReturn( 5 );
        Set<ContainerHost> mySet = new HashSet<>(  );
        mySet.add( containerHost );
        when( environment.getContainerHosts() ).thenReturn( mySet );

        solrSetupStrategy.setup();
    }


    @Test
    public void testSetup() throws Exception
    {
        when(solrClusterConfig.getClusterName()).thenReturn( "test" );
        when( solrClusterConfig.getNumberOfNodes() ).thenReturn( 1 );
        Set<ContainerHost> mySet = new HashSet<>(  );
        mySet.add( containerHost );
        mySet.add( containerHost );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( containerHost.getTemplateName() ).thenReturn( SolrClusterConfig.TEMPLATE_NAME );
        when( containerHost.getId() ).thenReturn( UUID.randomUUID() );
        when( solrImpl.getPluginDAO() ).thenReturn( pluginDAO );

        solrSetupStrategy.setup();

        // assertions
        verify( trackerOperation ).addLog( "Saving cluster information to database..." );
        assertNotNull(solrImpl.getPluginDAO());
        verify( trackerOperation ).addLog( "Cluster information saved to database" );
    }

}