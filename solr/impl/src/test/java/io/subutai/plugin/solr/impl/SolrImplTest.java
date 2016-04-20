package io.subutai.plugin.solr.impl;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

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
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.solr.api.SolrClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class SolrImplTest
{
    private SolrImpl solrImpl;
    private String id;
    @Mock
    SolrClusterConfig solrClusterConfig;
    @Mock
    Commands commands;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    CommandResult commandResult;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    RequestBuilder requestBuilder;
    @Mock
    ExecutorService executorService;
    @Mock
    DataSource dataSource;
    @Mock
    Connection connection;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    ResultSet resultSet;
    @Mock
    ResultSetMetaData resultSetMetaData;
    @Mock
    QuotaManager quotaManager;


    @Before
    public void setUp() throws Exception
    {
        id = UUID.randomUUID().toString();
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );
        when( pluginDAO.getInfo( SolrClusterConfig.PRODUCT_KEY, "test", SolrClusterConfig.class ) )
                .thenReturn( solrClusterConfig );

        solrImpl = new SolrImpl( pluginDAO );

        solrImpl.setTracker( tracker );
        solrImpl.setExecutor( executorService );
        solrImpl.setPluginDAO( pluginDAO );
        solrImpl.setEnvironmentManager( environmentManager );
    }


    @Test
    public void testGetPluginDAO() throws Exception
    {
        solrImpl.getPluginDAO();

        // assertions
        assertNotNull( solrImpl.getPluginDAO() );
        assertEquals( pluginDAO, solrImpl.getPluginDAO() );
    }


    @Test
    public void testInit() throws Exception
    {
        //        solrImpl.init();
    }


    @Test
    public void testDestroy() throws Exception
    {
        solrImpl.destroy();
    }


    @Test
    public void testGetEnvironmentManager() throws Exception
    {
        solrImpl.getEnvironmentManager();

        // assertions
        assertNotNull( solrImpl.getEnvironmentManager() );
        assertEquals( environmentManager, solrImpl.getEnvironmentManager() );
    }


    @Test
    public void testGetTracker() throws Exception
    {
        solrImpl.getTracker();

        // assertions
        assertNotNull( solrImpl.getTracker() );
        assertEquals( tracker, solrImpl.getTracker() );
    }


    @Test
    public void testInstallCluster() throws Exception
    {
        when( solrClusterConfig.getClusterName() ).thenReturn( "test" );
        solrImpl.installCluster( solrClusterConfig );

        // assertions
        assertNotNull( solrImpl.installCluster( solrClusterConfig ) );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        solrImpl.getCluster( "test" );
        solrImpl.uninstallCluster( "test" );
    }


    @Test
    public void testUninstallCluster1() throws Exception
    {
        //        when( solrClusterConfig.getClusterName() ).thenReturn( "test" );
        //        UUID id = solrImpl.uninstallCluster( solrClusterConfig );
        //
        //        // assertions
        //        assertNotNull( solrImpl.uninstallCluster( solrClusterConfig ) );
        //        assertEquals( id, id );
    }


    @Test
    public void testGetClusters() throws Exception
    {
        List<SolrClusterConfig> myList = new ArrayList<>();
        myList.add( solrClusterConfig );
        when( pluginDAO.getInfo( SolrClusterConfig.PRODUCT_KEY, SolrClusterConfig.class ) ).thenReturn( myList );


        solrImpl.getClusters();

        // assertions
        assertNotNull( solrImpl.getClusters() );
        assertEquals( myList, solrImpl.getClusters() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        solrImpl.getCluster( "test" );

        // assertions
        assertNotNull( solrImpl.getCluster( "test" ) );
        assertEquals( solrClusterConfig, solrImpl.getCluster( "test" ) );
    }


    @Test
    public void testAddNode() throws Exception
    {
        solrImpl.addNode( "test", "test" );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        solrImpl.destroyNode( "test", "test" );
    }


    @Test
    public void testStartNode() throws Exception
    {
        solrImpl.startNode( "test", "test" );

        // assertions
        assertNotNull( solrImpl.startNode( "test", "test" ) );
    }


    @Test
    public void testStopNode() throws Exception
    {
        solrImpl.stopNode( "test", "test" );

        // assertions
        assertNotNull( solrImpl.stopNode( "test", "test" ) );
    }


    @Test
    public void testCheckNode() throws Exception
    {
        solrImpl.checkNode( "test", "test" );

        // assertions
        assertNotNull( solrImpl.checkNode( "test", "test" ) );
    }


    @Test
    public void testGetClusterSetupStrategy() throws Exception
    {
        solrImpl.getClusterSetupStrategy( environment, solrClusterConfig, trackerOperation );

        // assertions
        assertNotNull( solrImpl.getClusterSetupStrategy( environment, solrClusterConfig, trackerOperation ) );
    }


    @Test
    public void testConfigureEnvironmentCluster() throws Exception
    {
        solrImpl.configureEnvironmentCluster( solrClusterConfig );

        // assertions
        assertNotNull( solrImpl.configureEnvironmentCluster( solrClusterConfig ) );
    }


    @Test
    public void testOnEnvironmentCreated() throws Exception
    {
        solrImpl.onEnvironmentCreated( environment );
    }


    @Test
    public void testOnEnvironmentGrown() throws Exception
    {
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );

        solrImpl.onEnvironmentGrown( environment, mySet );
    }


    @Test
    public void testOnContainerDestroyed() throws Exception
    {
        List<SolrClusterConfig> myList = new ArrayList<>();
        myList.add( solrClusterConfig );
        when( pluginDAO.getInfo( SolrClusterConfig.PRODUCT_KEY, SolrClusterConfig.class ) ).thenReturn( myList );
        solrImpl.getClusters();
        when( environment.getId() ).thenReturn( id );
        when( solrClusterConfig.getEnvironmentId() ).thenReturn( id );
        Set<String> myUUID = new HashSet<>();
        myUUID.add( id );
        when( solrClusterConfig.getNodes() ).thenReturn( myUUID );
        when( pluginDAO.saveInfo( anyString(), anyString(), any() ) ).thenReturn( true );

        solrImpl.onContainerDestroyed( environment, id );

        // assertions
        verify( pluginDAO ).deleteInfo( anyString(), anyString() );
    }


    @Test
    public void testOnContainerDestroyedNotSaved()
    {
        List<SolrClusterConfig> myList = new ArrayList<>();
        myList.add( solrClusterConfig );
        when( pluginDAO.getInfo( SolrClusterConfig.PRODUCT_KEY, SolrClusterConfig.class ) ).thenReturn( myList );
        solrImpl.getClusters();
        when( environment.getId() ).thenReturn( id );
        when( solrClusterConfig.getEnvironmentId() ).thenReturn( id );
        Set<String> myUUID = new HashSet<>();
        myUUID.add( id );
        myUUID.add( UUID.randomUUID().toString() );
        when( solrClusterConfig.getNodes() ).thenReturn( myUUID );
        when( pluginDAO.saveInfo( anyString(), anyString(), any() ) ).thenReturn( false );

        solrImpl.onContainerDestroyed( environment, id );

        // assertions
        verify( pluginDAO ).saveInfo( anyString(), anyString(), any() );
    }


    @Test
    public void testOnEnvironmentDestroyed()
    {
        List<SolrClusterConfig> myList = new ArrayList<>();
        myList.add( solrClusterConfig );
        when( pluginDAO.getInfo( SolrClusterConfig.PRODUCT_KEY, SolrClusterConfig.class ) ).thenReturn( myList );
        solrImpl.getClusters();
        when( environment.getId() ).thenReturn( id );
        when( solrClusterConfig.getEnvironmentId() ).thenReturn( id );
        when( pluginDAO.deleteInfo( anyString(), anyString() ) ).thenReturn( true );

        solrImpl.onEnvironmentDestroyed( id );

        // assertions
        verify( pluginDAO ).deleteInfo( anyString(), anyString() );
    }
}