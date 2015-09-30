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
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.solr.api.SolrClusterConfig;
import io.subutai.plugin.solr.impl.Commands;
import io.subutai.plugin.solr.impl.SolrImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class SolrImplTest
{
    private SolrImpl solrImpl;
    private UUID uuid;
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
    ContainerHost containerHost;
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
        uuid = new UUID( 50, 50 );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );
        when( pluginDAO.getInfo( SolrClusterConfig.PRODUCT_KEY, "test", SolrClusterConfig.class ) )
                .thenReturn( solrClusterConfig );

        solrImpl = new SolrImpl(pluginDAO);

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
        UUID id = solrImpl.installCluster( solrClusterConfig );

        // assertions
        assertNotNull( solrImpl.installCluster( solrClusterConfig ) );
        assertEquals( uuid, id );
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
        //        assertEquals( uuid, id );
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
        UUID id = solrImpl.destroyNode( "test", "test" );
    }


    @Test
    public void testStartNode() throws Exception
    {
        UUID id = solrImpl.startNode( "test", "test" );

        // assertions
        assertNotNull( solrImpl.startNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testStopNode() throws Exception
    {
        UUID id = solrImpl.stopNode( "test", "test" );

        // assertions
        assertNotNull( solrImpl.stopNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testCheckNode() throws Exception
    {
        UUID id = solrImpl.checkNode( "test", "test" );

        // assertions
        assertNotNull( solrImpl.checkNode( "test", "test" ) );
        assertEquals( uuid, id );
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
        UUID id = solrImpl.configureEnvironmentCluster( solrClusterConfig );

        // assertions
        assertNotNull( solrImpl.configureEnvironmentCluster( solrClusterConfig ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testOnEnvironmentCreated() throws Exception
    {
        solrImpl.onEnvironmentCreated( environment );
    }


    @Test
    public void testOnEnvironmentGrown() throws Exception
    {
        Set<ContainerHost> mySet = new HashSet<>();
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
        when( environment.getId() ).thenReturn( uuid );
        when( solrClusterConfig.getEnvironmentId() ).thenReturn( uuid );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( uuid );
        when( solrClusterConfig.getNodes() ).thenReturn( myUUID );
        when( pluginDAO.saveInfo( anyString(), anyString(), any() ) ).thenReturn( true );

        solrImpl.onContainerDestroyed( environment, uuid );

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
        when( environment.getId() ).thenReturn( uuid );
        when( solrClusterConfig.getEnvironmentId() ).thenReturn( uuid );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( uuid );
        myUUID.add( UUID.randomUUID() );
        when( solrClusterConfig.getNodes() ).thenReturn( myUUID );
        when( pluginDAO.saveInfo( anyString(), anyString(), any() ) ).thenReturn( false );

        solrImpl.onContainerDestroyed( environment, uuid );

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
        when( environment.getId() ).thenReturn( uuid );
        when( solrClusterConfig.getEnvironmentId() ).thenReturn( uuid );
        when( pluginDAO.deleteInfo( anyString(), anyString() ) ).thenReturn( true );

        solrImpl.onEnvironmentDestroyed( uuid );

        // assertions
        verify( pluginDAO ).deleteInfo( anyString(), anyString() );
    }
}