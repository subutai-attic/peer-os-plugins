package org.safehaus.subutai.plugin.mahout.impl;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class MahoutImplTest
{
    private MahoutImpl mahoutImpl;
    private UUID uuid;
    @Mock
    MahoutClusterConfig mahoutClusterConfig;
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
    Hadoop hadoop;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
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


    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID( 50, 50 );
        // mock InstallClusterHandler
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );
        when( pluginDAO.getInfo( MahoutClusterConfig.PRODUCT_KEY, "test", MahoutClusterConfig.class ) )
                .thenReturn( mahoutClusterConfig );

        // mock init
        when( dataSource.getConnection() ).thenReturn( connection );
        when( connection.prepareStatement( any( String.class ) ) ).thenReturn( preparedStatement );
        when( preparedStatement.executeQuery() ).thenReturn( resultSet );
        when( resultSet.getMetaData() ).thenReturn( resultSetMetaData );
        when( resultSetMetaData.getColumnCount() ).thenReturn( 1 );

        mahoutImpl = new MahoutImpl( tracker, environmentManager, hadoop );
        mahoutImpl.setTracker( tracker );
        mahoutImpl.setExecutor( executorService );
        mahoutImpl.setPluginDAO( pluginDAO );
        mahoutImpl.setEnvironmentManager( environmentManager );
        mahoutImpl.setHadoopManager( hadoop );
    }


    @Test
    public void testGetPluginDAO() throws Exception
    {
        mahoutImpl.getPluginDAO();

        // assertions
        assertEquals( pluginDAO, mahoutImpl.getPluginDAO() );
        assertNotNull( mahoutImpl.getPluginDAO() );
    }


    @Test
    public void testGetCommands() throws Exception
    {
        mahoutImpl.getCommands();
    }


    @Test
    public void testGetExecutor() throws Exception
    {
        mahoutImpl.getExecutor();

        // assertions
        assertEquals( executorService, mahoutImpl.getExecutor() );
        assertNotNull( mahoutImpl.getExecutor() );
    }


    @Test
    public void testGetEnvironmentManager() throws Exception
    {
        mahoutImpl.getEnvironmentManager();

        // assertions
        assertEquals( environmentManager, mahoutImpl.getEnvironmentManager() );
        assertNotNull( mahoutImpl.getEnvironmentManager() );
    }


    @Test
    public void testGetTracker() throws Exception
    {
        mahoutImpl.getTracker();

        // assertions
        assertEquals( tracker, mahoutImpl.getTracker() );
        assertNotNull( mahoutImpl.getTracker() );
    }


    @Test
    public void testGetHadoopManager() throws Exception
    {
        mahoutImpl.getHadoopManager();

        // assertions
        assertEquals( hadoop, mahoutImpl.getHadoopManager() );
        assertNotNull( mahoutImpl.getHadoopManager() );
    }


    @Test
    public void testInit() throws Exception
    {

    }


    @Test
    public void testDestroy() throws Exception
    {
        mahoutImpl.destroy();
    }


    @Test
    public void testInstallCluster() throws Exception
    {
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
        UUID id = mahoutImpl.installCluster( mahoutClusterConfig );

        // assertions
        assertNotNull( mahoutImpl.installCluster( mahoutClusterConfig ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testInstallCluster1() throws Exception
    {
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
        UUID id = mahoutImpl.installCluster( mahoutClusterConfig );

        // assertions
        assertNotNull( mahoutImpl.installCluster( mahoutClusterConfig ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        UUID id = mahoutImpl.uninstallCluster( "test" );

        // assertions
        assertNotNull( mahoutImpl.uninstallCluster( "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testUninstalllNode() throws Exception
    {
        UUID id = mahoutImpl.uninstallNode( "test", "test" );

        // assertions
        assertNotNull( mahoutImpl.uninstallNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testGetClusters() throws Exception
    {
        List<MahoutClusterConfig> myList = new ArrayList<>();
        myList.add( mahoutClusterConfig );
        when( pluginDAO.getInfo( MahoutClusterConfig.PRODUCT_KEY, MahoutClusterConfig.class ) ).thenReturn( myList );


        mahoutImpl.getClusters();

        // assertions
        assertNotNull( mahoutImpl.getClusters() );
        assertEquals( myList, mahoutImpl.getClusters() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        mahoutImpl.getCluster( "test" );

        // assertions
        assertNotNull( mahoutImpl.getCluster( "test" ) );
        assertEquals( mahoutClusterConfig, mahoutImpl.getCluster( "test" ) );
    }


    @Test
    public void testAddNode() throws Exception
    {
        UUID id = mahoutImpl.addNode( "test", "test" );

        // assertions
        assertNotNull( mahoutImpl.addNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testCheckNode() throws Exception
    {
        UUID id = mahoutImpl.checkNode( "test", "test" );
    }


    @Test
    public void testGetClusterSetupStrategy() throws Exception
    {
        mahoutImpl.getClusterSetupStrategy( mahoutClusterConfig, trackerOperation );

        // assertions
        assertNotNull( mahoutImpl.getClusterSetupStrategy( mahoutClusterConfig, trackerOperation ) );
    }
}