package org.safehaus.subutai.plugin.flume.impl;


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
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.flume.api.FlumeConfig;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class FlumeImplTest
{
    private FlumeImpl flumeImpl;
    private UUID uuid;
    @Mock
    FlumeConfig flumeConfig;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    CommandResult commandResult;
    @Mock
    DataSource dataSource;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    ExecutorService executor;
    @Mock
    Commands commands;
    @Mock
    Hadoop hadoop;
    @Mock
    Connection connection;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    ResultSet resultSet;
    @Mock
    ResultSetMetaData resultSetMetaData;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;


    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID( 50, 50 );

        flumeImpl = new FlumeImpl( tracker, environmentManager, hadoop );
        flumeImpl.setExecutor( executor );
        flumeImpl.setPluginDao( pluginDAO );

        // mock InstallClusterHandler
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );
        when( pluginDAO.getInfo( FlumeConfig.PRODUCT_KEY, "test", FlumeConfig.class ) ).thenReturn( flumeConfig );

        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );

    }


    @Test
    public void testGetTracker() throws Exception
    {
        flumeImpl.getTracker();

        // assertions
        assertNotNull( flumeImpl.getTracker() );
        assertEquals( tracker, flumeImpl.getTracker() );
    }


    @Test
    public void testGetPluginDao() throws Exception
    {
        flumeImpl.getPluginDao();

        // assertions
        assertNotNull( flumeImpl.getPluginDao() );
        assertEquals( pluginDAO, flumeImpl.getPluginDao() );
    }


    @Test
    public void testGetEnvironmentManager() throws Exception
    {
        flumeImpl.getEnvironmentManager();

        // assertions
        assertNotNull( flumeImpl.getEnvironmentManager() );
        assertEquals( environmentManager, flumeImpl.getEnvironmentManager() );
    }


    @Test
    public void testGetHadoopManager() throws Exception
    {
        flumeImpl.getHadoopManager();

        // assertions
        assertNotNull( flumeImpl.getHadoopManager() );
        assertEquals( hadoop, flumeImpl.getHadoopManager() );
    }


    @Test
    public void testDestroy() throws Exception
    {
        flumeImpl.destroy();
    }


    @Test
    public void testInstallCluster() throws Exception
    {
        UUID id = flumeImpl.installCluster( flumeConfig );

        // assertions
        assertNotNull( flumeImpl.installCluster( flumeConfig ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        flumeImpl.uninstallCluster( "test" );
    }


    @Test
    public void testGetClusters() throws Exception
    {
        List<FlumeConfig> myList = new ArrayList<>();
        myList.add( flumeConfig );
        when( pluginDAO.getInfo( FlumeConfig.PRODUCT_KEY, FlumeConfig.class ) ).thenReturn( myList );


        flumeImpl.getClusters();

        // assertions
        assertNotNull( flumeImpl.getClusters() );
        assertEquals( myList, flumeImpl.getClusters() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        flumeImpl.getCluster( "test" );

        // assertions
        assertNotNull( flumeImpl.getCluster( "test" ) );
        assertEquals( flumeConfig, flumeImpl.getCluster( "test" ) );
    }


    @Test
    public void testInstallCluster1() throws Exception
    {
        UUID id = flumeImpl.installCluster( flumeConfig);

        // assertions
        assertNotNull( flumeImpl.installCluster( flumeConfig ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testStartNode() throws Exception
    {
        UUID id = flumeImpl.startNode( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.startNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testStopNode() throws Exception
    {
        UUID id = flumeImpl.stopNode( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.stopNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testCheckNode() throws Exception
    {
        UUID id = flumeImpl.checkNode( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.checkNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testCheckServiceStatus() throws Exception
    {
        UUID id = flumeImpl.checkServiceStatus( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.checkServiceStatus( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testAddNode() throws Exception
    {
        UUID id = flumeImpl.addNode( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.addNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        UUID id = flumeImpl.destroyNode( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.destroyNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testUninstallCluster1() throws Exception
    {
        UUID id = flumeImpl.uninstallCluster( "test" );

        // assertions
        assertNotNull( flumeImpl.uninstallCluster( "test" ) );
        assertEquals( uuid, id );
    }




    @Test
    public void testGetClusterSetupStrategyOverHadoop() throws Exception
    {

        flumeImpl.getClusterSetupStrategy( flumeConfig, trackerOperation );

        // assertions
        assertNotNull( flumeImpl.getClusterSetupStrategy( flumeConfig, trackerOperation ) );
    }


    @Test
    public void testGetClusterSetupStrategy() throws Exception
    {
        flumeImpl.getClusterSetupStrategy( flumeConfig, trackerOperation );
    }
}