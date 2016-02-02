package io.subutai.plugin.flume.impl;


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

import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.flume.api.FlumeConfig;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class FlumeImplTest
{
    private FlumeImpl flumeImpl;
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

        flumeImpl = new FlumeImpl( tracker, environmentManager, hadoop, pluginDAO );
        flumeImpl.setExecutor( executor );
        flumeImpl.setPluginDao( pluginDAO );

        // mock InstallClusterHandler
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );
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
        flumeImpl.installCluster( flumeConfig );

        // assertions
        assertNotNull( flumeImpl.installCluster( flumeConfig ) );
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
        flumeImpl.installCluster( flumeConfig );

        // assertions
        assertNotNull( flumeImpl.installCluster( flumeConfig ) );
    }


    @Test
    public void testStartNode() throws Exception
    {
        flumeImpl.startNode( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.startNode( "test", "test" ) );
    }


    @Test
    public void testStopNode() throws Exception
    {
        flumeImpl.stopNode( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.stopNode( "test", "test" ) );
    }


    @Test
    public void testCheckNode() throws Exception
    {
        flumeImpl.checkNode( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.checkNode( "test", "test" ) );
    }


    @Test
    public void testCheckServiceStatus() throws Exception
    {
        flumeImpl.checkServiceStatus( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.checkServiceStatus( "test", "test" ) );
    }


    @Test
    public void testAddNode() throws Exception
    {
        flumeImpl.addNode( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.addNode( "test", "test" ) );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        flumeImpl.destroyNode( "test", "test" );

        // assertions
        assertNotNull( flumeImpl.destroyNode( "test", "test" ) );
    }


    @Test
    public void testUninstallCluster1() throws Exception
    {
        flumeImpl.uninstallCluster( "test" );

        // assertions
        assertNotNull( flumeImpl.uninstallCluster( "test" ) );
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