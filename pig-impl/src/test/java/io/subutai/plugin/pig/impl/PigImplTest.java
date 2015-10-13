package io.subutai.plugin.pig.impl;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.pig.api.PigConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class PigImplTest
{
    private PigImpl pigImpl;
    @Mock
    PigConfig pigConfig;
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
        // mock init
        when( dataSource.getConnection() ).thenReturn( connection );
        when( connection.prepareStatement( any( String.class ) ) ).thenReturn( preparedStatement );
        when( preparedStatement.executeQuery() ).thenReturn( resultSet );
        when( resultSet.getMetaData() ).thenReturn( resultSetMetaData );
        when( resultSetMetaData.getColumnCount() ).thenReturn( 1 );


        pigImpl = new PigImpl( tracker, environmentManager, hadoop, pluginDAO );


        // mock InstallClusterHandler
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );
        when( pluginDAO.getInfo( PigConfig.PRODUCT_KEY, "test", PigConfig.class ) ).thenReturn( pigConfig );

        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );

        // asserts
        assertEquals( connection, dataSource.getConnection() );
        assertEquals( preparedStatement, connection.prepareStatement( any( String.class ) ) );
        assertEquals( resultSet, preparedStatement.executeQuery() );
        assertEquals( resultSetMetaData, resultSet.getMetaData() );
        assertNotNull( resultSetMetaData.getColumnCount() );
    }


    @Test
    public void testGetEnvironmentManager() throws Exception
    {
        pigImpl.getEnvironmentManager();

        // assertions
        assertEquals( environmentManager, pigImpl.getEnvironmentManager() );
        assertNotNull( pigImpl.getEnvironmentManager() );
    }


    @Test
    public void testGetHadoopManager() throws Exception
    {
        pigImpl.getHadoopManager();

        // assertions
        assertEquals( hadoop, pigImpl.getHadoopManager() );
        assertNotNull( pigImpl.getHadoopManager() );
    }


    @Test
    public void testDestroy() throws Exception
    {
        //        pigImpl.destroy();
    }

    //    @Ignore
    //    @Test
    //    public void testGetCommands() throws Exception
    //    {
    //        pigImpl.getCommands();
    //
    //        // assertions
    //        assertEquals(commands,pigImpl.getCommands());
    //        assertNotNull(pigImpl.getCommands());
    //
    //    }
    //
    //    @Test
    //    public void testGetTracker() throws Exception
    //    {
    //        pigImpl.getTracker();
    //
    //        // assertions
    //        assertEquals(tracker,pigImpl.getTracker());
    //        assertNotNull(pigImpl.getTracker());
    //
    //    }
    //
    //    @Test
    //    public void testGetPluginDao() throws Exception
    //    {
    //        pigImpl.getPluginDao();
    //
    //        // assertions
    //        assertEquals(pluginDAO,pigImpl.getPluginDao());
    //        assertNotNull(pigImpl.getPluginDao());
    //
    //    }
    //
    //    @Test
    //    public void testInstallCluster() throws Exception
    //    {
    //        UUID id = pigImpl.installCluster(pigConfig);
    //
    //        // assertions
    //        assertNotNull(pigImpl.installCluster(pigConfig));
    //        assertEquals(uuid, id);
    //
    //    }
    //
    //
    //    @Ignore
    //    @Test
    //    public void testInstallCluster1() throws Exception
    //    {
    //        UUID id = pigImpl.installCluster(pigConfig,hadoopClusterConfig);
    //
    //        // assertions
    //        assertNotNull(pigImpl.installCluster(pigConfig, hadoopClusterConfig));
    //        assertEquals(uuid, id);
    //
    //    }
    //
    //    @Test
    //    public void testUninstallCluster() throws Exception
    //    {
    //        UUID id = pigImpl.uninstallCluster(pigConfig);
    //
    //        // assertions
    //        assertNotNull(pigImpl.uninstallCluster(pigConfig));
    //        assertEquals(uuid,id);
    //    }
    //
    //
    //    @Ignore
    //    @Test
    //    public void testGetDefaultEnvironmentBlueprint() throws Exception
    //    {
    //        pigImpl.getDefaultEnvironmentBlueprint(pigConfig);
    //
    //        // assertions
    //        assertNotNull(pigImpl.getDefaultEnvironmentBlueprint(pigConfig));
    //
    //    }
    //
    //    @Test
    //    public void testUninstallCluster1() throws Exception
    //    {
    //        pigImpl.uninstallCluster("test");
    //    }
    //
    //    @Test
    //    public void testDestroyNode() throws Exception
    //    {
    //        UUID id = pigImpl.destroyNode("test", "test");
    //
    //        // assertions
    //        assertNotNull(pigImpl.destroyNode("test", "test"));
    //        assertEquals(uuid, id);
    //
    //    }
    //
    //    @Test
    //    public void testAddNode() throws Exception
    //    {
    //        UUID id = pigImpl.addNode("test", "test");
    //
    //        // assertions
    //        assertNotNull(pigImpl.addNode("test", "test"));
    //        assertEquals(uuid,id);
    //
    //    }
    //
    //    @Ignore
    //    @Test
    //    public void testGetClusterSetupStrategyWithHadoop() throws Exception
    //    {
    //        when(pigConfig.getSetupType()).thenReturn(SetupType.WITH_HADOOP);
    //
    //        pigImpl.getClusterSetupStrategy(environment,pigConfig,trackerOperation);
    //
    //        // assertions
    //        assertNotNull(pigImpl.getClusterSetupStrategy(environment, pigConfig, trackerOperation));
    //    }
    //
    //    @Test
    //    public void testGetClusterSetupStrategyOverHadoop() throws Exception
    //    {
    //        when(pigConfig.getSetupType()).thenReturn(SetupType.OVER_HADOOP);
    //
    //        pigImpl.getClusterSetupStrategy(environment,pigConfig,trackerOperation);
    //
    //        // assertions
    //        assertNotNull(pigImpl.getClusterSetupStrategy(environment, pigConfig, trackerOperation));
    //    }
    //
    //    @Ignore
    //    @Test
    //    public void testGetClusterSetupStrategy() throws Exception
    //    {
    //        pigImpl.getClusterSetupStrategy(environment,pigConfig,trackerOperation);
    //    }
    //
    //    @Test
    //    public void testGetClusters() throws Exception
    //    {
    //        List<PigConfig> myList = new ArrayList<>();
    //        myList.add(pigConfig);
    //        when(pluginDAO.getInfo(PigConfig.PRODUCT_KEY, PigConfig.class)).thenReturn(myList);
    //
    //
    //        pigImpl.getClusters();
    //
    //        // assertions
    //        assertNotNull(pigImpl.getClusters());
    //        assertEquals(myList, pigImpl.getClusters());
    //
    //    }
    //
    //    @Test
    //    public void testGetCluster() throws Exception
    //    {
    //        pigImpl.getCluster("test");
    //
    //        // assertions
    //        assertNotNull(pigImpl.getCluster("test"));
    //        assertEquals(pigConfig, pigImpl.getCluster("test"));
    //
    //    }
}