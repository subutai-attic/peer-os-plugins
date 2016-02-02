package io.subutai.plugin.hipi.impl;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hipi.api.HipiConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class HipiImplTest
{
    @Mock
    CommandResult commandResult;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    HipiConfig hipiConfig;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    Hadoop hadoop;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    ExecutorService executorService;
    private HipiImpl hipiImpl;
    private String id;


    @Before
    public void setUp() throws Exception
    {
        id = UUID.randomUUID().toString();
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );
        when( pluginDAO.getInfo( HipiConfig.PRODUCT_KEY, "test", HipiConfig.class ) ).thenReturn( hipiConfig );


        hipiImpl = new HipiImpl( tracker, environmentManager, hadoop, pluginDAO );

        hipiImpl.setPluginDao( pluginDAO );
        hipiImpl.setExecutor( executorService );
    }


    @Test
    public void testGetTracker() throws Exception
    {
        hipiImpl.getTracker();

        // assertions
        assertNotNull( hipiImpl.getTracker() );
        assertEquals( tracker, hipiImpl.getTracker() );
    }


    @Test
    public void testGetPluginDao() throws Exception
    {
        hipiImpl.getPluginDao();

        // assertions
        assertNotNull( hipiImpl.getPluginDao() );
        assertEquals( pluginDAO, hipiImpl.getPluginDao() );
    }


    @Test
    public void testGetEnvironmentManager() throws Exception
    {
        hipiImpl.getEnvironmentManager();
    }


    @Test
    public void testGetHadoopManager() throws Exception
    {
        hipiImpl.getHadoopManager();
    }


    @Test
    public void testInit() throws Exception
    {

    }


    @Test
    public void testDestroy() throws Exception
    {
        hipiImpl.destroy();
    }


    @Test
    public void testInstallCluster() throws Exception
    {
        when( hipiConfig.getClusterName() ).thenReturn( "test" );
        hipiImpl.installCluster( hipiConfig );

        // assertions
        assertNotNull( hipiImpl.installCluster( hipiConfig ) );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        hipiImpl.getCluster( "test" );

        hipiImpl.uninstallCluster( "test" );

        // assertions
        assertNotNull( hipiImpl.uninstallCluster( "test" ) );
    }


    @Test
    public void testGetClusters() throws Exception
    {
        List<HipiConfig> myList = new ArrayList<>();
        myList.add( hipiConfig );
        when( pluginDAO.getInfo( HipiConfig.PRODUCT_KEY, HipiConfig.class ) ).thenReturn( myList );


        hipiImpl.getClusters();

        // assertions
        assertNotNull( hipiImpl.getClusters() );
        assertEquals( myList, hipiImpl.getClusters() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        hipiImpl.getCluster( "test" );

        // assertions
        assertNotNull( hipiImpl.getCluster( "test" ) );
        assertEquals( hipiConfig, hipiImpl.getCluster( "test" ) );
    }


    @Test
    public void testAddNode() throws Exception
    {
        hipiImpl.addNode( "test", "test" );

        // assertions
        assertNotNull( hipiImpl.addNode( "test", "test" ) );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        hipiImpl.destroyNode( "test", "test" );

        // assertions
        assertNotNull( hipiImpl.destroyNode( "test", "test" ) );
    }


    @Test
    public void testGetClusterSetupStrategy() throws Exception
    {
        hipiImpl.getClusterSetupStrategy( hipiConfig, trackerOperation );

        // assertions
        assertNotNull( hipiImpl.getClusterSetupStrategy( hipiConfig, trackerOperation ) );
    }


    @Test( expected = ClusterException.class )
    public void testSaveConfigNotSave() throws Exception
    {
        hipiImpl.saveConfig( hipiConfig );
    }


    @Test
    public void testSaveConfig() throws Exception
    {
        when( pluginDAO.saveInfo( anyString(), anyString(), any() ) ).thenReturn( true );

        hipiImpl.saveConfig( hipiConfig );
    }


    @Test
    public void testDeleteConfig() throws Exception
    {
        when( pluginDAO.deleteInfo( anyString(), anyString() ) ).thenReturn( true );

        hipiImpl.deleteConfig( hipiConfig );
    }


    @Test( expected = ClusterException.class )
    public void testDeleteConfigNotDelete() throws Exception
    {
        hipiImpl.deleteConfig( hipiConfig );
    }


    @Test
    public void testOnEnvironmentCreated() throws Exception
    {
        hipiImpl.onEnvironmentCreated( environment );
    }


    @Test
    public void testOnEnvironmentGrown() throws Exception
    {
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        hipiImpl.onEnvironmentGrown( environment, mySet );
    }


    @Test
    public void testOnContainerDestroyed() throws Exception
    {
        List<HipiConfig> myList = new ArrayList<>();
        myList.add( hipiConfig );
        hipiImpl.getClusters();
        when( environment.getId() ).thenReturn( id );
        when( hipiConfig.getEnvironmentId() ).thenReturn( id );
        Set<String> myUUID = new HashSet<>();
        myUUID.add( id );
        when( hipiConfig.getNodes() ).thenReturn( myUUID );

        hipiImpl.onContainerDestroyed( environment, id );
    }


    @Test
    public void testOnContainerDestroyedNotSaved()
    {
        List<HipiConfig> myList = new ArrayList<>();
        myList.add( hipiConfig );
        hipiImpl.getClusters();
        when( environment.getId() ).thenReturn( id );
        when( hipiConfig.getEnvironmentId() ).thenReturn( id );
        Set<String> myUUID = new HashSet<>();
        myUUID.add( id );
        myUUID.add( UUID.randomUUID().toString() );
        when( hipiConfig.getNodes() ).thenReturn( myUUID );

        hipiImpl.onContainerDestroyed( environment, id );
    }


    @Test
    public void testOnEnvironmentDestroyed()
    {
        List<HipiConfig> myList = new ArrayList<>();
        myList.add( hipiConfig );
        hipiImpl.getClusters();
        when( environment.getId() ).thenReturn( id );
        when( hipiConfig.getEnvironmentId() ).thenReturn( id );
        when( pluginDAO.deleteInfo( anyString(), anyString() ) ).thenReturn( true );

        hipiImpl.onEnvironmentDestroyed( id );
    }
}