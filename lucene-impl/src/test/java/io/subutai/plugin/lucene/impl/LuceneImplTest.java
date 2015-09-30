package io.subutai.plugin.lucene.impl;


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
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.lucene.api.LuceneConfig;
import io.subutai.plugin.lucene.impl.LuceneImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class LuceneImplTest
{
    private LuceneImpl luceneImpl;
    private UUID uuid;
    @Mock
    ContainerHost containerHost;
    @Mock
    LuceneConfig luceneConfig;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    Hadoop hadoop;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    CommandResult commandResult;
    @Mock
    ExecutorService executor;


    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID( 50, 50 );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );
        when( pluginDAO.getInfo( LuceneConfig.PRODUCT_KEY, "test", LuceneConfig.class ) ).thenReturn( luceneConfig );

        luceneImpl = new LuceneImpl( tracker, environmentManager, hadoop, pluginDAO );
        luceneImpl.setTracker( tracker );
        luceneImpl.setHadoopManager( hadoop );
        luceneImpl.setEnvironmentManager( environmentManager );
        luceneImpl.setPluginDao( pluginDAO );
        luceneImpl.setExecutor( executor );
    }


    @Test
    public void testGetHadoopManager() throws Exception
    {
        luceneImpl.getHadoopManager();

        // assertions
        assertNotNull( luceneImpl.getHadoopManager() );
        assertEquals( hadoop, luceneImpl.getHadoopManager() );
    }


    @Test
    public void testGetTracker() throws Exception
    {
        luceneImpl.getTracker();

        // assertions
        assertNotNull( luceneImpl.getTracker() );
        assertEquals( tracker, luceneImpl.getTracker() );
    }


    @Test
    public void testGetPluginDao() throws Exception
    {
        luceneImpl.getPluginDao();

        // assertions
        assertNotNull( luceneImpl.getPluginDao() );
        assertEquals( pluginDAO, luceneImpl.getPluginDao() );
    }


    @Test
    public void testGetEnvironmentManager() throws Exception
    {
        luceneImpl.getEnvironmentManager();

        // assertions
        assertNotNull( luceneImpl.getEnvironmentManager() );
        assertEquals( environmentManager, luceneImpl.getEnvironmentManager() );
    }


    @Test
    public void testInit() throws Exception
    {

    }


    @Test
    public void testDestroy() throws Exception
    {
        luceneImpl.destroy();
    }


    @Test
    public void testInstallCluster() throws Exception
    {
        when( luceneConfig.getClusterName() ).thenReturn( "test" );
        UUID id = luceneImpl.installCluster( luceneConfig );

        // assertions
        assertNotNull( luceneImpl.installCluster( luceneConfig ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        UUID id = luceneImpl.uninstallCluster( "test" );

        // assertions
        assertNotNull( luceneImpl.uninstallCluster( "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testGetClusters() throws Exception
    {
        List<LuceneConfig> myList = new ArrayList<>();
        myList.add( luceneConfig );
        when( pluginDAO.getInfo( LuceneConfig.PRODUCT_KEY, LuceneConfig.class ) ).thenReturn( myList );


        luceneImpl.getClusters();

        // assertions
        assertNotNull( luceneImpl.getClusters() );
        assertEquals( myList, luceneImpl.getClusters() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        luceneImpl.getCluster( "test" );

        // assertions
        assertNotNull( luceneImpl.getCluster( "test" ) );
        assertEquals( luceneConfig, luceneImpl.getCluster( "test" ) );
    }


    @Test
    public void testAddNode() throws Exception
    {
        UUID id = luceneImpl.addNode( "test", "test" );

        // assertions
        assertNotNull( luceneImpl.addNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testUninstallNode() throws Exception
    {
        UUID id = luceneImpl.uninstallNode( "test", "test" );

        // assertions
        assertNotNull( luceneImpl.uninstallNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testGetClusterSetupStrategy() throws Exception
    {
        luceneImpl.getClusterSetupStrategy( environment, luceneConfig, trackerOperation );

        // assertions
        assertNotNull( luceneImpl.getClusterSetupStrategy( environment, luceneConfig, trackerOperation ) );
    }


    @Test
    public void testOnEnvironmentCreated() throws Exception
    {
        luceneImpl.onEnvironmentCreated( environment );
    }


    @Test
    public void testOnEnvironmentGrown() throws Exception
    {
        Set<ContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        luceneImpl.onEnvironmentGrown( environment, mySet );
    }


    @Test
    public void testOnContainerDestroyed() throws Exception
    {
        List<LuceneConfig> myList = new ArrayList<>();
        myList.add( luceneConfig );
        when( pluginDAO.getInfo( LuceneConfig.PRODUCT_KEY, LuceneConfig.class ) ).thenReturn( myList );
        luceneImpl.getClusters();
        when( environment.getId() ).thenReturn( uuid );
        when( luceneConfig.getEnvironmentId() ).thenReturn( uuid );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( uuid );
        when( luceneConfig.getNodes() ).thenReturn( myUUID );
        when( pluginDAO.saveInfo( anyString(), anyString(), any() ) ).thenReturn( true );

        luceneImpl.onContainerDestroyed( environment, uuid );
    }


    @Test
    public void testOnEnvironmentDestroyed()
    {
        List<LuceneConfig> myList = new ArrayList<>();
        myList.add( luceneConfig );
        when( pluginDAO.getInfo( LuceneConfig.PRODUCT_KEY, LuceneConfig.class ) ).thenReturn( myList );
        luceneImpl.getClusters();
        when( environment.getId() ).thenReturn( uuid );
        when( luceneConfig.getEnvironmentId() ).thenReturn( uuid );
        when( pluginDAO.deleteInfo( anyString(), anyString() ) ).thenReturn( true );

        luceneImpl.onEnvironmentDestroyed( uuid );
    }


    @Test( expected = ClusterException.class )
    public void testNotSaveConfig() throws ClusterException
    {
        luceneImpl.saveConfig( luceneConfig );
    }


    @Test( expected = ClusterException.class )
    public void testNotDeleteConfig() throws ClusterException
    {
        luceneImpl.deleteConfig( luceneConfig );
    }
}