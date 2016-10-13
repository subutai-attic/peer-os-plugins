package io.subutai.plugin.storm.impl;


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
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.storm.api.StormClusterConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class StormImplTest
{
    @Mock
    StormClusterConfiguration stormClusterConfiguration;
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
    io.subutai.core.metric.api.Monitor monitor;
    private StormImpl stormImpl;
    private String id;


    @Before
    public void setUp() throws Exception
    {
        id = UUID.randomUUID().toString();
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );
        when( pluginDAO.getInfo( StormClusterConfiguration.PRODUCT_KEY, "test", StormClusterConfiguration.class ) )
                .thenReturn( stormClusterConfiguration );


        stormImpl = new StormImpl( monitor, pluginDAO );

        stormImpl.setTracker( tracker );
        stormImpl.setEnvironmentManager( environmentManager );
        stormImpl.setPluginDAO( pluginDAO );
        stormImpl.setMonitor( monitor );
        stormImpl.setExecutor( executorService );
        when( stormClusterConfiguration.getClusterName() ).thenReturn( "test" );
    }


    @Test
    public void testInstallCluster() throws Exception
    {
        stormImpl.installCluster( stormClusterConfiguration );

        // assertions
        assertNotNull( stormImpl.installCluster( stormClusterConfiguration ) );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        stormImpl.uninstallCluster( "test" );

        // assertions
        assertNotNull( stormImpl.uninstallCluster( "test" ) );
    }


    @Test
    public void testGetClusters() throws Exception
    {
        List<StormClusterConfiguration> myList = new ArrayList<>();
        myList.add( stormClusterConfiguration );
        when( pluginDAO.getInfo( StormClusterConfiguration.PRODUCT_KEY, StormClusterConfiguration.class ) )
                .thenReturn( myList );


        stormImpl.getClusters();

        // assertions
        assertNotNull( stormImpl.getClusters() );
        assertEquals( myList, stormImpl.getClusters() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        stormImpl.getCluster( "test" );

        // assertions
        assertNotNull( stormImpl.getCluster( "test" ) );
        assertEquals( stormClusterConfiguration, stormImpl.getCluster( "test" ) );
    }


    @Test
    public void testAddNode() throws Exception
    {
        stormImpl.addNode( "testClusterName", "testAgentHostName" );
    }


    @Test
    public void testCheckNode() throws Exception
    {
        stormImpl.checkNode( "test", "test" );

        // assertions
        assertNotNull( stormImpl.checkNode( "test", "test" ) );
    }


    @Test
    public void testStartNode() throws Exception
    {
        stormImpl.startNode( "test", "test" );

        // assertions
        assertNotNull( stormImpl.startNode( "test", "test" ) );
    }


    @Test
    public void testStopNode() throws Exception
    {
        stormImpl.stopNode( "test", "test" );

        // assertions
        assertNotNull( stormImpl.stopNode( "test", "test" ) );
    }


    @Test
    public void testRestartNode() throws Exception
    {
        stormImpl.restartNode( "test", "test" );

        // assertions
        assertNotNull( stormImpl.restartNode( "test", "test" ) );
    }


    @Test
    public void testAddNode1() throws Exception
    {
        stormImpl.addNode( "test" );

        // assertions
        assertNotNull( stormImpl.addNode( "test" ) );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        stormImpl.destroyNode( "test", "test" );

        // assertions
        assertNotNull( stormImpl.destroyNode( "test", "test" ) );
    }


    @Test
    public void testRemoveCluster() throws Exception
    {
        stormImpl.removeCluster( "test" );

        // assertions
        assertNotNull( stormImpl.removeCluster( "test" ) );
    }


    @Test
    public void testGetClusterSetupStrategy() throws Exception
    {
        stormImpl.getClusterSetupStrategy( stormClusterConfiguration, trackerOperation );

        // assertions
        assertNotNull( stormImpl.getClusterSetupStrategy( stormClusterConfiguration, trackerOperation ) );
    }


    @Test( expected = ClusterException.class )
    public void testSaveConfigNotSave() throws Exception
    {
        stormImpl.saveConfig( stormClusterConfiguration );
    }


    @Test
    public void testSaveConfig() throws Exception
    {
        when( pluginDAO.saveInfo( anyString(), anyString(), any() ) ).thenReturn( true );

        stormImpl.saveConfig( stormClusterConfiguration );
    }


    @Test
    public void testDeleteConfig() throws Exception
    {
        when( pluginDAO.deleteInfo( anyString(), anyString() ) ).thenReturn( true );

        stormImpl.deleteConfig( stormClusterConfiguration );
    }


    @Test( expected = ClusterException.class )
    public void testDeleteConfigNotDelete() throws Exception
    {
        stormImpl.deleteConfig( stormClusterConfiguration );
    }


    @Test
    public void testOnEnvironmentCreated() throws Exception
    {
        stormImpl.onEnvironmentCreated( environment );
    }


    @Test
    public void testOnEnvironmentGrown() throws Exception
    {
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        stormImpl.onEnvironmentGrown( environment, mySet );
    }


    @Test
    public void testOnContainerDestroyed() throws Exception
    {

        List<StormClusterConfiguration> myList = new ArrayList<>();
        myList.add( stormClusterConfiguration );
        when( pluginDAO.getInfo( StormClusterConfiguration.PRODUCT_KEY, StormClusterConfiguration.class ) )
                .thenReturn( myList );
        stormImpl.getClusters();
        when( environment.getId() ).thenReturn( id );
        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( id );
        Set<String> myUUID = new HashSet<>();
        myUUID.add( id );
        when( stormClusterConfiguration.getAllNodes() ).thenReturn( myUUID );

//        stormImpl.onContainerDestroyed( environment, id );
    }


    @Test
    public void testOnContainerDestroyedNotSaved()
    {
        List<StormClusterConfiguration> myList = new ArrayList<>();
        myList.add( stormClusterConfiguration );
        when( pluginDAO.getInfo( StormClusterConfiguration.PRODUCT_KEY, StormClusterConfiguration.class ) )
                .thenReturn( myList );
        stormImpl.getClusters();
        when( environment.getId() ).thenReturn( id );
        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( id );
        Set<String> myUUID = new HashSet<>();
        myUUID.add( id );
        myUUID.add( UUID.randomUUID().toString() );
        when( stormClusterConfiguration.getAllNodes() ).thenReturn( myUUID );

//        stormImpl.onContainerDestroyed( environment, id );
    }


    @Test
    public void testOnEnvironmentDestroyed()
    {
        List<StormClusterConfiguration> myList = new ArrayList<>();
        myList.add( stormClusterConfiguration );
        when( pluginDAO.getInfo( StormClusterConfiguration.PRODUCT_KEY, StormClusterConfiguration.class ) )
                .thenReturn( myList );
        stormImpl.getClusters();
        when( environment.getId() ).thenReturn( id );
        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( id );
        when( pluginDAO.deleteInfo( anyString(), anyString() ) ).thenReturn( true );

        stormImpl.onEnvironmentDestroyed( id );
    }
}