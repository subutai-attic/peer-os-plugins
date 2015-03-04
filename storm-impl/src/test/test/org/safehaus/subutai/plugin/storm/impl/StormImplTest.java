package org.safehaus.subutai.plugin.storm.impl;


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
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class StormImplTest
{
    @Mock StormClusterConfiguration stormClusterConfiguration;
    @Mock Commands commands;
    @Mock Tracker tracker;
    @Mock EnvironmentManager environmentManager;
    @Mock TrackerOperation trackerOperation;
    @Mock Environment environment;
    @Mock ContainerHost containerHost;
    @Mock CommandResult commandResult;
    @Mock ClusterSetupStrategy clusterSetupStrategy;
    @Mock PluginDAO pluginDAO;
    @Mock RequestBuilder requestBuilder;
    @Mock ExecutorService executorService;
    @Mock org.safehaus.subutai.core.metric.api.Monitor monitor;
    private StormImpl stormImpl;
    private UUID uuid;


    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID( 50, 50 );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );
        when( pluginDAO.getInfo( StormClusterConfiguration.PRODUCT_KEY, "test", StormClusterConfiguration.class ) )
                .thenReturn( stormClusterConfiguration );


        stormImpl = new StormImpl();

        stormImpl.setTracker( tracker );
        stormImpl.setEnvironmentManager( environmentManager );
        stormImpl.setPluginDAO( pluginDAO );
        stormImpl.setMonitor( monitor );
        stormImpl.setExecutor( executorService );
    }


    @Test
    public void testInstallCluster() throws Exception
    {
        UUID id = stormImpl.installCluster( stormClusterConfiguration );

        // assertions
        assertNotNull( stormImpl.installCluster( stormClusterConfiguration ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        UUID id = stormImpl.uninstallCluster( "test" );

        // assertions
        assertNotNull( stormImpl.uninstallCluster( "test" ) );
        assertEquals( uuid, id );
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
        UUID id = stormImpl.checkNode( "test", "test" );

        // assertions
        assertNotNull( stormImpl.checkNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testStartNode() throws Exception
    {
        UUID id = stormImpl.startNode( "test", "test" );

        // assertions
        assertNotNull( stormImpl.startNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testStopNode() throws Exception
    {
        UUID id = stormImpl.stopNode( "test", "test" );

        // assertions
        assertNotNull( stormImpl.stopNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testRestartNode() throws Exception
    {
        UUID id = stormImpl.restartNode( "test", "test" );

        // assertions
        assertNotNull( stormImpl.restartNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testAddNode1() throws Exception
    {
        UUID id = stormImpl.addNode( "test" );

        // assertions
        assertNotNull( stormImpl.addNode( "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        UUID id = stormImpl.destroyNode( "test", "test" );

        // assertions
        assertNotNull( stormImpl.destroyNode( "test", "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testRemoveCluster() throws Exception
    {
        UUID id = stormImpl.removeCluster( "test" );

        // assertions
        assertNotNull( stormImpl.removeCluster( "test" ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testGetClusterSetupStrategy() throws Exception
    {
        stormImpl.getClusterSetupStrategy( stormClusterConfiguration, trackerOperation );

        // assertions
        assertNotNull( stormImpl.getClusterSetupStrategy( stormClusterConfiguration, trackerOperation ) );
    }


    @Test
    public void testConfigureEnvironmentCluster() throws Exception
    {
        when( stormClusterConfiguration.getClusterName() ).thenReturn( "test" );

        UUID id = stormImpl.configureEnvironmentCluster( stormClusterConfiguration );

        // assertions
        assertNotNull( stormImpl.configureEnvironmentCluster( stormClusterConfiguration ) );
        assertEquals( uuid, id );
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
        Set<ContainerHost> mySet = new HashSet<>();
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
        when( environment.getId() ).thenReturn( uuid );
        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( uuid );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( uuid );
        when( stormClusterConfiguration.getAllNodes() ).thenReturn( myUUID );

        stormImpl.onContainerDestroyed( environment, uuid );
    }


    @Test
    public void testOnContainerDestroyedNotSaved()
    {
        List<StormClusterConfiguration> myList = new ArrayList<>();
        myList.add( stormClusterConfiguration );
        when( pluginDAO.getInfo( StormClusterConfiguration.PRODUCT_KEY, StormClusterConfiguration.class ) )
                .thenReturn( myList );
        stormImpl.getClusters();
        when( environment.getId() ).thenReturn( uuid );
        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( uuid );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( uuid );
        myUUID.add( UUID.randomUUID() );
        when( stormClusterConfiguration.getAllNodes() ).thenReturn( myUUID );

        stormImpl.onContainerDestroyed( environment, uuid );
    }


    @Test
    public void testOnEnvironmentDestroyed()
    {
        List<StormClusterConfiguration> myList = new ArrayList<>();
        myList.add( stormClusterConfiguration );
        when( pluginDAO.getInfo( StormClusterConfiguration.PRODUCT_KEY, StormClusterConfiguration.class ) )
                .thenReturn( myList );
        stormImpl.getClusters();
        when( environment.getId() ).thenReturn( uuid );
        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( uuid );
        when( pluginDAO.deleteInfo( anyString(), anyString() ) ).thenReturn( true );

        stormImpl.onEnvironmentDestroyed( uuid );
    }
}