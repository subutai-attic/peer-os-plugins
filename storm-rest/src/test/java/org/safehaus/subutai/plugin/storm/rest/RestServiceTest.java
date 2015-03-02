package org.safehaus.subutai.plugin.storm.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.storm.api.Storm;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceTest
{
    private RestServiceImpl restService;
    private StormClusterConfiguration stormClusterConfiguration;
    private UUID uuid;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Storm storm;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    Zookeeper zookeeper;
    @Mock
    ZookeeperClusterConfig zookeeperClusterConfig;
    @Mock
    Environment environment;
    @Mock
    ContainerHost containerHost;
    @Mock
    TrackerOperationView trackerOperationView;

    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID( 50, 50 );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        stormClusterConfiguration = new StormClusterConfiguration();

        restService = new RestServiceImpl();
        restService.setEnvironmentManager( environmentManager );
        restService.setStormManager( storm );
        restService.setZookeeperManager( zookeeper );
        restService.setTracker( tracker );

        // mock
        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostByHostname( anyString() ) ).thenReturn( containerHost );
        when( containerHost.getId() ).thenReturn( UUID.randomUUID() );
        when( storm.installCluster( any( StormClusterConfiguration.class ) ) ).thenReturn( UUID.randomUUID() );
        when( storm.getCluster( anyString() ) ).thenReturn( stormClusterConfiguration );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class) ) ).thenReturn( trackerOperationView );
        when( trackerOperationView.getState() ).thenReturn( OperationState.SUCCEEDED );

    }


    @Test
    public void testGetEnvironmentManager() throws Exception
    {
        restService.getEnvironmentManager();

        //assertions
        assertNotNull( restService.getEnvironmentManager() );
        assertEquals( environmentManager, restService.getEnvironmentManager() );
    }


    @Test
    public void testGetZookeeperManager() throws Exception
    {
        restService.getZookeeperManager();

        //assertions
        assertNotNull( restService.getZookeeperManager() );
        assertEquals( zookeeper, restService.getZookeeperManager() );
    }


    @Test
    public void testGetClusters() throws Exception
    {
        List<StormClusterConfiguration> myList = new ArrayList<>();
        myList.add( stormClusterConfiguration );
        when( storm.getClusters() ).thenReturn( myList );

        Response response = restService.getClusters();

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        when( storm.getCluster( anyString() ) ).thenReturn( stormClusterConfiguration );

        Response response = restService.getCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testInstallClusterExternalZookeeper() throws Exception
    {
//        Response response = restService.installCluster( "test", false, "testZookeeperClusterName", "testNimbus", UUID.randomUUID().toString() );

        // assertions
//        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testInstallCluster() throws Exception
    {
//        Response response = restService.installCluster( "test", true, "testZookeeperClusterName", "testNimbus", "5" );

        // assertions
//        assertEquals( Response.Status.CREATED.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testInstallClusterExternalZookeeperNoEnvironment() throws EnvironmentNotFoundException
    {
//        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenThrow( EnvironmentNotFoundException.class );

//        Response response = restService.installCluster( "test", true, "testZookeeperClusterName", "testNimbus", "5" );

        // assertions
//        assertEquals( Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testInstallClusterExternalZookeeperNoContainerHost() throws ContainerHostNotFoundException
    {
//        when( environment.getContainerHostByHostname( anyString() ) ).thenThrow( ContainerHostNotFoundException.class );

//        restService.installCluster( "test", true, "testZookeeperClusterName", "testNimbus", "5" );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        when( storm.uninstallCluster( anyString() ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.uninstallCluster( "testClusterName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testAddNode() throws Exception
    {
        when( storm.addNode( "testClusterName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.addNode( "testClusterName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        when( storm.destroyNode( "testClusterName", "testHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.destroyNode( "testClusterName", "testHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStatusCheck() throws Exception
    {
        when( storm.checkNode( "testClusterName", "testHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.statusCheck( "testClusterName", "testHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStartNode() throws Exception
    {
        when( storm.startNode( "testClusterName", "testHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.startNode( "testClusterName", "testHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );

    }


    @Test
    public void testStopNode() throws Exception
    {
        when( storm.stopNode( "testClusterName", "testHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.stopNode( "testClusterName", "testHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );

    }
}