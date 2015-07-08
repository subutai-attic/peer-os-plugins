package io.subutai.plugin.oozie.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.oozie.api.Oozie;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.rest.RestServiceImpl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceImplTest
{
    private RestServiceImpl restService;
    private OozieClusterConfig oozieClusterConfig;
    private String config2 =
            "{\"clusterName\": \"my-accumulo-cluster\",\"instanceName\": \"instance-name\",\"password\": " +
                    "\"password\",\"masterNode\": \"master-node-hostname\",\"gcNode\": \"gc-node-hostname\"," +
                    "\"monitor\": \"monitor-node-hostname\",\"tracers\": [\"lxc-2\",\"lxc-1\"],\"slaves\": " +
                    "[\"lxc-3\",\"lxc-4\"]}";

    @Mock
    Oozie oozie;
    @Mock
    Hadoop hadoop;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperationView trackerOperationView;


    @Before
    public void setUp() throws Exception
    {
        restService = new RestServiceImpl( oozie );
        restService.setTracker( tracker );
        oozieClusterConfig = new OozieClusterConfig();
        when( oozie.getCluster( anyString() ) ).thenReturn( oozieClusterConfig );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class) ) ).thenReturn( trackerOperationView );
        when( trackerOperationView.getState() ).thenReturn( OperationState.SUCCEEDED );
    }


    @Test
    public void testListClusters() throws Exception
    {
        List<OozieClusterConfig> myList = new ArrayList<>();
        myList.add( oozieClusterConfig );
        when( oozie.getClusters() ).thenReturn( myList );

        Response response = restService.getClusters();

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        Response response = restService.getCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }



    @Test
    public void testUninstallCluster() throws Exception
    {
        Response response = restService.uninstallCluster( "testClusterName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testAddNode() throws Exception
    {
        Response response = restService.addNode( "testClusterName", "testNode" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        Response response = restService.destroyNode( "testClusterName", "testNode" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStartNode() throws Exception
    {
        Response response = restService.startNode( "testClusterName", "testNode" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStopNode() throws Exception
    {
        when( oozie.stopNode( anyString(), anyString() ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.stopNode( "testClusterName", "testNode" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testCheckNode() throws Exception
    {
        Response response = restService.checkNode( "testClusterName", "testNode" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }
}