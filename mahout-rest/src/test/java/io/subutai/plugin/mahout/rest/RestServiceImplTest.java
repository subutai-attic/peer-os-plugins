package io.subutai.plugin.mahout.rest;


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
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.mahout.api.Mahout;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceImplTest
{
    private RestServiceImpl restService;
    private MahoutClusterConfig mahoutClusterConfig;


    @Mock
    Mahout mahout;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperationView trackerOperationView;


    @Before
    public void setUp() throws Exception
    {
        restService = new RestServiceImpl( mahout );
        mahoutClusterConfig = new MahoutClusterConfig();
        restService.setMahoutManager( mahout );
        restService.setTracker( tracker );
        when( mahout.getCluster( anyString() ) ).thenReturn( mahoutClusterConfig );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class ) ) ).thenReturn( trackerOperationView );
        when( trackerOperationView.getState() ).thenReturn( OperationState.SUCCEEDED );
    }


    @Test
    public void testGetMahoutManager() throws Exception
    {
        restService.getMahoutManager();

        // assertions
        assertNotNull( restService.getMahoutManager() );
        assertEquals( mahout, restService.getMahoutManager() );
    }


    @Test
    public void testListClusters() throws Exception
    {
        List<MahoutClusterConfig> myList = new ArrayList<>();
        myList.add( mahoutClusterConfig );
        when( mahout.getClusters() ).thenReturn( myList );

        Response response = restService.listClusters();

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        when( mahout.getCluster( anyString() ) ).thenReturn( mahoutClusterConfig );

        Response response = restService.getCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testCreateCluster() throws Exception
    {
    }


    @Test
    public void testDestroyCluster() throws Exception
    {
        Response response = restService.destroyCluster( "testClusterName" );

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
    public void testCheckNode() throws Exception
    {
        Response response = restService.checkNode( "testClusterName", "testNode" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }
}