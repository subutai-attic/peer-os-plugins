package io.subutai.plugin.hipi.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hipi.api.Hipi;
import io.subutai.plugin.hipi.api.HipiConfig;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
@Ignore
public class RestServiceTest
{
    @Mock Hipi hipi;
    @Mock Tracker tracker;
    @Mock TrackerOperationView trackerOperationView;
    private RestServiceImpl restService;
    private HipiConfig hipiConfig;


    @Before
    public void setUp() throws Exception
    {
        hipiConfig = new HipiConfig();
        restService = new RestServiceImpl( hipi );
        restService.setTracker( tracker );
        when( hipi.getCluster( anyString() ) ).thenReturn( hipiConfig );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class ) ) ).thenReturn( trackerOperationView );
        when( trackerOperationView.getState() ).thenReturn( OperationState.SUCCEEDED );
    }


    @Test
    public void testListClusters() throws Exception
    {
        List<HipiConfig> myList = new ArrayList<>();
        myList.add( hipiConfig );
        when( hipi.getClusters() ).thenReturn( myList );

        Response response = restService.listClusters();

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
    public void testInstallCluster() throws Exception
    {
        Response response = restService.installCluster( "test", "test", UUID.randomUUID().toString() );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        Response response = restService.uninstallCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testAddNode() throws Exception
    {
        Response response = restService.addNode( "testClusterName", "testLxcHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        Response response = restService.destroyNode( "testClusterName", "testLxcHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }
}