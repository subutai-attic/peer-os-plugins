package io.subutai.plugin.lucene.rest;


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
import io.subutai.plugin.lucene.api.Lucene;
import io.subutai.plugin.lucene.api.LuceneConfig;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceTest
{
    private RestServiceImpl restService;
    private LuceneConfig luceneConfig;
    @Mock
    Lucene lucene;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperationView trackerOperationView;


    @Before
    public void setUp() throws Exception
    {
        luceneConfig = new LuceneConfig();
        restService = new RestServiceImpl( lucene );
        restService.setTracker( tracker );
        when( lucene.getCluster( anyString() ) ).thenReturn( luceneConfig );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class ) ) ).thenReturn( trackerOperationView );
        when( trackerOperationView.getState() ).thenReturn( OperationState.SUCCEEDED );
    }


    @Test
    public void testListClusters() throws Exception
    {
        List<LuceneConfig> myList = new ArrayList<>();
        myList.add( luceneConfig );
        when( lucene.getClusters() ).thenReturn( myList );

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
        Response response = restService.uninstallCluster( "testClusterName" );

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