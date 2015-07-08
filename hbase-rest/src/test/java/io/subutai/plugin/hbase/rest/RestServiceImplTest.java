package io.subutai.plugin.hbase.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hbase.api.HBase;
import io.subutai.plugin.hbase.api.HBaseConfig;
import io.subutai.plugin.hbase.rest.RestServiceImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceImplTest
{
    @Mock HBase hBase;
    @Mock HBaseConfig hBaseConfig;
    @Mock Tracker tracker;
    private RestServiceImpl restService;


    @Before
    public void setUp() throws Exception
    {
        restService = new RestServiceImpl();
        restService.setHbaseManager( hBase );
        restService.setTracker( tracker );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class ) ) )
                .thenReturn( mock( TrackerOperationView.class ) );
    }


    @Test
    public void testGetHbaseManager() throws Exception
    {
        restService.getHbaseManager();

        // assertions
        assertNotNull( restService.getHbaseManager() );
        assertEquals( hBase, restService.getHbaseManager() );
    }


    @Test
    public void testCheckNode()
    {
        when( hBase.checkNode( anyString(), any( String.class ) ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.checkNode( "test", UUID.randomUUID().toString() );

        // assertions
        assertEquals( Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testGetCluster()
    {
        HBaseConfig hBaseConfig1 = new HBaseConfig();
        when( hBase.getCluster( "test" ) ).thenReturn( hBaseConfig1 );

        Response response = restService.getCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testListClusters()
    {
        HBaseConfig hBaseConfig1 = new HBaseConfig();
        List<HBaseConfig> myList = new ArrayList<>();
        myList.add( hBaseConfig1 );
        when( hBase.getClusters() ).thenReturn( myList );

        Response response = restService.listClusters();

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testCreateCluster() throws Exception
    {
        when( hBase.installCluster( any( HBaseConfig.class ) ) ).thenReturn( UUID.randomUUID() );
        final String testConfig = "{\n" + "    \"clusterName\": \"test\",\n" + "    \"domainName\": \"intra.lan\",\n"
                + "    \"environmentId\": \"d1780bce-d877-4e31-80eb-70f273bea3cc\",\n"
                + "    \"hmaster\": \"5c518df9-bd88-42c1-9f5c-254b78a57057\",\n" + "    \"regionServers\": [\n"
                + "        \"d1780bce-d877-4e31-80eb-70f273bea3cc\"\n" + "    ],\n" + "    \"quorumPeers\": [\n"
                + "        \"13a9aec8-ac7b-4d10-ae97-ca1a473ad242\"\n" + "    ],\n" + "    \"backupMasters\": [\n"
                + "        \"d0d83420-aa52-4994-9297-0a52ff162793\"\n" + "    ]\n" + "}";

        Response response = restService.configureCluster( testConfig );

        // assertions
        assertEquals( Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testDestroyCluster() throws Exception
    {
        when( hBase.uninstallCluster( anyString() ) ).thenReturn( UUID.randomUUID() );
        Response response = restService.destroyCluster( "test" );

        // assertions
        assertEquals( Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStartCluster() throws Exception
    {
        when( hBase.startCluster( anyString() ) ).thenReturn( UUID.randomUUID() );
        Response response = restService.startCluster( "test" );

        // assertions
        assertEquals( Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStopCluster() throws Exception
    {
        when( hBase.stopCluster( anyString() ) ).thenReturn( UUID.randomUUID() );
        Response response = restService.stopCluster( "test" );
        restService.setTracker( tracker );

        // assertions
        assertEquals( Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        when( hBase.destroyNode( anyString(), anyString() ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.destroyNode( "test", "test", "test" );
        restService.setTracker( tracker );

        // assertions
        assertEquals( Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testAddNode() throws Exception
    {
        when( hBase.addNode( anyString(), anyString() ) ).thenReturn( UUID.randomUUID() );
        Response response = restService.addNode( "test", "test" );

        // assertions
        assertEquals( Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus() );
    }
}