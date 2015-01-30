package org.safehaus.subutai.plugin.mahout.rest;


import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.plugin.mahout.api.Mahout;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceImplTest
{
    private RestServiceImpl restService;
    private MahoutClusterConfig mahoutClusterConfig;
    private String config2 =
            "{\"clusterName\": \"my-accumulo-cluster\",\"instanceName\": \"instance-name\",\"password\": " +
                    "\"password\",\"masterNode\": \"master-node-hostname\",\"gcNode\": \"gc-node-hostname\"," +
                    "\"monitor\": \"monitor-node-hostname\",\"tracers\": [\"lxc-2\",\"lxc-1\"],\"slaves\": " +
                    "[\"lxc-3\",\"lxc-4\"]}";

    @Mock
    Mahout mahout;
    
    @Before
    public void setUp() throws Exception
    {
        restService = new RestServiceImpl();
        mahoutClusterConfig = new MahoutClusterConfig();
        restService.setMahoutManager( mahout );
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
    public void testStartCluster() throws Exception
    {
        restService.startCluster( "test" );
    }


    @Test
    public void testStopCluster() throws Exception
    {
        restService.stopCluster( "test" );
    }


    @Test
    public void testAddNode() throws Exception
    {
        Response response = restService.addNode( "testClusterName", "testNode" );

        // assertions
        assertEquals( Response.Status.CREATED.getStatusCode(), response.getStatus() );

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