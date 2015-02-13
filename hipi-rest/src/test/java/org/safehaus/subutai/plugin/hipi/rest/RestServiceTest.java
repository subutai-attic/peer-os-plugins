package org.safehaus.subutai.plugin.hipi.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.plugin.hipi.api.Hipi;
import org.safehaus.subutai.plugin.hipi.api.HipiConfig;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceTest
{
    private RestService restService;
    private HipiConfig hipiConfig;
    @Mock
    Hipi hipi;

    @Before
    public void setUp() throws Exception
    {
        hipiConfig = new HipiConfig();
        restService = new RestService( hipi );
    }


    @Test
    public void testGetClusters() throws Exception
    {
        List<HipiConfig> myList = new ArrayList<>();
        myList.add( hipiConfig );
        when( hipi.getClusters() ).thenReturn( myList );

        Response response = restService.getClusters();

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        when( hipi.getCluster( anyString() ) ).thenReturn( hipiConfig );

        Response response = restService.getCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testInstallCluster() throws Exception
    {
//        restService.installCluster( "testClusterName", "testHostName", "nodes,sdff" );

    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        when( hipi.uninstallCluster( anyString() ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.uninstallCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testAddNode() throws Exception
    {
        when( hipi.addNode( "testClusterName", "testLxcHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.addNode( "testClusterName", "testLxcHostName" );

        // assertions
        assertEquals( Response.Status.CREATED.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        when( hipi.destroyNode( "testClusterName", "testLxcHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.destroyNode( "testClusterName", "testLxcHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }
}