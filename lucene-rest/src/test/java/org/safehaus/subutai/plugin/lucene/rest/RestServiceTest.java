package org.safehaus.subutai.plugin.lucene.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.plugin.lucene.api.Lucene;
import org.safehaus.subutai.plugin.lucene.api.LuceneConfig;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceTest
{
    private RestService restService;
    private LuceneConfig luceneConfig;
    @Mock
    Lucene lucene;

    @Before
    public void setUp() throws Exception
    {
        luceneConfig = new LuceneConfig();

        restService = new RestService();
        restService.setLuceneManager( lucene );

    }


    @Test
    public void testGetClusters() throws Exception
    {
        List<LuceneConfig> myList = new ArrayList<>();
        myList.add( luceneConfig );
        when( lucene.getClusters() ).thenReturn( myList );

        Response response = restService.getClusters();

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        when( lucene.getCluster( anyString() ) ).thenReturn( luceneConfig );

        Response response = restService.getCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testInstallCluster() throws Exception
    {

    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        when( lucene.uninstallCluster( anyString() ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.uninstallCluster( "testClusterName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testAddNode() throws Exception
    {
        when( lucene.addNode( "testClusterName", "testLxcHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.addNode( "testClusterName", "testLxcHostName" );

        // assertions
        assertEquals( Response.Status.CREATED.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        when( lucene.addNode( "testClusterName", "testLxcHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.destroyNode( "testClusterName", "testLxcHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }
}