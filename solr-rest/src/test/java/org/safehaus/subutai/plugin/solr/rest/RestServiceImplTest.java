package org.safehaus.subutai.plugin.solr.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.plugin.solr.api.Solr;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceImplTest
{
    private RestServiceImpl restService;
    private SolrClusterConfig solrClusterConfig;
    private String config2 =
            "{\"clusterName\": \"my-accumulo-cluster\",\"instanceName\": \"instance-name\",\"password\": " +
                    "\"password\",\"masterNode\": \"master-node-hostname\",\"gcNode\": \"gc-node-hostname\"," +
                    "\"monitor\": \"monitor-node-hostname\",\"tracers\": [\"lxc-2\",\"lxc-1\"],\"slaves\": " +
                    "[\"lxc-3\",\"lxc-4\"]}";

    @Mock
    Solr solr;
    @Mock
    EnvironmentManager environmentManager;


    @Before
    public void setUp() throws Exception
    {
        restService = new RestServiceImpl();
        restService.setSolrManager( solr );
        solrClusterConfig = new SolrClusterConfig();
    }


    @Test
    public void testListClusters() throws Exception
    {
        List<SolrClusterConfig> myList = new ArrayList<>();
        myList.add( solrClusterConfig );
        when( solr.getClusters() ).thenReturn( myList );

        Response response = restService.listClusters();

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        when( solr.getCluster( anyString() ) ).thenReturn( solrClusterConfig );

        Response response = restService.getCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        when( solr.destroyNode( "testClusterName", "testLxcHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.destroyNode( "testClusterName", "testLxcHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStartNode() throws Exception
    {
        when( solr.startNode( "testClusterName", "testLxcHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.startNode( "testClusterName", "testLxcHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStopNode() throws Exception
    {
        when( solr.stopNode( "testClusterName", "testLxcHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.stopNode( "testClusterName", "testLxcHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testCheckNode() throws Exception
    {
        when( solr.checkNode( "testClusterName", "testLxcHostName" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.checkNode( "testClusterName", "testLxcHostName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }

    @Test
    public void testDestroyCluster()
    {
        when( solr.uninstallCluster( anyString() ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.destroyCluster( "testClusterName" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testCreateCluster()
    {
        Response response = restService.createCluster( config2 );

        // assertions
        assertEquals( Response.Status.CREATED.getStatusCode(), response.getStatus() );
    }
}