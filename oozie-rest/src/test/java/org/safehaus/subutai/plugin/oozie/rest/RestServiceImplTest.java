package org.safehaus.subutai.plugin.oozie.rest;


import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.oozie.api.Oozie;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;

import static org.junit.Assert.assertEquals;
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


    @Before
    public void setUp() throws Exception
    {
        restService = new RestServiceImpl( oozie, hadoop, environmentManager );
        oozieClusterConfig = new OozieClusterConfig();
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
        when( oozie.getCluster( anyString() ) ).thenReturn( oozieClusterConfig );

        Response response = restService.getCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testInstall() throws Exception
    {
        restService.install( "test","test","5","test","test" );
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
    public void testStartNode() throws Exception
    {
        Response response = restService.startNode( "testClusterName", "testNode" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStopNode() throws Exception
    {
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