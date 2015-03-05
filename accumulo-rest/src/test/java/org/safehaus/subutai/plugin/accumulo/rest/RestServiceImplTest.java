package org.safehaus.subutai.plugin.accumulo.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.accumulo.api.Accumulo;
import org.safehaus.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceImplTest
{
    @Mock Accumulo accumulo;
    @Mock Hadoop hadoop;
    @Mock HadoopClusterConfig hadoopClusterConfig;
    @Mock ContainerHost containerHost;
    @Mock Tracker tracker;
    @Mock TrackerOperationView trackerOperationView;
    private RestServiceImpl restService;
    private AccumuloClusterConfig accumuloClusterConfig;
    private String config2 = "{\"clusterName\": \"test\",\"instanceName\": \"instance-name\",\"password\": " +
            "\"password\",\"masterNode\": \"" + UUID.randomUUID().toString() + "\",\"gcNode\": \"" + UUID.randomUUID()
                                                                                                         .toString()
            + "\"," +
            "\"monitor\": \"" + UUID.randomUUID().toString() + "\",\"tracers\": [\"" + UUID.randomUUID().toString()
            + "\",\"" + UUID.randomUUID().toString() + "\"],\"slaves\": " +
            "[\"" + UUID.randomUUID().toString() + "\",\"" + UUID.randomUUID().toString() + "\"]}";


    @Before
    public void setUp() throws Exception
    {
        restService = new RestServiceImpl( accumulo );
        accumuloClusterConfig = new AccumuloClusterConfig();
        restService.setTracker( tracker );
        restService.setHadoop( hadoop );
        restService.setAccumuloManager( accumulo );
        when( accumulo.getCluster( anyString() ) ).thenReturn( accumuloClusterConfig );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class ) ) ).thenReturn( trackerOperationView );
        when( trackerOperationView.getState() ).thenReturn( OperationState.SUCCEEDED );
    }


    @Test
    public void testListClusters() throws Exception
    {
        List<AccumuloClusterConfig> myList = new ArrayList<>();
        myList.add( accumuloClusterConfig );
        when( accumulo.getClusters() ).thenReturn( myList );

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
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( hadoopClusterConfig.getEnvironmentId() ).thenReturn( UUID.randomUUID() );

        Response response = restService.installCluster( config2 );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testDestroyCluster() throws Exception
    {
        Response response = restService.destroyCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStartCluster() throws Exception
    {
        Response response = restService.startCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testStopCluster() throws Exception
    {
        Response response = restService.stopCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testAddNode() throws Exception
    {
        Response response = restService.addNode( "test", "test", "MASTER_NODE" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        Response response = restService.destroyNode( "test", "test", "MASTER_NODE" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testCheckNode() throws Exception
    {
        Response response = restService.checkNode( "test", "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testGetAccumuloManager() throws Exception
    {
        restService.getAccumuloManager();
    }


    @Test
    public void testGetHadoop() throws Exception
    {
        restService.getHadoop();
    }
}