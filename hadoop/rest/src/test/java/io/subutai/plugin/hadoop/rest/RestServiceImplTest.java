package io.subutai.plugin.hadoop.rest;


import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class RestServiceImplTest
{
    private RestServiceImpl restService;
    private String testConfig = "{\"clusterName\": \"test\",\"domainName\": \"intra.lan\",\"replicationFactor\": " +
            "\"1\", \"environmentId\": \"" + UUID.randomUUID().toString() + "\", \"nameNode\": \"" + UUID.randomUUID()
                                                                                                         .toString()
            + "\",\"jobTracker\": \"" + UUID.randomUUID().toString() + "\"," +
            "\"secNameNode\": \"" + UUID.randomUUID().toString() + "\",\"slaves\": [\"" + UUID.randomUUID().toString()
            + "\",\"" + UUID.randomUUID().toString() + "\"]}";
    @Mock
    Hadoop hadoop;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperationView trackerOperationView;


    @Before
    public void setUp() throws Exception
    {
        restService = new RestServiceImpl( hadoop, tracker, null );

        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class ) ) ).thenReturn( trackerOperationView );
        when( trackerOperationView.getState() ).thenReturn( OperationState.SUCCEEDED );
    }


    @Test
    public void testGetHadoopManager() throws Exception
    {
        restService.getHadoopManager();

        // assertions
        assertNotNull( restService.getHadoopManager() );
        assertEquals( hadoop, restService.getHadoopManager() );
    }


    @Test
    public void testListClusters() throws Exception
    {
        List<HadoopClusterConfig> myList = Lists.newArrayList();
        myList.add( hadoopClusterConfig );
        when( hadoop.getClusters() ).thenReturn( myList );
        when( hadoopClusterConfig.getClusterName() ).thenReturn( "test" );

        Response response = restService.listClusters();

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    @Ignore
    public void testGetCluster() throws Exception
    {
        HadoopClusterConfig hadoopClusterConfig1 = new HadoopClusterConfig();
        when( hadoop.getCluster( "test" ) ).thenReturn( hadoopClusterConfig1 );

        Response response = restService.getCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testConfigureCluster() throws Exception
    {
        //when(hadoop.installCluster(hadoopClusterConfig)).thenReturn(UUID.randomUUID());
        Response response = restService.configureCluster( testConfig );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        when( hadoop.uninstallCluster( "test" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.uninstallCluster( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }


    @Test
    public void testAddNode() throws Exception
    {
        when( hadoop.addNode( "test" ) ).thenReturn( UUID.randomUUID() );

        Response response = restService.addNode( "test" );

        // assertions
        assertEquals( Response.Status.OK.getStatusCode(), response.getStatus() );
    }
}