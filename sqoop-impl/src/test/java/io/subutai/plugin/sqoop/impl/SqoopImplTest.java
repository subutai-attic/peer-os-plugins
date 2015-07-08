package io.subutai.plugin.sqoop.impl;


import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.mock.TrackerOperationMock;
import io.subutai.plugin.sqoop.api.SqoopConfig;
import io.subutai.plugin.sqoop.impl.SqoopImpl;


@RunWith( MockitoJUnitRunner.class )
public class SqoopImplTest
{
    @Mock
    private SqoopImpl sqoop;

    @Mock
    private Tracker tracker;

    private TrackerOperation trackOperation;
    private String clusterName = "cluster-test";


    @Before
    public void setUp()
    {
        sqoop.executor = Mockito.mock( ExecutorService.class );
        trackOperation = new TrackerOperationMock();

        SqoopConfig config = new SqoopConfig();
        config.setClusterName( clusterName );
        Mockito.when( sqoop.getCluster( clusterName ) ).thenReturn( config );

        Mockito.when( sqoop.getTracker() ).thenReturn( tracker );
        Mockito.when( tracker.createTrackerOperation( Matchers.anyString(), Matchers.anyString() ) )
                .thenReturn( trackOperation );
    }


    @After
    public void tearDown()
    {
    }


    @Test
    public void testGetClusters()
    {
    }


    @Test
    public void testGetCluster()
    {
        SqoopConfig config = sqoop.getCluster( clusterName );
        Assert.assertEquals( clusterName, config.getClusterName() );
    }


    @Test
    public void testGetClusterSetupStrategy()
    {
//        // init real instance
//        sqoop = new SqoopImpl( Mockito.mock( DataSource.class ) );
//
//        SqoopConfig config = new SqoopConfig();
//        // no setup type
//        ClusterSetupStrategy s = sqoop.getClusterSetupStrategy( new Environment( "environment" ), config, trackOperation );
//        Assert.assertNull( s );
//
//        config.setSetupType( SetupType.OVER_HADOOP );
//        s = sqoop.getClusterSetupStrategy( new Environment( "environment" ), config, trackOperation );
//        Assert.assertTrue( s instanceof SetupStrategyOverHadoop );
//
//        config.setSetupType( SetupType.WITH_HADOOP );
//        s = sqoop.getClusterSetupStrategy( new Environment( "environment" ), config, trackOperation );
//        Assert.assertTrue( s instanceof SetupStrategyWithHadoop );
    }

}

