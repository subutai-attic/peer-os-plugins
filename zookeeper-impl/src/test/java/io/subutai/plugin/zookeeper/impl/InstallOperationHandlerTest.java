package io.subutai.plugin.zookeeper.impl;


import org.junit.Test;
import io.subutai.common.tracker.OperationState;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.mock.TrackerMock;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.zookeeper.api.SetupType;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.plugin.zookeeper.impl.ZookeeperImpl;
import io.subutai.plugin.zookeeper.impl.handler.ZookeeperClusterOperationHandler;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class InstallOperationHandlerTest
{

    @Test
    public void testWithExistingCluster()
    {
        ZookeeperClusterConfig config = mock( ZookeeperClusterConfig.class );
        ZookeeperImpl zookeeperMock = mock( ZookeeperImpl.class );
        when( zookeeperMock.getHadoopManager() ).thenReturn( mock( Hadoop.class ) );
        when( zookeeperMock.getTracker() ).thenReturn( new TrackerMock() );
        when( zookeeperMock.getEnvironmentManager() ).thenReturn( mock( EnvironmentManager.class ) );
        when( zookeeperMock.getHadoopManager() ).thenReturn( mock( Hadoop.class ) );
        when( config.getClusterName() ).thenReturn( "test" );
        when( zookeeperMock.getCluster( anyString() ) ).thenReturn( config );
        AbstractOperationHandler operationHandler =
                new ZookeeperClusterOperationHandler( zookeeperMock, config, ClusterOperationType.INSTALL );
        operationHandler.run();

        assertTrue( operationHandler.getTrackerOperation().getLog().contains( "already exists" ) );
        assertEquals( operationHandler.getTrackerOperation().getState(), OperationState.FAILED );
    }


    @Test
    public void testWithMalformedConfiguration()
    {
        ZookeeperClusterConfig config = mock( ZookeeperClusterConfig.class );
        when( config.getClusterName() ).thenReturn( null );
        when( config.getSetupType() ).thenReturn( SetupType.STANDALONE );
        when( config.getNumberOfNodes() ).thenReturn( 0 );

        ZookeeperImpl zookeeperMock = mock( ZookeeperImpl.class );
        when( zookeeperMock.getHadoopManager() ).thenReturn( mock( Hadoop.class ) );
        when( zookeeperMock.getTracker() ).thenReturn( new TrackerMock() );
        when( zookeeperMock.getEnvironmentManager() ).thenReturn( mock( EnvironmentManager.class ) );
        when( zookeeperMock.getHadoopManager() ).thenReturn( mock( Hadoop.class ) );
        when( zookeeperMock.getCluster( anyString() ) ).thenReturn( new ZookeeperClusterConfig() );
        AbstractOperationHandler operationHandler =
                new ZookeeperClusterOperationHandler( zookeeperMock, config, ClusterOperationType.INSTALL );
        operationHandler.run();

        assertTrue( operationHandler.getTrackerOperation().getLog().contains( "Malformed configuration" ) );
        assertEquals( operationHandler.getTrackerOperation().getState(), OperationState.FAILED );
    }


    @Test
    public void testWithMalformedConfigurationOverHadoop()
    {
        ZookeeperClusterConfig config = mock( ZookeeperClusterConfig.class );
        when( config.getClusterName() ).thenReturn( null );
        when( config.getSetupType() ).thenReturn( SetupType.OVER_HADOOP );
        when( config.getNodes() ).thenReturn( null );

        ZookeeperImpl zookeeperMock = mock( ZookeeperImpl.class );
        when( zookeeperMock.getHadoopManager() ).thenReturn( mock( Hadoop.class ) );
        when( zookeeperMock.getTracker() ).thenReturn( new TrackerMock() );
        when( zookeeperMock.getEnvironmentManager() ).thenReturn( mock( EnvironmentManager.class ) );
        when( zookeeperMock.getHadoopManager() ).thenReturn( mock( Hadoop.class ) );
        when( zookeeperMock.getCluster( anyString() ) ).thenReturn( new ZookeeperClusterConfig() );
        AbstractOperationHandler operationHandler =
                new ZookeeperClusterOperationHandler( zookeeperMock, config, ClusterOperationType.INSTALL );
        operationHandler.run();

        assertTrue( operationHandler.getTrackerOperation().getLog().contains( "Malformed configuration" ) );
        assertEquals( operationHandler.getTrackerOperation().getState(), OperationState.FAILED );
    }
}
