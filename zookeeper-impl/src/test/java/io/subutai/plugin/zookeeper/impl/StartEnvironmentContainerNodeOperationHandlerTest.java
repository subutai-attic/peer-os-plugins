package io.subutai.plugin.zookeeper.impl;


import org.junit.Test;

import io.subutai.common.tracker.OperationState;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.mock.TrackerMock;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.zookeeper.impl.handler.ZookeeperNodeOperationHandler;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class StartEnvironmentContainerNodeOperationHandlerTest
{
    @Test
    public void testWithoutCluster()
    {
        ZookeeperImpl zookeeperMock = mock( ZookeeperImpl.class );
        when( zookeeperMock.getHadoopManager() ).thenReturn( mock( Hadoop.class ) );
        when( zookeeperMock.getTracker() ).thenReturn( new TrackerMock() );
        when( zookeeperMock.getEnvironmentManager() ).thenReturn( mock( EnvironmentManager.class ) );
        when( zookeeperMock.getHadoopManager() ).thenReturn( mock( Hadoop.class ) );
        when( zookeeperMock.getCluster( anyString() ) ).thenReturn( null );
        AbstractOperationHandler operationHandler =
                new ZookeeperNodeOperationHandler( zookeeperMock, "test-cluster", "test-node",
                        NodeOperationType.START );
        operationHandler.run();

        assertTrue( operationHandler.getTrackerOperation().getLog().contains( "not exist" ) );
        assertEquals( operationHandler.getTrackerOperation().getState(), OperationState.FAILED );
    }
}
