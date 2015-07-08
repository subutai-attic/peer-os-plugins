//package io.subutai.plugin.presto.impl;
//
//
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import io.subutai.common.protocol.AbstractOperationHandler;
//import io.subutai.common.tracker.OperationState;
//import io.subutai.common.tracker.TrackerOperation;
//import io.subutai.core.environment.api.EnvironmentManager;
//import io.subutai.plugin.common.api.NodeOperationType;
//import io.subutai.plugin.common.mock.TrackerMock;
//import io.subutai.plugin.hadoop.api.Hadoop;
//import PrestoClusterConfig;
//import NodeOperationHanler;
//import io.subutai.plugin.presto.impl.mock.PrestoImplMock;
//
//import static junit.framework.TestCase.assertEquals;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.Matchers.anyString;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//
//public class StartEnvironmentContainerNodeOperationHandlerTest
//{
//
//
//    @Test
//    public void testWithoutCluster()
//    {
//
//        PrestoImpl prestoMock = mock( PrestoImpl.class );
//        when( prestoMock.getHadoopManager() ).thenReturn( mock( Hadoop.class ) );
//        when( prestoMock.getTracker() ).thenReturn( new TrackerMock() );
//        when( prestoMock.getEnvironmentManager() ).thenReturn( mock( EnvironmentManager.class ) );
//        when( prestoMock.getHadoopManager() ).thenReturn( mock( Hadoop.class ) );
//        when( prestoMock.getCluster( anyString() ) ).thenReturn( null );
//        AbstractOperationHandler operationHandler =
//                new NodeOperationHanler( prestoMock, "test-cluster", "test-node", NodeOperationType.START );
//        operationHandler.run();
//
//        assertTrue( operationHandler.getTrackerOperation().getLog().contains( "not exist" ) );
//        assertEquals( operationHandler.getTrackerOperation().getState(), OperationState.FAILED );
//    }
//
//
//    @Test
//    public void testFail()
//    {
//        PrestoImpl prestoMock = mock( PrestoImpl.class );
//        AbstractOperationHandler operationHandler =
//                new NodeOperationHanler( prestoMock, "test-cluster", "test-node", NodeOperationType.START );
//
//        operationHandler.run();
//
//        TrackerOperation po = operationHandler.getTrackerOperation();
//        assertTrue( po.getLog().contains( "not connected" ) );
//        Assert.assertEquals( po.getState(), OperationState.FAILED );
//    }
//}