//package io.subutai.plugin.presto.impl;
//
//
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import io.subutai.common.protocol.AbstractOperationHandler;
//import io.subutai.common.tracker.OperationState;
//import io.subutai.common.tracker.TrackerOperation;
//import io.subutai.plugin.common.api.NodeOperationType;
//import PrestoClusterConfig;
//import NodeOperationHanler;
//import io.subutai.plugin.presto.impl.mock.PrestoImplMock;
//
//
//public class StopEnvironmentContainerOperationHandlerTest
//{
//    private PrestoImplMock mock;
//    private AbstractOperationHandler handler;
//
//
//    @Before
//    public void setUp()
//    {
//        mock = new PrestoImplMock();
//        handler = new NodeOperationHanler( mock, "test-cluster", "test-host", NodeOperationType.STOP );
//    }
//
//
//    @Test
//    public void testWithoutCluster()
//    {
//
//        handler.run();
//
//        TrackerOperation po = handler.getTrackerOperation();
//        Assert.assertTrue( po.getLog().contains( "not exist" ) );
//        Assert.assertEquals( po.getState(), OperationState.FAILED );
//    }
//
//
//    @Test
//    public void testWithNotConnectedAgents()
//    {
//        PrestoClusterConfig config = new PrestoClusterConfig();
//        mock.setClusterConfig( config );
//        handler.run();
//
//        TrackerOperation po = handler.getTrackerOperation();
//        Assert.assertTrue( po.getLog().contains( "not connected" ) );
//        Assert.assertEquals( po.getState(), OperationState.FAILED );
//    }
//}
