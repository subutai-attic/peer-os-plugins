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
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//
//public class DestroyWorkerEnvironmentContainerNodeOperationHandlerTest
//{
//
//    private PrestoImpl prestoMock;
//    private AbstractOperationHandler handler;
//    private PrestoClusterConfig config;
//
//
//    @Before
//    public void setUp()
//    {
//        prestoMock = mock( PrestoImpl.class );
//        config = mock( PrestoClusterConfig.class );
//        handler = new NodeOperationHanler( prestoMock, "test-cluster", "test-host", NodeOperationType.UNINSTALL);
//    }
//
//
//    @Test
//    public void testWithoutCluster()
//    {
//        handler.run();
//
//        TrackerOperation po = handler.getTrackerOperation();
//        Assert.assertTrue( po.getLog().toLowerCase().contains( "not exist" ) );
//        Assert.assertEquals( po.getState(), OperationState.FAILED );
//    }
//
//
//    @Test
//    public void testWithUnconnectedAgents()
//    {
//        when(config.getClusterName()).thenReturn("test-cluster");
//
//        handler.run();
//
//        TrackerOperation po = handler.getTrackerOperation();
//        Assert.assertTrue( po.getLog().toLowerCase().contains( "not connected" ) );
//        Assert.assertEquals( po.getState(), OperationState.FAILED );
//    }
//}
