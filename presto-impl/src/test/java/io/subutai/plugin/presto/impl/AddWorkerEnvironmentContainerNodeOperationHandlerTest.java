//package io.subutai.plugin.presto.impl;
//
//
//import java.util.Arrays;
//import java.util.HashSet;
//
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import io.subutai.common.protocol.AbstractOperationHandler;
//import io.subutai.common.tracker.OperationState;
//import io.subutai.common.tracker.TrackerOperation;
//import io.subutai.plugin.common.api.NodeOperationType;
//import io.subutai.plugin.common.mock.CommonMockBuilder;
//import PrestoClusterConfig;
//import NodeOperationHanler;
//import io.subutai.plugin.presto.impl.mock.PrestoImplMock;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//
//public class AddWorkerEnvironmentContainerNodeOperationHandlerTest
//{
//
//    //private PrestoImplMock mock;
//    private AbstractOperationHandler handler;
//
//
//    @Before
//    public void setUp()
//    {
//        PrestoImpl manager = mock(PrestoImpl.class);
//        handler = new NodeOperationHanler( manager, "test-cluster", "test-host", NodeOperationType.INSTALL );
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
//        PrestoClusterConfig config = mock(PrestoClusterConfig.class);
//        when(config.getClusterName()).thenReturn( "test-cluster" );
//        when(config.getWorkers()).thenReturn( new HashSet<>( Arrays.asList( CommonMockBuilder.createAgent().getUuid() ) ));
//        when(config.getCoordinatorNode()).thenReturn( CommonMockBuilder.createAgent().getUuid() );
//
//        handler.run();
//
//        TrackerOperation po = handler.getTrackerOperation();
//        Assert.assertTrue( po.getLog().toLowerCase().contains( "not connected" ) );
//        Assert.assertEquals( po.getState(), OperationState.FAILED );
//    }
//}
