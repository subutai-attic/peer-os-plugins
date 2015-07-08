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
//import io.subutai.common.protocol.Agent;
//import io.subutai.common.tracker.OperationState;
//import io.subutai.common.tracker.TrackerOperation;
//import io.subutai.plugin.common.mock.CommonMockBuilder;
//import PrestoClusterConfig;
//import io.subutai.plugin.presto.impl.handler.ChangeCoordinatorNodeOperationHandler;
//import io.subutai.plugin.presto.impl.mock.PrestoImplMock;
//
//
//public class ChangeCoordinatorNodeOperation
//{
//    private PrestoImplMock mock;
//    private AbstractOperationHandler handler;
//
//
//    @Before
//    public void setUp()
//    {
//        mock = new PrestoImplMock();
//        handler = new ChangeCoordinatorNodeOperationHandler( mock, "test-cluster", "test-host" );
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
//        PrestoClusterConfig config = new PrestoClusterConfig();
//        config.setClusterName( "test-cluster" );
//        config.setWorkers( new HashSet<Agent>( Arrays.asList( CommonMockBuilder.createAgent() ) ) );
//        config.setCoordinatorNode( CommonMockBuilder.createAgent() );
//        mock.setClusterConfig( config );
//
//        handler.run();
//
//        TrackerOperation po = handler.getTrackerOperation();
//        Assert.assertTrue( po.getLog().toLowerCase().contains( "not connected" ) );
//        Assert.assertEquals( po.getState(), OperationState.FAILED );
//    }
//}
