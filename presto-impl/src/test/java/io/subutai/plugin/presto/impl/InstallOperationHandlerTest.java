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
//import io.subutai.plugin.common.api.ClusterOperationType;
//import io.subutai.plugin.common.mock.CommonMockBuilder;
//import PrestoClusterConfig;
//import io.subutai.plugin.presto.api.SetupType;
//import ClusterOperationHandler;
//import io.subutai.plugin.presto.impl.mock.PrestoImplMock;
//
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//
//public class InstallOperationHandlerTest
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
//        prestoMock = mock(PrestoImpl.class);
//        config = mock(PrestoClusterConfig.class);
//
//    }
//
//
//    @Test( expected = NullPointerException.class )
//    public void testWithNullConfig()
//    {
//        handler = new ClusterOperationHandler( prestoMock, null, ClusterOperationType.INSTALL );
//        handler.run();
//    }
//
//
//    @Test
//    public void testWithInvalidConfig()
//    {
//        PrestoClusterConfig config = new PrestoClusterConfig();
//        config.setSetupType( SetupType.OVER_HADOOP );
//        config.setClusterName( "test" );
//        handler = new ClusterOperationHandler( prestoMock, config, ClusterOperationType.INSTALL );
//        handler.run();
//
//        TrackerOperation po = handler.getTrackerOperation();
//        Assert.assertTrue( po.getLog().toLowerCase().contains( "malformed" ) );
//        Assert.assertEquals( po.getState(), OperationState.FAILED );
//    }
//
//
//    @Test
//    public void testWithExistingCluster()
//    {
//        when(config.getSetupType()).thenReturn(SetupType.OVER_HADOOP);
//        when(config.getClusterName()).thenReturn( "test" );
//        when(config.getWorkers()).thenReturn( new HashSet<>( Arrays.asList( CommonMockBuilder.createAgent().getUuid() ) ) );
//        when(config.getCoordinatorNode()).thenReturn( CommonMockBuilder.createAgent().getUuid() );
//
//        handler = new ClusterOperationHandler( prestoMock, config, ClusterOperationType.INSTALL );
//        handler.run();
//
//        TrackerOperation po = handler.getTrackerOperation();
//        Assert.assertTrue( po.getLog().toLowerCase().contains( "exists" ) );
//        Assert.assertTrue( po.getLog().toLowerCase().contains( config.getClusterName() ) );
//        Assert.assertEquals( po.getState(), OperationState.FAILED );
//    }
//}
//
