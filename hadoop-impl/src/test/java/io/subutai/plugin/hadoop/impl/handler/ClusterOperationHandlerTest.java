package io.subutai.plugin.hadoop.impl.handler;


import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.environment.api.exception.EnvironmentCreationException;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.NodeState;
import io.subutai.plugin.common.api.NodeType;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hadoop.impl.HadoopImpl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ClusterOperationHandlerTest
{
    ClusterOperationHandler clusterOperationHandler;
    ClusterOperationHandler clusterOperationHandler1;
    ClusterOperationHandler clusterOperationHandler2;
    TrackerOperation trackerOperation;
    String id;
    ExecutorService executorService;
    HadoopClusterConfig hadoopClusterConfig;
    HadoopImpl hadoop;
    Environment environment;
    EnvironmentManager environmentManager;
    EnvironmentContainerHost containerHost;
    EnvironmentContainerHost containerHost2;
    CommandResult commandResult;


    @Before
    public void setUp() throws ClusterSetupException, EnvironmentNotFoundException, EnvironmentCreationException,
            ContainerHostNotFoundException
    {
        containerHost = mock( EnvironmentContainerHost.class );
        containerHost2 = mock( EnvironmentContainerHost.class );
        PluginDAO pluginDAO = mock( PluginDAO.class );
        Set<EnvironmentContainerHost> mySet = mock( Set.class );
        mySet.add( containerHost );
        mySet.add( containerHost2 );
        hadoop = mock( HadoopImpl.class );
        environment = mock( Environment.class );
        environmentManager = mock( EnvironmentManager.class );
        hadoopClusterConfig = mock( HadoopClusterConfig.class );
        executorService = mock( ExecutorService.class );
        trackerOperation = mock( TrackerOperation.class );
        commandResult = mock( CommandResult.class );
        id = new UUID( 50, 50 ).toString();
        Tracker tracker = mock( Tracker.class );

        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( hadoop.getTracker() ).thenReturn( tracker );
        when( hadoop.getPluginDAO() ).thenReturn( pluginDAO );
        when( hadoop.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( id ) ).thenReturn( environment );
        when( environment.getContainerHostById( id ) ).thenReturn( containerHost );

        when( hadoopClusterConfig.getNameNode() ).thenReturn( id );
        when( hadoopClusterConfig.getSecondaryNameNode() ).thenReturn( id );

        when( hadoopClusterConfig.getClusterName() ).thenReturn( "test" );
        when( hadoopClusterConfig.getEnvironmentId() ).thenReturn( id );

        clusterOperationHandler =
                new ClusterOperationHandler( hadoop, hadoopClusterConfig, ClusterOperationType.INSTALL );
        clusterOperationHandler1 =
                new ClusterOperationHandler( hadoop, hadoopClusterConfig, ClusterOperationType.UNINSTALL,
                        NodeType.JOBTRACKER );
        clusterOperationHandler2 =
                new ClusterOperationHandler( hadoop, hadoopClusterConfig, ClusterOperationType.INSTALL,
                        NodeType.NAMENODE );
    }


    @Test
    public void testRunWithClusterOperationTypeInstallCluster()
    {
        when( hadoopClusterConfig.getClusterName() ).thenReturn( "test" );
        when( hadoop.getCluster( "test" ) ).thenReturn( hadoopClusterConfig );
        clusterOperationHandler.run();
    }


    @Test
    public void testRunClusterOperationTypeInstall()
    {
        when( hadoopClusterConfig.getClusterName() ).thenReturn( null );
        clusterOperationHandler.run();
    }


    @Test
    public void testRunClusterOperationTypeUninstall()
    {
        clusterOperationHandler1.run();
    }


    @Test
    @Ignore
    public void testRunOperationOnContainers() throws CommandException, EnvironmentNotFoundException
    {
        when( hadoopClusterConfig.getEnvironmentId() ).thenReturn( id );
        when( hadoopClusterConfig.getNameNode() ).thenReturn( id );
        when( hadoopClusterConfig.getJobTracker() ).thenReturn( id );
        when( hadoopClusterConfig.getSecondaryNameNode() ).thenReturn( id );
        when( hadoop.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );

        // NodeType.NAMENODE
        clusterOperationHandler2.runOperationOnContainers( ClusterOperationType.START_ALL );
        clusterOperationHandler2.runOperationOnContainers( ClusterOperationType.STOP_ALL );
        clusterOperationHandler2.runOperationOnContainers( ClusterOperationType.STATUS_ALL );
        clusterOperationHandler2.runOperationOnContainers( ClusterOperationType.DECOMISSION_STATUS );

        // NodeType.JOBTRACKER
        clusterOperationHandler1.runOperationOnContainers( ClusterOperationType.START_ALL );
        clusterOperationHandler1.runOperationOnContainers( ClusterOperationType.STOP_ALL );
        clusterOperationHandler1.runOperationOnContainers( ClusterOperationType.STATUS_ALL );


        // asserts for RunOperationOnContainers method
        assertEquals( id, hadoopClusterConfig.getEnvironmentId() );
        assertEquals( id, hadoopClusterConfig.getNameNode() );
        assertEquals( id, hadoopClusterConfig.getJobTracker() );
        assertEquals( id, hadoopClusterConfig.getSecondaryNameNode() );

        // tests for NodeType.NAMENODE
        verify( containerHost ).execute( new RequestBuilder( "service hadoop-dfs start" ) );
        verify( containerHost ).execute( new RequestBuilder( "service hadoop-dfs stop" ) );
        verify( containerHost ).execute( new RequestBuilder( "service hadoop-dfs status" ) );
        verify( containerHost ).execute( new RequestBuilder( "/opt/hadoop*/bin/hadoop dfsadmin -report" ) );

        // tests for NodeType.JOBTRACKER
        verify( containerHost ).execute( new RequestBuilder( "service hadoop-mapred start" ) );
        verify( containerHost ).execute( new RequestBuilder( "service hadoop-mapred stop" ) );
        verify( containerHost ).execute( new RequestBuilder( "service hadoop-mapred status" ) );
    }


    @Test
    public void testLogStatusResultsWithNodeTypeNONAME()
    {
        when( commandResult.getStdOut() ).thenReturn( "NameNode" );
        clusterOperationHandler.logResults( trackerOperation, commandResult, NodeType.NAMENODE );

        verify( trackerOperation ).addLogDone( String.format( "Node state is %s", NodeState.RUNNING ) );
    }


    @Test
    public void testLogStatusResultsWithNodeTypeJOBTRACKER()
    {
        CommandResult commandResult = mock( CommandResult.class );
        when( commandResult.getStdOut() ).thenReturn( "JobTracker" );
        clusterOperationHandler.logResults( trackerOperation, commandResult, NodeType.JOBTRACKER );

        verify( trackerOperation ).addLogDone( String.format( "Node state is %s", NodeState.RUNNING ) );
    }


    @Test
    public void testLogStatusResultsWithNodeTypeSECONDARY_NAMENODE()
    {
        CommandResult commandResult = mock( CommandResult.class );
        when( commandResult.getStdOut() ).thenReturn( "SecondaryNameNode" );
        clusterOperationHandler.logResults( trackerOperation, commandResult, NodeType.SECONDARY_NAMENODE );

        verify( trackerOperation ).addLogDone( String.format( "Node state is %s", NodeState.RUNNING ) );
    }


    @Test
    public void testLogStatusResultsWithNodeTypeDATANODE()
    {
        CommandResult commandResult = mock( CommandResult.class );
        when( commandResult.getStdOut() ).thenReturn( "DataNode" );
        clusterOperationHandler.logResults( trackerOperation, commandResult, NodeType.DATANODE );

        verify( trackerOperation ).addLogDone( String.format( "Node state is %s", NodeState.RUNNING ) );
    }


    @Test
    public void testLogStatusResultsWithNodeTypeTASKTRACKER()
    {
        CommandResult commandResult = mock( CommandResult.class );
        when( commandResult.getStdOut() ).thenReturn( "TaskTracker" );
        clusterOperationHandler.logResults( trackerOperation, commandResult, NodeType.TASKTRACKER );

        verify( trackerOperation ).addLogDone( String.format( "Node state is %s", NodeState.RUNNING ) );
    }


    @Test
    public void testLogStatusResultsWithNodeTypeSLAVE_NODE()
    {
        CommandResult commandResult = mock( CommandResult.class );
        clusterOperationHandler.logResults( trackerOperation, commandResult, NodeType.SLAVE_NODE );
    }


    @Test
    public void testDestroyCluster()
    {
        PluginDAO pluginDAO = mock( PluginDAO.class );
        when( hadoopClusterConfig.getEnvironmentId() ).thenReturn( id );
        when( hadoop.getPluginDAO() ).thenReturn( pluginDAO );
        when( hadoop.getCluster( "test" ) ).thenReturn( hadoopClusterConfig );
        clusterOperationHandler.destroyCluster();

        assertEquals( hadoopClusterConfig, hadoop.getCluster( "test" ) );
        assertEquals( id, hadoopClusterConfig.getEnvironmentId() );
        verify( hadoop ).getEnvironmentManager();
    }
}