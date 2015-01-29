package org.safehaus.subutai.plugin.hadoop.impl.handler;


import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.environment.Topology;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.env.api.exception.EnvironmentCreationException;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentBuildException;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.NodeState;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterOperationHandlerTest
{
    ClusterOperationHandler clusterOperationHandler;
    ClusterOperationHandler clusterOperationHandler1;
    ClusterOperationHandler clusterOperationHandler2;
    ClusterOperationHandler clusterOperationHandler3;
    TrackerOperation trackerOperation;
    UUID uuid;
    ExecutorService executorService;
    HadoopClusterConfig hadoopClusterConfig;
    HadoopImpl hadoop;
    Environment environment;
    EnvironmentManager environmentManager;
    ContainerHost containerHost;
    ContainerHost containerHost2;
    CommandResult commandResult;

    @Before
    public void setUp() throws ClusterSetupException, EnvironmentBuildException, EnvironmentNotFoundException,
            ContainerHostNotFoundException, EnvironmentCreationException
    {
        containerHost = mock(ContainerHost.class);
        containerHost2 = mock(ContainerHost.class);
        Set<ContainerHost> mySet = mock(Set.class);
        mySet.add(containerHost);
        mySet.add(containerHost2);
        hadoop = mock(HadoopImpl.class);
        environment = mock(Environment.class);
        environmentManager = mock(EnvironmentManager.class);
        hadoopClusterConfig = mock(HadoopClusterConfig.class);
        executorService = mock(ExecutorService.class);
        trackerOperation = mock(TrackerOperation.class);
        commandResult = mock(CommandResult.class);
        uuid = new UUID(50, 50);
        Tracker tracker = mock(Tracker.class);

        when(trackerOperation.getId()).thenReturn(uuid);
        when(tracker.createTrackerOperation(anyString(), anyString())).thenReturn(trackerOperation);
        when(hadoop.getTracker()).thenReturn(tracker);
        when(hadoop.getEnvironmentManager()).thenReturn(environmentManager);
        when( environmentManager.findEnvironment( uuid ) ).thenReturn( environment );
        when(environment.getContainerHostById( uuid )).thenReturn(containerHost);

        when( environmentManager.createEnvironment( anyString(), any( Topology.class ), anyBoolean() ) )
                .thenReturn(environment);
        when(hadoopClusterConfig.getNameNode()).thenReturn(uuid);

        when(hadoopClusterConfig.getClusterName()).thenReturn("test");

        clusterOperationHandler = new ClusterOperationHandler(hadoop, hadoopClusterConfig, ClusterOperationType
                .INSTALL, NodeType.NAMENODE);
        clusterOperationHandler1 = new ClusterOperationHandler(hadoop, hadoopClusterConfig, ClusterOperationType
                .UNINSTALL, NodeType.JOBTRACKER);
        clusterOperationHandler2 = new ClusterOperationHandler(hadoop, hadoopClusterConfig, ClusterOperationType
                .STATUS_ALL, NodeType.SECONDARY_NAMENODE);
        clusterOperationHandler3 = new ClusterOperationHandler(hadoop, hadoopClusterConfig, ClusterOperationType
                .DECOMISSION_STATUS, NodeType.MASTER_NODE);
    }

    @Test
    public void testRunWithClusterOperationTypeInstallCluster()
    {
        when(hadoopClusterConfig.getClusterName()).thenReturn("test");
        when(hadoop.getCluster("test")).thenReturn(hadoopClusterConfig);
        clusterOperationHandler.run();
    }

    @Test
    public void testRunClusterOperationTypeInstall()
    {
        when(hadoopClusterConfig.getClusterName()).thenReturn(null);
        clusterOperationHandler.run();
    }

    @Test
    public void testRunClusterOperationTypeUninstall()
    {
        clusterOperationHandler1.run();
    }

    @Test
    public void testRunOperationOnContainers() throws CommandException, EnvironmentNotFoundException
    {
        when(hadoopClusterConfig.getEnvironmentId()).thenReturn(uuid);
        when(hadoopClusterConfig.getNameNode()).thenReturn(uuid);
        when(hadoopClusterConfig.getJobTracker()).thenReturn(uuid);
        when(hadoopClusterConfig.getSecondaryNameNode()).thenReturn(uuid);
        when(hadoop.getEnvironmentManager()).thenReturn(environmentManager);
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when(containerHost.execute(any(RequestBuilder.class))).thenReturn(commandResult);

        // NodeType.NAMENODE
        clusterOperationHandler.runOperationOnContainers(ClusterOperationType.START_ALL);
        clusterOperationHandler.runOperationOnContainers(ClusterOperationType.STOP_ALL);
        clusterOperationHandler.runOperationOnContainers(ClusterOperationType.STATUS_ALL);
        clusterOperationHandler.runOperationOnContainers(ClusterOperationType.DECOMISSION_STATUS);

        // NodeType.JOBTRACKER
        clusterOperationHandler1.runOperationOnContainers(ClusterOperationType.START_ALL);
        clusterOperationHandler1.runOperationOnContainers(ClusterOperationType.STOP_ALL);
        clusterOperationHandler1.runOperationOnContainers(ClusterOperationType.STATUS_ALL);


        // asserts for RunOperationOnContainers method
        assertEquals(uuid, hadoopClusterConfig.getEnvironmentId());
        assertEquals(uuid, hadoopClusterConfig.getNameNode());
        assertEquals(uuid, hadoopClusterConfig.getJobTracker());
        assertEquals(uuid, hadoopClusterConfig.getSecondaryNameNode());

        // tests for NodeType.NAMENODE
        verify(containerHost).execute(new RequestBuilder("service hadoop-dfs start"));
        verify(containerHost).execute(new RequestBuilder("service hadoop-dfs stop"));
        verify(containerHost).execute(new RequestBuilder("service hadoop-dfs status"));
        verify(containerHost).execute(new RequestBuilder(". /etc/profile && " + "hadoop dfsadmin -report"));

        // tests for NodeType.JOBTRACKER
        verify(containerHost).execute(new RequestBuilder("service hadoop-mapred start"));
        verify(containerHost).execute(new RequestBuilder("service hadoop-mapred stop"));
        verify(containerHost).execute(new RequestBuilder("service hadoop-mapred status"));

    }

    @Test
    public void testLogStatusResultsWithNodeTypeNONAME()
    {
        when(commandResult.getStdOut()).thenReturn("NameNode");
        clusterOperationHandler.logStatusResults(trackerOperation, commandResult, NodeType.NAMENODE);

        verify(trackerOperation).addLogDone(String.format("Node state is %s", NodeState.RUNNING));
    }

    @Test
    public void testLogStatusResultsWithNodeTypeJOBTRACKER()
    {
        CommandResult commandResult = mock(CommandResult.class);
        when(commandResult.getStdOut()).thenReturn("JobTracker");
        clusterOperationHandler.logStatusResults(trackerOperation, commandResult, NodeType.JOBTRACKER);

        verify(trackerOperation).addLogDone(String.format("Node state is %s", NodeState.RUNNING));
    }

    @Test
    public void testLogStatusResultsWithNodeTypeSECONDARY_NAMENODE()
    {
        CommandResult commandResult = mock(CommandResult.class);
        when(commandResult.getStdOut()).thenReturn("SecondaryNameNode");
        clusterOperationHandler.logStatusResults(trackerOperation, commandResult, NodeType.SECONDARY_NAMENODE);

        verify(trackerOperation).addLogDone(String.format("Node state is %s", NodeState.RUNNING));
    }

    @Test
    public void testLogStatusResultsWithNodeTypeDATANODE()
    {
        CommandResult commandResult = mock(CommandResult.class);
        when(commandResult.getStdOut()).thenReturn("DataNode");
        clusterOperationHandler.logStatusResults(trackerOperation, commandResult, NodeType.DATANODE);

        verify(trackerOperation).addLogDone(String.format("Node state is %s", NodeState.RUNNING));
    }

    @Test
    public void testLogStatusResultsWithNodeTypeTASKTRACKER()
    {
        CommandResult commandResult = mock(CommandResult.class);
        when(commandResult.getStdOut()).thenReturn("TaskTracker");
        clusterOperationHandler.logStatusResults(trackerOperation, commandResult, NodeType.TASKTRACKER);

        verify(trackerOperation).addLogDone(String.format("Node state is %s", NodeState.RUNNING));
    }

    @Test
    public void testLogStatusResultsWithNodeTypeSLAVE_NODE()
    {
        CommandResult commandResult = mock(CommandResult.class);
        clusterOperationHandler.logStatusResults(trackerOperation, commandResult, NodeType.SLAVE_NODE);

    }

    @Test
    public void testDestroyCluster()
    {
        PluginDAO pluginDAO = mock(PluginDAO.class);
        when(hadoopClusterConfig.getEnvironmentId()).thenReturn(uuid);
        when(hadoop.getPluginDAO()).thenReturn(pluginDAO);
        when(hadoop.getCluster("test")).thenReturn(hadoopClusterConfig);
        clusterOperationHandler.destroyCluster();

        assertEquals(hadoopClusterConfig, hadoop.getCluster("test"));
        assertEquals(uuid, hadoopClusterConfig.getEnvironmentId());
        verify(hadoop).getEnvironmentManager();
    }

}