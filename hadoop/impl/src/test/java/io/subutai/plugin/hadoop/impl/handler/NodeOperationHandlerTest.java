package io.subutai.plugin.hadoop.impl.handler;


import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.host.HostInterfaceModel;
import io.subutai.common.host.HostInterfaces;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hadoop.impl.HadoopImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class NodeOperationHandlerTest
{
    private static final String ETH0_IP = "192.168.0.1";
    private NodeOperationHandler nodeOperationHandler;
    private String id;
    @Mock
    HadoopImpl hadoopImpl;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    EnvironmentContainerHost containerHost2;
    @Mock
    Environment environment;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    CommandResult commandResult;
    @Mock
    ClusterOperationHandler clusterOperationHandler;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    RequestBuilder requestBuilder;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    private HostInterfaceModel eth0Interface;


    @Before
    public void setUp() throws CommandException, EnvironmentNotFoundException, ContainerHostNotFoundException
    {
        when( eth0Interface.getIp() ).thenReturn( ETH0_IP );
        when( commandResult.getStdOut() ).thenReturn( "NameNode" );
        Set<EnvironmentContainerHost> mySet = mock( Set.class );
        mySet.add( containerHost );
        mySet.add( containerHost2 );
        when( hadoopImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getId() ).thenReturn( id );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( hadoopClusterConfig.getNameNode() ).thenReturn( UUID.randomUUID().toString() );
        Iterator<EnvironmentContainerHost> iterator = mock( Iterator.class );
        when( mySet.iterator() ).thenReturn( iterator );
        when( iterator.hasNext() ).thenReturn( true ).thenReturn( true ).thenReturn( false );
        when( iterator.next() ).thenReturn( containerHost ).thenReturn( containerHost2 );
        when( containerHost.getHostname() ).thenReturn( "test" );
        when( containerHost.getInterfaceByName( "eth0" ) ).thenReturn( eth0Interface );
        when( containerHost2.getHostname() ).thenReturn( "test" );
        when( hadoopImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( hadoopImpl.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( hadoopClusterConfig.getNameNode() ).thenReturn( UUID.randomUUID().toString() );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostById( any( String.class ) ).getId() )
                .thenReturn( UUID.randomUUID().toString() );
        when( hadoopImpl.getPluginDAO() ).thenReturn( pluginDAO );

        nodeOperationHandler =
                new NodeOperationHandler( hadoopImpl, "test", "test", NodeOperationType.INSTALL, NodeType.NAMENODE );
        id = UUID.randomUUID().toString();

        // assertions
        verify( hadoopImpl ).getTracker();
        assertEquals( tracker, hadoopImpl.getTracker() );
    }


    @Test
    public void testRun() throws EnvironmentNotFoundException
    {
        Set<EnvironmentContainerHost> mySet = mock( Set.class );
        mySet.add( containerHost );
        mySet.add( containerHost2 );

        when( hadoopImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getId() ).thenReturn( id );

        when( environment.getContainerHosts() ).thenReturn( mySet );
        Iterator<EnvironmentContainerHost> iterator = mock( Iterator.class );
        when( mySet.iterator() ).thenReturn( iterator );
        when( iterator.hasNext() ).thenReturn( true ).thenReturn( true ).thenReturn( false );
        when( iterator.next() ).thenReturn( containerHost ).thenReturn( containerHost2 );

        when( containerHost.getHostname() ).thenReturn( "test" );
        when( containerHost2.getHostname() ).thenReturn( "test" );
        when( hadoopImpl.getTracker() ).thenReturn( tracker );
        when( hadoopImpl.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );

        nodeOperationHandler =
                new NodeOperationHandler( hadoopImpl, "test", "test", NodeOperationType.INSTALL, NodeType.NAMENODE );
        nodeOperationHandler.run();

        // assertions
        verify( hadoopImpl ).getEnvironmentManager();
        verify( hadoopImpl, atLeastOnce() ).getCluster( "test" );
        assertEquals( "test", containerHost.getHostname() );
        assertEquals( id, environment.getId() );
    }


    @Test()
    public void testRunCommandWithNodeOperationTypeSTART() throws CommandException
    {
        nodeOperationHandler =
                new NodeOperationHandler( hadoopImpl, "test", "test", NodeOperationType.INSTALL, NodeType.NAMENODE );

        nodeOperationHandler.runCommand( containerHost, NodeOperationType.START, NodeType.NAMENODE );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.START, NodeType.JOBTRACKER );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.START, NodeType.TASKTRACKER );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.START, NodeType.DATANODE );

        // assertions
        assertEquals( commandResult, containerHost.execute( any( RequestBuilder.class ) ) );
    }


    @Test()
    public void testRunCommandWithNodeOperationTypeSTOP() throws CommandException
    {
        nodeOperationHandler =
                new NodeOperationHandler( hadoopImpl, "test", "test", NodeOperationType.INSTALL, NodeType.NAMENODE );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.STOP, NodeType.NAMENODE );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.STOP, NodeType.JOBTRACKER );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.STOP, NodeType.TASKTRACKER );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.STOP, NodeType.DATANODE );

        // assertions
        assertEquals( commandResult, containerHost.execute( any( RequestBuilder.class ) ) );
    }


    @Test()
    public void testRunCommandWithNodeOperationTypeSTATUS() throws CommandException
    {
        nodeOperationHandler =
                new NodeOperationHandler( hadoopImpl, "test", "test", NodeOperationType.INSTALL, NodeType.NAMENODE );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.STATUS, NodeType.NAMENODE );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.STATUS, NodeType.JOBTRACKER );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.STATUS, NodeType.TASKTRACKER );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.STATUS, NodeType.DATANODE );
        nodeOperationHandler.runCommand( containerHost, NodeOperationType.STATUS, NodeType.SECONDARY_NAMENODE );

        // assertions
        assertEquals( commandResult, containerHost.execute( any( RequestBuilder.class ) ) );
    }


    @Test
    public void testFindNodeInCluster() throws CommandException
    {
        nodeOperationHandler =
                new NodeOperationHandler( hadoopImpl, "test", "test", NodeOperationType.INSTALL, NodeType.NAMENODE );
        containerHost2 = nodeOperationHandler.findNodeInCluster( "test" );

        // assertions
        assertNotNull( nodeOperationHandler.findNodeInCluster( "tes" ) );
        assertEquals( containerHost, containerHost2 );
    }


    @Test
    public void testExcludeNode() throws CommandException
    {
        nodeOperationHandler =
                new NodeOperationHandler( hadoopImpl, "test", "test", NodeOperationType.INSTALL, NodeType.NAMENODE );
        nodeOperationHandler.excludeNode();

        // assertions
//        verify( hadoopImpl ).getPluginDAO();
//        verify( trackerOperation ).addLogDone( "Cluster info saved to DB" );
    }


    @Test
    public void testIncludeNode() throws CommandException
    {
        nodeOperationHandler =
                new NodeOperationHandler( hadoopImpl, "test", "test", NodeOperationType.INSTALL, NodeType.NAMENODE );
        nodeOperationHandler.includeNode();

        // assertions
//        verify( hadoopImpl ).getPluginDAO();
//        verify( trackerOperation ).addLogDone( "Cluster info saved to DB" );
    }
}