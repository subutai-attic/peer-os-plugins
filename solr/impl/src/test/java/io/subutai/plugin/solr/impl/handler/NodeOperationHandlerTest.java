package io.subutai.plugin.solr.impl.handler;


import java.util.HashSet;
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
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.plugin.solr.api.SolrClusterConfig;
import io.subutai.plugin.solr.impl.Commands;
import io.subutai.plugin.solr.impl.SolrImpl;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class NodeOperationHandlerTest
{
    private NodeOperationHandler nodeOperationHandler;
    private NodeOperationHandler nodeOperationHandler2;
    private NodeOperationHandler nodeOperationHandler3;
    private UUID uuid;
    @Mock
    SolrImpl solrImpl;
    @Mock
    SolrClusterConfig solrClusterConfig;
    @Mock
    Commands commands;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    CommandResult commandResult;
    @Mock
    RequestBuilder requestBuilder;


    @Before
    public void setUp() throws Exception
    {
        // mock constructor
        uuid = new UUID( 50, 50 );
        when( solrImpl.getCluster( "testClusterName" ) ).thenReturn( solrClusterConfig );
        when( solrImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        nodeOperationHandler =
                new NodeOperationHandler( solrImpl, "testClusterName", "testHostName", NodeOperationType.START );
        nodeOperationHandler2 =
                new NodeOperationHandler( solrImpl, "testClusterName", "testHostName", NodeOperationType.STOP );
        nodeOperationHandler3 =
                new NodeOperationHandler( solrImpl, "testClusterName", "testHostName", NodeOperationType.STATUS );

        // mock run method
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( containerHost.getHostname() ).thenReturn( "testHostName" );
        when( containerHost.getId() ).thenReturn( UUID.randomUUID().toString() );
        when( solrImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );

        // assertions
        assertNotNull( solrImpl.getEnvironmentManager() );
        assertNotNull( environment.getContainerHosts() );
    }


    @Test
    public void testRunNodeSolrClusterNull() throws Exception
    {
        when( solrImpl.getCluster( anyString() ) ).thenReturn( null );

        nodeOperationHandler.run();
    }


    @Test
    public void testRunNodeTypeStart() throws EnvironmentNotFoundException, CommandException
    {
//        when( containerHost.execute( Commands.getStartZkServerCommand() ) ).thenReturn( commandResult );
//        when( commandResult.getStdOut() ).thenReturn( "Solr is running" );
//
//        nodeOperationHandler.run();
//
//        // assertions
//        verify( containerHost ).execute( Commands.getStartZkServerCommand() );
    }


    @Test
    public void testRunNodeTypeStop() throws EnvironmentNotFoundException, CommandException
    {
//        when( containerHost.execute( Commands.getStopZkServerCommand() ) ).thenReturn( commandResult );
//        when( commandResult.getStdOut() ).thenReturn( "Solr is running" );
//
//        nodeOperationHandler2.run();
//
//        // assertions
//        verify( containerHost ).execute( Commands.getStopZkServerCommand() );
    }


    @Test
    public void testRunNodeTypeStatus() throws EnvironmentNotFoundException, CommandException
    {
//        when( containerHost.execute( Commands.getSolrStatusCommand() ) ).thenReturn( commandResult );
//        when( commandResult.getStdOut() ).thenReturn( "Solr is running" );
//
//        nodeOperationHandler3.run();
//
//        // assertions
//        verify( containerHost ).execute( Commands.getSolrStatusCommand() );
    }


    @Test
    public void testLogStatusResults()
    {
        when( commandResult.getStdOut() ).thenReturn( "Solr is running" );

        nodeOperationHandler.logStatusResults( trackerOperation, commandResult );
    }


    @Test
    public void testLogStatusResults2()
    {
        when( commandResult.getStdOut() ).thenReturn( "Solr is not running" );

        nodeOperationHandler.logStatusResults( trackerOperation, commandResult );
    }
}