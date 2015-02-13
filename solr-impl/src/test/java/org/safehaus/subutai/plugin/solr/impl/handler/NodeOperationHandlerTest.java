package org.safehaus.subutai.plugin.solr.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;
import org.safehaus.subutai.plugin.solr.impl.Commands;
import org.safehaus.subutai.plugin.solr.impl.SolrImpl;

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
    ContainerHost containerHost;
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
        Set<ContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( containerHost.getHostname() ).thenReturn( "testHostName" );
        when( solrImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );

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
        when( containerHost.execute( new RequestBuilder( Commands.startCommand ) ) ).thenReturn( commandResult );

        nodeOperationHandler.run();

        // assertions
        verify( containerHost ).execute( new RequestBuilder( Commands.startCommand ) );
    }


    @Test
    public void testRunNodeTypeStop() throws EnvironmentNotFoundException, CommandException
    {
        when( containerHost.execute( new RequestBuilder( Commands.stopCommand ) ) ).thenReturn( commandResult );

        nodeOperationHandler2.run();

        // assertions
        verify( containerHost ).execute( new RequestBuilder( Commands.stopCommand ) );
    }


    @Test
    public void testRunNodeTypeStatus() throws EnvironmentNotFoundException, CommandException
    {
        when( containerHost.execute( new RequestBuilder( Commands.statusCommand ) ) ).thenReturn( commandResult );

        nodeOperationHandler3.run();

        // assertions
        verify( containerHost ).execute( new RequestBuilder( Commands.statusCommand ) );
    }


    @Test
    public void testLogStatusResults()
    {
        when( commandResult.getExitCode() ).thenReturn( 768 );

        nodeOperationHandler.logStatusResults( trackerOperation, commandResult );
    }

    @Test
    public void testLogStatusResults2()
    {
        when( commandResult.getExitCode() ).thenReturn( 5 );

        nodeOperationHandler.logStatusResults( trackerOperation, commandResult );
    }

}