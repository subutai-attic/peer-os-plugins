package io.subutai.plugin.lucene.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.lucene.api.LuceneConfig;
import io.subutai.plugin.lucene.impl.LuceneImpl;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class NodeOperationHandlerTest
{
    private NodeOperationHandler nodeOperationHandler;
    private NodeOperationHandler nodeOperationHandler2;
    private String id;
    private Set<EnvironmentContainerHost> mySet;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    LuceneImpl luceneImpl;
    @Mock
    LuceneConfig luceneConfig;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    Hadoop hadoop;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    CommandResult commandResult;


    @Before
    public void setUp() throws Exception
    {
        // mock constructor
        id = UUID.randomUUID().toString();
        when( luceneImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );
        when( luceneImpl.getCluster( anyString() ) ).thenReturn( luceneConfig );

        nodeOperationHandler =
                new NodeOperationHandler( luceneImpl, "testClusterName", "testHostName", NodeOperationType.INSTALL );
        nodeOperationHandler2 =
                new NodeOperationHandler( luceneImpl, "testClusterName", "testHostName", NodeOperationType.UNINSTALL );

        when( luceneImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        mySet = new HashSet<>();
        mySet.add( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
    }


    @Test
    public void testRunOperationTypeInstallClusterDoesNotExist() throws Exception
    {
        when( luceneImpl.getCluster( anyString() ) ).thenReturn( null );

        nodeOperationHandler.run();
    }


    @Test
    public void testRunOperationTypeInstallNoEnvironment() throws Exception
    {
        when( luceneImpl.getCluster( anyString() ) ).thenReturn( luceneConfig );
        when( environmentManager.loadEnvironment( any( String.class ) ) )
                .thenThrow( EnvironmentNotFoundException.class );

        nodeOperationHandler.run();

        // assertions
        verify( trackerOperation ).addLogFailed( String.format( "Environment not found: %s",
                "io.subutai" + ".common.environment.EnvironmentNotFoundException" ) );
    }


    @Test
    public void testRunOperationTypeInstallCommandResultHasNotSucceeded() throws Exception
    {
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( containerHost.getHostname() ).thenReturn( "testHostName" );

        nodeOperationHandler.run();

        // assertions
        verify( trackerOperation )
                .addLogFailed( "Could not install " + LuceneConfig.PRODUCT_KEY + " to node " + "testHostName" );
        assertFalse( commandResult.hasSucceeded() );
    }


    @Test
    public void testRunOperationTypeInstall() throws Exception
    {
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( containerHost.getHostname() ).thenReturn( "testHostName" );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        nodeOperationHandler.run();

        // assertions
        verify( luceneImpl ).saveConfig( luceneConfig );
        verify( trackerOperation ).addLogDone(
                LuceneConfig.PRODUCT_KEY + " is installed on node " + containerHost.getHostname() + " successfully." );
    }


    @Test
    public void testRunOperationTypeUninstallCommandResultHasNotSucceeded() throws EnvironmentNotFoundException
    {
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( containerHost.getHostname() ).thenReturn( "testHostName" );

        nodeOperationHandler2.run();

        // assertions
        verify( trackerOperation )
                .addLogFailed( "Could not uninstall " + LuceneConfig.PRODUCT_KEY + " from node " + "testHostName" );
    }


    @Test
    public void testRunOperationTypeUninstall() throws EnvironmentNotFoundException, ClusterException
    {
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( containerHost.getHostname() ).thenReturn( "testHostName" );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        nodeOperationHandler2.run();

        // assertions
        verify( luceneImpl ).saveConfig( luceneConfig );
        verify( trackerOperation ).addLogDone(
                LuceneConfig.PRODUCT_KEY + " is uninstalled from node " + containerHost.getHostname()
                        + " successfully." );
    }
}