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
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.lucene.api.LuceneConfig;
import io.subutai.plugin.lucene.impl.LuceneImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ClusterOperationHandlerTest
{
    private ClusterOperationHandler clusterOperationHandler;
    private ClusterOperationHandler clusterOperationHandler2;
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

        clusterOperationHandler = new ClusterOperationHandler( luceneImpl, luceneConfig, ClusterOperationType.INSTALL );
        clusterOperationHandler2 =
                new ClusterOperationHandler( luceneImpl, luceneConfig, ClusterOperationType.DESTROY );

        when( luceneImpl.getHadoopManager() ).thenReturn( hadoop );
        when( luceneImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        mySet = new HashSet<>();
        mySet.add( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
    }


    @Test
    public void testRunOperationOnContainers() throws Exception
    {
        clusterOperationHandler.runOperationOnContainers( ClusterOperationType.INSTALL );
    }


    @Test
    public void testRunOperationTypeInstallHadoopNotFound() throws Exception
    {
        when( hadoop.getCluster( anyString() ) ).thenReturn( null );

        clusterOperationHandler.run();
    }


    @Test
    public void testRunOperationTypeInstallNoEnvironment() throws Exception
    {
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( environmentManager.loadEnvironment( any( String.class ) ) )
                .thenThrow( EnvironmentNotFoundException.class );

        clusterOperationHandler.run();
    }


    @Test
    public void testRunOperationTypeInstall() throws Exception
    {
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( luceneImpl.getClusterSetupStrategy( environment, luceneConfig, trackerOperation ) )
                .thenReturn( clusterSetupStrategy );

        clusterOperationHandler.run();
    }


    @Test
    public void testRunOperationTypeDestroyClusterDoesNotExist() throws Exception
    {
        when( luceneImpl.getCluster( anyString() ) ).thenReturn( null );

        clusterOperationHandler2.run();
    }


    @Test
    public void testRunOperationTypeDestroyNoEnvironment() throws Exception
    {
        when( luceneImpl.getCluster( anyString() ) ).thenReturn( luceneConfig );
        when( environmentManager.loadEnvironment( any( String.class ) ) )
                .thenThrow( EnvironmentNotFoundException.class );

        clusterOperationHandler2.run();

        // assertions
        verify( trackerOperation ).addLogFailed( String.format( "Environment not found: %s",
                "io.subutai" + ".common.environment.EnvironmentNotFoundException" ) );
    }


    @Test
    public void testRunOperationTypeDestroyFailedObtainingContainers() throws Exception
    {
        when( luceneImpl.getCluster( anyString() ) ).thenReturn( luceneConfig );
        when( environmentManager.loadEnvironment( any( String.class ) ) )
                .thenThrow( ContainerHostNotFoundException.class );

        clusterOperationHandler2.run();

        // assertions
        verify( trackerOperation ).addLogFailed( String.format( "Failed obtaining environment containers: %s",
                "io.subutai" + ".common.environment.ContainerHostNotFoundException" ) );
    }


    @Test
    public void testRunOperationTypeDestroyCommandResultHasNotSucceeded() throws Exception
    {
        when( luceneImpl.getCluster( anyString() ) ).thenReturn( luceneConfig );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( mySet );

        clusterOperationHandler2.run();

        // assertions
        verify( trackerOperation ).addLogFailed( "Uninstallation failed" );
        assertFalse( commandResult.hasSucceeded() );
    }


    @Test
    public void testRunOperationTypeDestroy() throws Exception
    {
        when( luceneImpl.getCluster( anyString() ) ).thenReturn( luceneConfig );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( mySet );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( luceneImpl.getPluginDao() ).thenReturn( pluginDAO );

        clusterOperationHandler2.run();

        // assertions
        verify( trackerOperation ).addLog( "Updating db..." );
        assertEquals( pluginDAO, luceneImpl.getPluginDao() );
        verify( trackerOperation ).addLogDone( "Cluster info deleted from DB\nDone" );
        assertTrue( commandResult.hasSucceeded() );
    }
}