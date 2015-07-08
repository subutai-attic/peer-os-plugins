package io.subutai.plugin.oozie.impl.handler;


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
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.PluginDAO;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.Commands;
import io.subutai.plugin.oozie.impl.OozieImpl;
import io.subutai.plugin.oozie.impl.handler.ClusterOperationHandler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ClusterOperationHandlerTest
{
    private ClusterOperationHandler clusterOperationHandler;
    private ClusterOperationHandler clusterOperationHandler2;
    private UUID uuid;
    @Mock
    RequestBuilder requestBuilder;
    @Mock
    Tracker tracker;
    @Mock
    OozieImpl oozieImpl;
    @Mock
    OozieClusterConfig oozieClusterConfig;
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
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    Hadoop hadoop;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    Commands commands;


    @Before
    public void setUp() throws Exception
    {
        // mock constructor
        uuid = UUID.randomUUID();
        when( oozieImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        // mock runOperationOnContainers method
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );

        clusterOperationHandler =
                new ClusterOperationHandler( oozieImpl, oozieClusterConfig, ClusterOperationType.INSTALL );
        clusterOperationHandler2 =
                new ClusterOperationHandler( oozieImpl, oozieClusterConfig, ClusterOperationType.UNINSTALL );

        when( oozieImpl.getPluginDao() ).thenReturn( pluginDAO );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( uuid );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( myUUID );
    }


    @Test
    public void testRunOperationOnContainers() throws Exception
    {
        clusterOperationHandler.runOperationOnContainers( ClusterOperationType.ADD );
    }


    @Test
    public void testRunOperationTypeInstallMalformedConfiguration() throws Exception
    {
        clusterOperationHandler.run();

        // assertions
        verify( trackerOperation ).addLogFailed( "Malformed configuration" );
    }


    @Test
    public void testRunOperationTypeInstallClusterAlreadyExist()
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( oozieClusterConfig );

        clusterOperationHandler.run();

        // assertions
        verify( trackerOperation ).addLogFailed( String.format( "Cluster with name 'null' already exists" ) );
    }


    @Test
    public void testRunOperationTypeUninstallClusterNotExist() throws Exception
    {
        clusterOperationHandler2.run();
    }


    @Test
    public void testRunOperationTypeUninstallFailed() throws Exception
    {
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( oozieClusterConfig );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( UUID.randomUUID() );
        when( oozieClusterConfig.getClients() ).thenReturn( myUUID );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( false );

        clusterOperationHandler2.run();

        // assertions
        verify( trackerOperation ).addLogFailed( "Uninstallation of oozie client failed" );
        assertFalse( commandResult.hasSucceeded() );
    }


    @Test
    public void testRunOperationTypeUninstall() throws Exception
    {
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( oozieClusterConfig );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( UUID.randomUUID() );
        when( oozieClusterConfig.getClients() ).thenReturn( myUUID );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        clusterOperationHandler2.run();

        // assertions
        verify( trackerOperation ).addLog( "Uninstalling Oozie server..." );
        assertTrue( commandResult.hasSucceeded() );
        verify( trackerOperation ).addLogDone( "Cluster info deleted from DB\nDone" );
    }
}