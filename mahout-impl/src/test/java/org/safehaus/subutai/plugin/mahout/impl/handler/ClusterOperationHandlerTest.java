package org.safehaus.subutai.plugin.mahout.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;
import org.safehaus.subutai.plugin.mahout.impl.Commands;
import org.safehaus.subutai.plugin.mahout.impl.MahoutImpl;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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
    MahoutImpl mahoutImpl;
    @Mock
    MahoutClusterConfig mahoutClusterConfig;
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
        when( mahoutImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );

        // mock runOperationOnContainers method
        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );

        clusterOperationHandler =
                new ClusterOperationHandler( mahoutImpl, mahoutClusterConfig, ClusterOperationType.INSTALL );
        clusterOperationHandler2 =
                new ClusterOperationHandler( mahoutImpl, mahoutClusterConfig, ClusterOperationType.DESTROY );

        when( mahoutImpl.getPluginDAO() ).thenReturn( pluginDAO );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( uuid );
        when( mahoutClusterConfig.getNodes() ).thenReturn( myUUID );
    }


    @Test
    public void testDestroyCluster()
    {
        clusterOperationHandler.destroyCluster();
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
    }


    @Test
    public void testRunOperationTypeInstallClusterAlreadyExist()
    {
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( mahoutClusterConfig );

        clusterOperationHandler.run();
    }





    @Test
    public void testRunOperationTypeUninstallClusterNotExist() throws Exception
    {
        clusterOperationHandler2.run();
    }


    @Test
    public void testRunOperationTypeUninstallFailed() throws Exception
    {
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( mahoutClusterConfig );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( UUID.randomUUID() );
        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any(UUID.class) ) ).thenReturn( environment );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( false );
        when( mahoutImpl.getCommands() ).thenReturn( commands );
        when( commands.getUninstallCommand() ).thenReturn( requestBuilder );

        clusterOperationHandler2.run();
    }

    @Test
    public void testRunOperationTypeUninstall() throws Exception
    {
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( mahoutClusterConfig );
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add( UUID.randomUUID() );
        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any(UUID.class) ) ).thenReturn( environment );
        when( environment.getContainerHostById( any(UUID.class) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( mahoutImpl.getCommands() ).thenReturn( commands );
        when( commands.getUninstallCommand() ).thenReturn( requestBuilder );

        clusterOperationHandler2.run();
    }

}