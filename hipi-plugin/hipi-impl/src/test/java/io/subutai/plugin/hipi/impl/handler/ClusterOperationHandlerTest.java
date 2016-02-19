package io.subutai.plugin.hipi.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.environment.Topology;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.hipi.api.HipiConfig;
import io.subutai.plugin.hipi.impl.HipiImpl;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ClusterOperationHandlerTest
{
    @Mock
    CommandResult commandResult;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    HipiImpl hipiImpl;
    @Mock
    HipiConfig hipiConfig;
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
    Topology topology;
    @Mock
    CommandUtil commandUtil;
    private ClusterOperationHandler clusterOperationHandler;
    private ClusterOperationHandler clusterOperationHandler2;
    private ClusterOperationHandler clusterOperationHandler3;
    private ClusterOperationHandler clusterOperationHandler4;
    private ClusterOperationHandler clusterOperationHandler5;
    private String id;


    @Before
    public void setUp() throws Exception
    {
        // mock constructor
        id = UUID.randomUUID().toString();
        when( hipiImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );

        clusterOperationHandler = new ClusterOperationHandler( hipiImpl, hipiConfig, ClusterOperationType.INSTALL );
        clusterOperationHandler2 = new ClusterOperationHandler( hipiImpl, hipiConfig, ClusterOperationType.UNINSTALL );
        clusterOperationHandler3 = new ClusterOperationHandler( hipiImpl, hipiConfig, ClusterOperationType.STATUS_ALL );
        clusterOperationHandler4 = new ClusterOperationHandler( hipiImpl, hipiConfig, ClusterOperationType.START_ALL );
        clusterOperationHandler5 = new ClusterOperationHandler( hipiImpl, hipiConfig, ClusterOperationType.STOP_ALL );
    }


    @Test
    public void testRunOperationOnContainersInstall() throws Exception
    {
        when( hipiImpl.getClusterSetupStrategy( hipiConfig, trackerOperation ) ).thenReturn( clusterSetupStrategy );

        clusterOperationHandler.run();

        // assertions
        assertNotNull( hipiImpl.getClusterSetupStrategy( hipiConfig, trackerOperation ) );
        verify( clusterSetupStrategy ).setup();
        verify( trackerOperation ).addLogDone( "Installing successfully completed" );
    }


    @Test
    public void testRunOperationOnContainersInstallationFailed() throws Exception
    {
        when( hipiImpl.getClusterSetupStrategy( hipiConfig, trackerOperation ) ).thenReturn( clusterSetupStrategy );
        when( clusterSetupStrategy.setup() ).thenThrow( ClusterSetupException.class );

        clusterOperationHandler.run();

        // assertions
        assertNotNull( hipiImpl.getClusterSetupStrategy( hipiConfig, trackerOperation ) );
    }


    @Test
    public void testRunOperationOnContainersUninstallHipiNotFound() throws Exception
    {
        clusterOperationHandler2.run();
    }


    @Test
    public void testRunOperationOnContainersUninstallEnvNotFound() throws Exception
    {
        when( hipiImpl.getCluster( anyString() ) ).thenReturn( hipiConfig );
        when( hipiImpl.getEnvironmentManager() ).thenThrow( EnvironmentNotFoundException.class );

        clusterOperationHandler2.run();
    }


    @Test
    public void testRunOperationOnContainersUninstallContainerNotConnected() throws Exception
    {
        when( hipiImpl.getCluster( anyString() ) ).thenReturn( hipiConfig );
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( mySet );

        clusterOperationHandler2.run();
    }


    @Test
    public void testRunOperationOnContainersUninstallContainersNotFound() throws Exception
    {
        when( hipiImpl.getCluster( anyString() ) ).thenReturn( hipiConfig );
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) )
                .thenThrow( ContainerHostNotFoundException.class );

        clusterOperationHandler2.run();
    }


    @Test
    public void testRunOperationOnContainers() throws Exception
    {
        clusterOperationHandler3.run();
        clusterOperationHandler4.run();
        clusterOperationHandler5.run();
    }
}