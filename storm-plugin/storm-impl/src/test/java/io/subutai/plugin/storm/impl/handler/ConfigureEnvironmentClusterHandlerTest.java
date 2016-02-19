package io.subutai.plugin.storm.impl.handler;


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
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.impl.StormImpl;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ConfigureEnvironmentClusterHandlerTest
{
    @Mock
    CommandResult commandResult;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    StormImpl stormImpl;
    @Mock
    StormClusterConfiguration stormClusterConfiguration;
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
    Zookeeper zookeeper;
    @Mock
    ZookeeperClusterConfig zookeeperClusterConfig;
    @Mock
    PeerManager peerManager;
    private ConfigureEnvironmentClusterHandler configureEnvironmentClusterHandler;
    private String id;
    private Set<EnvironmentContainerHost> mySet;
    private Set<String> myUUID;


    @Before
    public void setUp() throws Exception
    {
        mySet = new HashSet<>();
        mySet.add( containerHost );

        myUUID = new HashSet<>();
        myUUID.add( id );

        // mock constructor
        id = UUID.randomUUID().toString();
        when( stormImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );
        when( stormClusterConfiguration.getClusterName() ).thenReturn( "testClusterName" );

        configureEnvironmentClusterHandler =
                new ConfigureEnvironmentClusterHandler( stormImpl, stormClusterConfiguration );

        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( id );
    }


    @Test
    public void testGetTrackerId() throws Exception
    {
        configureEnvironmentClusterHandler.getTrackerId();

        // assertions
        assertNotNull( configureEnvironmentClusterHandler.getTrackerId() );
    }


    @Test
    public void testRunNoEnvironment() throws Exception
    {
        when( stormImpl.getEnvironmentManager() ).thenThrow( EnvironmentNotFoundException.class );

        configureEnvironmentClusterHandler.run();
    }


    @Test
    public void testRun() throws Exception
    {
        when( stormImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( stormClusterConfiguration.getZookeeperClusterName() ).thenReturn( "testZookeeper" );
        when( stormImpl.getZookeeperManager() ).thenReturn( zookeeper );
        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( mySet );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( id );
        when( containerHost.getId() ).thenReturn( id );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( stormImpl.getPluginDAO() ).thenReturn( pluginDAO );

        configureEnvironmentClusterHandler.run();

        // assertions
        assertEquals( pluginDAO, stormImpl.getPluginDAO() );
        verify( trackerOperation ).addLogDone( "Cluster info successfully saved" );
    }
}