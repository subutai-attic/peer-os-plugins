package org.safehaus.subutai.plugin.storm.impl.handler;


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
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.peer.api.PeerManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.storm.impl.StormImpl;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

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
    @Mock CommandResult commandResult;
    @Mock ContainerHost containerHost;
    @Mock StormImpl stormImpl;
    @Mock StormClusterConfiguration stormClusterConfiguration;
    @Mock Tracker tracker;
    @Mock EnvironmentManager environmentManager;
    @Mock TrackerOperation trackerOperation;
    @Mock Environment environment;
    @Mock ClusterSetupStrategy clusterSetupStrategy;
    @Mock PluginDAO pluginDAO;
    @Mock Zookeeper zookeeper;
    @Mock ZookeeperClusterConfig zookeeperClusterConfig;
    @Mock PeerManager peerManager;
    private ConfigureEnvironmentClusterHandler configureEnvironmentClusterHandler;
    private UUID uuid;
    private Set<ContainerHost> mySet;
    private Set<UUID> myUUID;


    @Before
    public void setUp() throws Exception
    {
        mySet = new HashSet<>();
        mySet.add( containerHost );

        myUUID = new HashSet<>();
        myUUID.add( uuid );

        // mock constructor
        uuid = UUID.randomUUID();
        when( stormImpl.getTracker() ).thenReturn( tracker );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );
        when( stormClusterConfiguration.getClusterName() ).thenReturn( "testClusterName" );

        configureEnvironmentClusterHandler =
                new ConfigureEnvironmentClusterHandler( stormImpl, stormClusterConfiguration );

        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( uuid );
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
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( stormClusterConfiguration.isExternalZookeeper() ).thenReturn( true );
        when( stormClusterConfiguration.getZookeeperClusterName() ).thenReturn( "testZookeeper" );
        when( stormImpl.getZookeeperManager() ).thenReturn( zookeeper );
        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( mySet );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( uuid );
        when( containerHost.getId() ).thenReturn( uuid );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( stormImpl.getPluginDAO() ).thenReturn( pluginDAO );

        configureEnvironmentClusterHandler.run();

        // assertions
        assertEquals( pluginDAO, stormImpl.getPluginDAO() );
        verify( trackerOperation ).addLogDone( "Cluster info successfully saved" );
    }
}