package org.safehaus.subutai.plugin.storm.impl;


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
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ClusterConfigurationTest
{
    private ClusterConfiguration clusterConfiguration;
    private UUID uuid;
    private Set<ContainerHost> mySet;
    private Set<UUID> myUUID;
    @Mock
    CommandResult commandResult;
    @Mock
    ContainerHost containerHost;
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
    ConfigBase configBase;


    @Before
    public void setUp() throws Exception
    {
        uuid = UUID.randomUUID();
        mySet = new HashSet<>();
        mySet.add( containerHost );

        myUUID = new HashSet<>();
        myUUID.add( uuid );
        when( stormImpl.getEnvironmentManager() ).thenReturn( environmentManager );

        clusterConfiguration = new ClusterConfiguration( trackerOperation, stormImpl );

        // mock
        when( zookeeperClusterConfig.getEnvironmentId() ).thenReturn( uuid );
        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( uuid );
        when( containerHost.execute( any( RequestBuilder.class) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( stormImpl.getPluginDAO() ).thenReturn( pluginDAO );
    }


    @Test
    public void testConfigureClusterNoEnvironmentException() throws Exception
    {
        when( environmentManager.findEnvironment( any(UUID.class) ) ).thenThrow( EnvironmentNotFoundException.class );
        when( environment.getContainerHostById( any(UUID.class) ) ).thenReturn( containerHost );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( uuid );

        clusterConfiguration.configureCluster( stormClusterConfiguration, environment );
    }

    @Test
    public void testConfigureClusterNoContainerHostException() throws Exception
    {
        when( environmentManager.findEnvironment( any(UUID.class) ) ).thenThrow( ContainerHostNotFoundException.class );
        when( environment.getContainerHostById( any(UUID.class) ) ).thenReturn( containerHost );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( uuid );

        clusterConfiguration.configureCluster( stormClusterConfiguration, environment );
    }

}