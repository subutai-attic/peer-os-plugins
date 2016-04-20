package io.subutai.plugin.storm.impl;


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
import io.subutai.common.host.HostInterface;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
//import io.subutai.plugin.zookeeper.api.Zookeeper;
//import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ClusterConfigurationTest
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
//    @Mock
//    Zookeeper zookeeper;
//    @Mock
//    ZookeeperClusterConfig zookeeperClusterConfig;
    @Mock
    ConfigBase configBase;
    private ClusterConfiguration clusterConfiguration;
    private String id;
    private Set<EnvironmentContainerHost> mySet;
    private Set<String> myUUID;
    @Mock
    private HostInterface hostInterface;


    @Before
    public void setUp() throws Exception
    {
        id = UUID.randomUUID().toString();
        mySet = new HashSet<>();
        mySet.add( containerHost );

        myUUID = new HashSet<>();
        myUUID.add( id );
        when( stormImpl.getEnvironmentManager() ).thenReturn( environmentManager );

        clusterConfiguration = new ClusterConfiguration( trackerOperation, stormImpl );

        // mock
//        when( zookeeperClusterConfig.getEnvironmentId() ).thenReturn( id );
        when( stormClusterConfiguration.getEnvironmentId() ).thenReturn( id );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( containerHost.getInterfaceByName( "eth0" ) ).thenReturn( hostInterface );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( stormImpl.getPluginDAO() ).thenReturn( pluginDAO );
        when( hostInterface.getIp() ).thenReturn( "192.168.0.1" );
    }


    @Test
    public void testConfigureClusterNoEnvironmentException() throws Exception
    {
        when( environmentManager.loadEnvironment( any( String.class ) ) )
                .thenThrow( EnvironmentNotFoundException.class );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( id );

        clusterConfiguration.configureCluster( stormClusterConfiguration, environment );
    }


    @Test
    public void testConfigureClusterNoContainerHostException() throws Exception
    {
        when( environmentManager.loadEnvironment( any( String.class ) ) )
                .thenThrow( ContainerHostNotFoundException.class );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( stormClusterConfiguration.getNimbus() ).thenReturn( id );

        clusterConfiguration.configureCluster( stormClusterConfiguration, environment );
    }
}