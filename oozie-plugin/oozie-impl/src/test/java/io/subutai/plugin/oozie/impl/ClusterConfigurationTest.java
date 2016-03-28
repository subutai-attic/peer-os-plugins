package io.subutai.plugin.oozie.impl;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import io.subutai.common.host.HostInterface;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ClusterConfigurationTest
{
    private ClusterConfiguration clusterConfiguration;
    @Mock
    OozieImpl oozieImpl;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    OozieClusterConfig oozieClusterConfig;
    @Mock
    Environment environment;
    @Mock
    Hadoop hadoop;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    CommandResult commandResult;
    @Mock
    HostInterface hostInterface;


    @Before
    public void setUp() throws Exception
    {
        clusterConfiguration = new ClusterConfiguration( oozieImpl, trackerOperation );
    }


    @Test
    public void testConfigureCluster() throws Exception
    {
        when( hostInterface.getIp() ).thenReturn( "192.168.0.1" );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<String> myUUID = new ArrayList<>();
        myUUID.add( UUID.randomUUID().toString() );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myUUID );
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( mySet );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( containerHost.getInterfaceByName( "eth0" ) ).thenReturn( hostInterface );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        clusterConfiguration.configureCluster( oozieClusterConfig, environment );

        // assertions
        verify( trackerOperation ).addLog( "Cluster configured\n" );
    }


    @Test//( expected = ClusterConfigurationException.class )
    public void testConfigureClusterHasNotSucceeded() throws Exception
    {
        when( hostInterface.getIp() ).thenReturn( "192.168.0.1" );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<String> myUUID = new ArrayList<>();
        myUUID.add( UUID.randomUUID().toString() );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myUUID );
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( mySet );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( false );
        when( containerHost.getInterfaceByName( "eth0" ) ).thenReturn( hostInterface );
        clusterConfiguration.configureCluster( oozieClusterConfig, environment );
    }
}