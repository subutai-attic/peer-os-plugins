package org.safehaus.subutai.plugin.oozie.impl;


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
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;

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
    ContainerHost containerHost;
    @Mock
    CommandResult commandResult;

    @Before
    public void setUp() throws Exception
    {
        clusterConfiguration = new ClusterConfiguration( oozieImpl, trackerOperation );
    }


    @Test
    public void testConfigureCluster() throws Exception
    {
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myUUID = new ArrayList<>();
        myUUID.add( UUID.randomUUID() );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn(myUUID);
        Set<ContainerHost> mySet = new HashSet<>(  );
        mySet.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn(mySet);
        when( environment.getContainerHostById( any(UUID.class) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        clusterConfiguration.configureCluster( oozieClusterConfig, environment );

        // assertions
        verify( trackerOperation ).addLog( "Cluster configured\n" );
    }


    @Test(expected = ClusterConfigurationException.class)
    public void testConfigureClusterHasNotSucceeded() throws Exception
    {
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myUUID = new ArrayList<>();
        myUUID.add( UUID.randomUUID() );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn(myUUID);
        Set<ContainerHost> mySet = new HashSet<>(  );
        mySet.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn(mySet);
        when( environment.getContainerHostById( any(UUID.class) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( false );

        clusterConfiguration.configureCluster( oozieClusterConfig, environment );
    }


}