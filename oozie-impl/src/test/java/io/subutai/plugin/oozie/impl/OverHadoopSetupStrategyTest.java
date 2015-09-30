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
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.Commands;
import io.subutai.plugin.oozie.impl.OozieImpl;
import io.subutai.plugin.oozie.impl.OverHadoopSetupStrategy;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class OverHadoopSetupStrategyTest
{
    private OverHadoopSetupStrategy overHadoopSetupStrategy;
    private UUID uuid;
    private Set<UUID> mySet;
    @Mock
    OozieImpl oozieImpl;
    @Mock
    OozieClusterConfig oozieClusterConfig;
    @Mock
    Commands commands;
    @Mock
    Tracker tracker;
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
    PluginDAO pluginDAO;
    @Mock
    Hadoop hadoop;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    RequestBuilder requestBuilder;
    @Mock
    io.subutai.core.metric.api.Monitor monitor;


    @Before
    public void setUp() throws Exception
    {
        overHadoopSetupStrategy =
                new OverHadoopSetupStrategy( oozieClusterConfig, trackerOperation, oozieImpl );
        uuid = new UUID( 50, 50 );
        mySet = new HashSet<>();
        mySet.add( uuid );

        when( oozieClusterConfig.getServer() ).thenReturn( UUID.randomUUID() );
        when( oozieClusterConfig.getClients() ).thenReturn( mySet );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupMalformedConfiguration() throws Exception
    {
        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterAlreadyExist() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( oozieClusterConfig );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNoHadoop() throws Exception
    {
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( null );
        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNoSpecifiedNodes() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( UUID.randomUUID() );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( mySet );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterEnvironmentNotFound() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( null );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNodeNotConnected() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        Set<ContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterFailed() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        Set<ContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );


        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterClientAlreadyInstalled() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( mySet );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        Set<ContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( commandResult.getStdOut() ).thenReturn( Common.PACKAGE_PREFIX + OozieClusterConfig.PRODUCT_NAME_CLIENT );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterServerAlreadyInstalled() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( mySet );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        Set<ContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( commandResult.getStdOut() ).thenReturn( "test" );


        overHadoopSetupStrategy.setup();
    }


    @Test
    public void testSetupCluster() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( mySet );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        Set<ContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( commandResult.getStdOut() ).thenReturn( Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME );
        when( oozieImpl.getPluginDao() ).thenReturn( pluginDAO );

        //overHadoopSetupStrategy.setup();

        // assertions
        //assertNotNull(overHadoopSetupStrategy.setup());
    }
}