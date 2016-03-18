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
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class OverHadoopSetupStrategyTest
{
    private OverHadoopSetupStrategy overHadoopSetupStrategy;
    private String id;
    private Set<String> mySet;
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
    EnvironmentContainerHost containerHost;
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
        overHadoopSetupStrategy = new OverHadoopSetupStrategy( oozieClusterConfig, trackerOperation, oozieImpl );
        id = UUID.randomUUID().toString();
        mySet = new HashSet<>();
        mySet.add( id );

        when( oozieClusterConfig.getServer() ).thenReturn( UUID.randomUUID().toString() );
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
        List<String> myList = new ArrayList<>();
        myList.add( UUID.randomUUID().toString() );
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
        List<String> myList = new ArrayList<>();
        myList.add( id );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( null );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNodeNotConnected() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<String> myList = new ArrayList<>();
        myList.add( id );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        Set<EnvironmentContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterFailed() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<String> myList = new ArrayList<>();
        myList.add( id );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        Set<EnvironmentContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );


        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterClientAlreadyInstalled() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<String> myList = new ArrayList<>();
        myList.add( id );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( mySet );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        Set<EnvironmentContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
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
        List<String> myList = new ArrayList<>();
        myList.add( id );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( mySet );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        Set<EnvironmentContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
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
        List<String> myList = new ArrayList<>();
        myList.add( id );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( mySet );
        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
        Set<EnvironmentContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.hasSucceeded() ).thenReturn( true );
        when( commandResult.getStdOut() ).thenReturn( Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME );
        when( oozieImpl.getPluginDao() ).thenReturn( pluginDAO );
    }
}