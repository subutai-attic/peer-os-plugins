package io.subutai.plugin.mahout.impl;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
//import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;

import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
@Ignore
public class OverHadoopSetupStrategyTest
{
    private OverHadoopSetupStrategy overHadoopSetupStrategy;
    private String id;
    private Set<String> mySet;

    @Mock
    MahoutImpl mahoutImpl;
    @Mock
    MahoutClusterConfig mahoutClusterConfig;
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
    ExecutorService executorService;
    @Mock
    DataSource dataSource;
    @Mock
    Connection connection;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    ResultSet resultSet;
    @Mock
    ResultSetMetaData resultSetMetaData;
//    @Mock
//    QuotaManager quotaManager;


    @Before
    public void setUp() throws Exception
    {
        overHadoopSetupStrategy = new OverHadoopSetupStrategy( mahoutImpl, mahoutClusterConfig, trackerOperation );

        id = UUID.randomUUID().toString();
        mySet = new HashSet<>();
        mySet.add( id );
        when( mahoutClusterConfig.getHadoopClusterName() ).thenReturn( "testHadoop" );

        when( mahoutImpl.getCommands() ).thenReturn( commands );
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupMalformedConfiguration() throws Exception
    {
//        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterAlreadyExist() throws Exception
    {
//        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
//        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
//        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( mahoutClusterConfig );
//
//        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNoHadoop() throws Exception
    {
//        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
//        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
//        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
//        when( hadoop.getCluster( anyString() ) ).thenReturn( null );
//        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNoSpecifiedNodes() throws Exception
    {
//        when( mahoutClusterConfig.getHadoopClusterName() ).thenReturn( "testHadoop" );
//        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
//        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
//        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
//        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
//        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
//        List<String> myList = new ArrayList<>();
//        myList.add( UUID.randomUUID().toString() );
//        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
//
//        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterEnvironmentNotFound() throws Exception
    {
//        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
//        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
//        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
//        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
//        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
//        List<String> myList = new ArrayList<>();
//        myList.add( id );
//        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
//        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
//        doThrow( new EnvironmentNotFoundException() ).when( environmentManager ).loadEnvironment( any( String.class ) );
//
//        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNodeNotConnected() throws Exception
    {
//        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
//        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
//        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
//        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
//        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
//        List<String> myList = new ArrayList<>();
//        myList.add( id );
//        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
//        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
//        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
//        Set<EnvironmentContainerHost> myCont = new HashSet<>();
//        myCont.add( containerHost );
//        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
//
//        overHadoopSetupStrategy.setup();
    }


    @Test//( expected = ClusterSetupException.class )
    public void testSetupClusterMahoutInstalled() throws Exception
    {
//        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
//        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
//        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
//        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
//        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
//        List<String> myList = new ArrayList<>();
//        myList.add( id );
//        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
//        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
//        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
//        Set<EnvironmentContainerHost> myCont = new HashSet<>();
//        myCont.add( containerHost );
//        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
//        when( containerHost.isConnected() ).thenReturn( true );
//        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
//        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
//        when( commandResult.getStdOut() ).thenReturn( MahoutClusterConfig.PRODUCT_PACKAGE );
//        when( mahoutImpl.getPluginDAO() ).thenReturn( pluginDAO );
//
//        overHadoopSetupStrategy.setup();
    }


    @Test
    public void testSetupClusterMahoutInstalled2() throws Exception
    {
//        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
//        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
//        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
//        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
//        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
//        List<String> myList = new ArrayList<>();
//        myList.add( id );
//        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
//        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
//        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
//        Set<EnvironmentContainerHost> myCont = new HashSet<>();
//        myCont.add( containerHost );
//        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
//        when( containerHost.isConnected() ).thenReturn( true );
//        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
//        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
//        when( commandResult.getStdOut() ).thenReturn( MahoutClusterConfig.PRODUCT_PACKAGE );
//        when( mahoutImpl.getPluginDAO() ).thenReturn( pluginDAO );
//        when( commandResult.hasSucceeded() ).thenReturn( true );
//
//        overHadoopSetupStrategy.setup();
    }


//    @Test( expected = ClusterSetupException.class )
//    public void testSetupClusterNoHadoopInstallation() throws Exception
//    {
//        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
//        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
//        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
//        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
//        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
//        List<String> myList = new ArrayList<>();
//        myList.add( id );
//        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
//        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
//        when( environmentManager.loadEnvironment( any( String.class ) ) ).thenReturn( environment );
//        Set<EnvironmentContainerHost> myCont = new HashSet<>();
//        myCont.add( containerHost );
//        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( myCont );
//        when( containerHost.isConnected() ).thenReturn( true );
//        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
//        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
//        when( commandResult.getStdOut() ).thenReturn( "test" );
//        when( mahoutImpl.getPluginDAO() ).thenReturn( pluginDAO );
//
//        overHadoopSetupStrategy.setup();
//    }
}