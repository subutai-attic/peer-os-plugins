package org.safehaus.subutai.plugin.mahout.impl;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.lxc.quota.api.QuotaManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;

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
    @Mock
    QuotaManager quotaManager;


    @Before
    public void setUp() throws Exception
    {
        overHadoopSetupStrategy =
                new OverHadoopSetupStrategy( mahoutImpl, mahoutClusterConfig, trackerOperation, environment );

        uuid = new UUID( 50, 50 );
        mySet = new HashSet<>();
        mySet.add( uuid );
        when( mahoutClusterConfig.getHadoopClusterName() ).thenReturn( "testHadoop" );

        when( mahoutImpl.getCommands() ).thenReturn( commands );


    }

    @Test( expected = ClusterSetupException.class )
    public void testSetupMalformedConfiguration() throws Exception
    {
        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterAlreadyExist() throws Exception
    {
        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( mahoutClusterConfig);

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNoHadoop() throws Exception
    {
        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( null );
        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNoSpecifiedNodes() throws Exception
    {
        when( mahoutClusterConfig.getHadoopClusterName() ).thenReturn( "testHadoop" );
        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( UUID.randomUUID() );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterEnvironmentNotFound() throws Exception
    {
        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.getEnvironmentByUUID( any( UUID.class ) ) ).thenReturn( null );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNodeNotConnected() throws Exception
    {
        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.getEnvironmentByUUID( any( UUID.class ) ) ).thenReturn( environment );
        Set<ContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterMahoutInstalled() throws Exception
    {
        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.getEnvironmentByUUID( any( UUID.class ) ) ).thenReturn( environment );
        Set<ContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.getStdOut() ).thenReturn( Commands.PACKAGE_NAME );
        when( mahoutImpl.getPluginDAO() ).thenReturn( pluginDAO );

        overHadoopSetupStrategy.setup();
    }


    @Test
    public void testSetupClusterMahoutInstalled2() throws Exception
    {
        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.getEnvironmentByUUID( any( UUID.class ) ) ).thenReturn( environment );
        Set<ContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.getStdOut() ).thenReturn( Commands.PACKAGE_NAME );
        when( mahoutImpl.getPluginDAO() ).thenReturn( pluginDAO );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNoHadoopInstallation() throws Exception
    {
        when( mahoutClusterConfig.getNodes() ).thenReturn( mySet );
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );
        when( mahoutImpl.getCluster( anyString() ) ).thenReturn( null );
        when( mahoutImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( mahoutImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.getEnvironmentByUUID( any( UUID.class ) ) ).thenReturn( environment );
        Set<ContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );
        when( containerHost.isConnected() ).thenReturn( true );
        when( environment.getContainerHostById( any( UUID.class ) ) ).thenReturn( containerHost );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( commandResult.getStdOut() ).thenReturn( "test");
        when( mahoutImpl.getPluginDAO() ).thenReturn( pluginDAO );

        overHadoopSetupStrategy.setup();
    }
}