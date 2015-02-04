package org.safehaus.subutai.plugin.oozie.impl;


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
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.lxc.quota.api.QuotaManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;

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
    org.safehaus.subutai.core.metric.api.Monitor monitor;
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
                new OverHadoopSetupStrategy( oozieClusterConfig, trackerOperation, oozieImpl );
        uuid = new UUID( 50, 50 );
        mySet = new HashSet<>();
        mySet.add( uuid );

        when( oozieClusterConfig.getServer() ).thenReturn( UUID.randomUUID() );
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupMalformedConfiguration() throws Exception
    {
        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterAlreadyExist() throws Exception
    {
        when( oozieClusterConfig.getClients() ).thenReturn( mySet );
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( oozieClusterConfig );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNoHadoop() throws Exception
    {
        when( oozieClusterConfig.getClients() ).thenReturn( mySet );
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( null );
        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNoSpecifiedNodes() throws Exception
    {
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
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
        when( oozieClusterConfig.getClients() ).thenReturn( mySet );
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( null );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterNodeNotConnected() throws Exception
    {
        when( oozieClusterConfig.getClients() ).thenReturn( mySet );
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        Set<ContainerHost> myCont = new HashSet<>();
        myCont.add( containerHost );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterFailed() throws Exception
    {
        when( oozieClusterConfig.getClients() ).thenReturn( mySet );
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
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
        when( oozieClusterConfig.getClients() ).thenReturn( mySet );
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( mySet );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
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
        when( oozieClusterConfig.getClients() ).thenReturn( mySet );
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( mySet );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
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
        when( oozieClusterConfig.getClients() ).thenReturn( mySet );
        when( oozieClusterConfig.getClusterName() ).thenReturn( "test" );
        when( oozieImpl.getCluster( anyString() ) ).thenReturn( null );
        when( oozieImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        List<UUID> myList = new ArrayList<>();
        myList.add( uuid );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( oozieClusterConfig.getAllNodes() ).thenReturn( mySet );
        when( oozieImpl.getEnvironmentManager() ).thenReturn( environmentManager );
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

        overHadoopSetupStrategy.setup();
    }


    @Test
    public void testGetFailedCommandResults() throws Exception
    {

    }
}