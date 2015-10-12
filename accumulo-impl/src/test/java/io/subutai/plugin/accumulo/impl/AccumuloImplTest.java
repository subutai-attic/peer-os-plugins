package io.subutai.plugin.accumulo.impl;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.collections.Sets;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.NodeType;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@Ignore
@RunWith( MockitoJUnitRunner.class )
public class AccumuloImplTest
{
    @Mock
    AccumuloClusterConfig accumuloClusterConfig;
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
    PluginDAO pluginDAO;
    @Mock
    ExecutorService executor;
    @Mock
    Commands commands;
    @Mock
    Hadoop hadoop;
    @Mock
    Zookeeper zookeeper;
    @Mock
    Connection connection;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    ResultSet resultSet;
    @Mock
    ResultSetMetaData resultSetMetaData;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    ZookeeperClusterConfig zookeeperClusterConfig;
    @Mock
    Monitor monitor;
    @Mock
    QuotaManager quotaManager;
    private AccumuloImpl accumuloImpl;
    private String id;


    @Before
    public void setUp() throws Exception
    {
        // mock init
        when( connection.prepareStatement( any( String.class ) ) ).thenReturn( preparedStatement );
        when( preparedStatement.executeQuery() ).thenReturn( resultSet );
        when( resultSet.getMetaData() ).thenReturn( resultSetMetaData );
        when( resultSetMetaData.getColumnCount() ).thenReturn( 1 );

        id = UUID.randomUUID().toString();
        accumuloImpl = new AccumuloImpl( monitor, pluginDAO );
        //        accumuloImpl.init();
        accumuloImpl.setExecutor( executor );
        accumuloImpl.setEnvironmentManager( environmentManager );
        accumuloImpl.setHadoopManager( hadoop );
        accumuloImpl.setTracker( tracker );
        accumuloImpl.setZkManager( zookeeper );
        accumuloImpl.setPluginDAO( pluginDAO );
        accumuloImpl.setMonitor( monitor );
        accumuloImpl.setQuotaManager( quotaManager );

        // mock InstallClusterHandler
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( UUID.randomUUID() );

        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( zookeeper.getCluster( anyString() ) ).thenReturn( zookeeperClusterConfig );
        when( pluginDAO.getInfo( AccumuloClusterConfig.PRODUCT_KEY, "test", AccumuloClusterConfig.class ) )
                .thenReturn( accumuloClusterConfig );
        when( pluginDAO.getInfo( AccumuloClusterConfig.PRODUCT_KEY, AccumuloClusterConfig.class ) )
                .thenReturn( Arrays.asList( accumuloClusterConfig ) );
        when( accumuloClusterConfig.getEnvironmentId() ).thenReturn( id );
        when( accumuloClusterConfig.getAllNodes() ).thenReturn( Sets.newSet( id ) );
        when( accumuloClusterConfig.getClusterName() ).thenReturn( "clusterName" );
        when( environment.getId() ).thenReturn( id );

        // asserts
        //assertEquals( preparedStatement, connection.prepareStatement( any( String.class ) ) );
        //assertEquals( resultSet, preparedStatement.executeQuery() );
        //assertEquals( resultSetMetaData, resultSet.getMetaData() );
        //assertNotNull( resultSetMetaData.getColumnCount() );
    }


    @Test
    public void testGetPluginDAO() throws Exception
    {
        accumuloImpl.getPluginDAO();

        // assertions
        assertNotNull( accumuloImpl.getPluginDAO() );
    }


    @Test
    public void testGetExecutor() throws Exception
    {
        accumuloImpl.getExecutor();

        // assertions
        assertNotNull( accumuloImpl.getExecutor() );
        assertEquals( executor, accumuloImpl.getExecutor() );
    }


    @Test
    public void testGetEnvironmentManager() throws Exception
    {
        accumuloImpl.getEnvironmentManager();

        // assertions
        assertNotNull( accumuloImpl.getEnvironmentManager() );
        assertEquals( environmentManager, accumuloImpl.getEnvironmentManager() );
    }


    @Test
    public void testGetCommands() throws Exception
    {
        accumuloImpl.getCommands();
    }


    @Test
    public void testGetTracker() throws Exception
    {
        accumuloImpl.getTracker();

        // assertions
        assertNotNull( accumuloImpl.getTracker() );
        assertEquals( tracker, accumuloImpl.getTracker() );
    }


    @Test
    public void testGetHadoopManager() throws Exception
    {
        accumuloImpl.getHadoopManager();

        // assertions
        assertNotNull( accumuloImpl.getHadoopManager() );
        assertEquals( hadoop, accumuloImpl.getHadoopManager() );
    }


    @Test( expected = NullPointerException.class )
    public void testInit() throws Exception
    {
        accumuloImpl.init();
    }


    @Test
    public void testDestroy() throws Exception
    {
        accumuloImpl.destroy();
    }


    @Test
    public void testInstallCluster() throws Exception
    {
        accumuloImpl.installCluster( accumuloClusterConfig );

        // assertions
        assertNotNull( accumuloImpl.installCluster( accumuloClusterConfig ) );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        accumuloImpl.uninstallCluster( "test" );

        // assertions
        assertNotNull( accumuloImpl.uninstallCluster( "test" ) );
    }


    @Test
    public void testGetClusters() throws Exception
    {
        List<AccumuloClusterConfig> myList = new ArrayList<>();
        myList.add( accumuloClusterConfig );
        when( pluginDAO.getInfo( AccumuloClusterConfig.PRODUCT_KEY, AccumuloClusterConfig.class ) )
                .thenReturn( myList );


        accumuloImpl.getClusters();

        // assertions
        assertNotNull( accumuloImpl.getClusters() );
        assertEquals( myList, accumuloImpl.getClusters() );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        accumuloImpl.getCluster( "test" );

        // assertions
        assertNotNull( accumuloImpl.getCluster( "test" ) );
        assertEquals( accumuloClusterConfig, accumuloImpl.getCluster( "test" ) );
    }


    @Test
    public void testAddNode() throws Exception
    {
        accumuloImpl.addNode( "test", "test" );
    }


    @Test
    public void testStartCluster() throws Exception
    {
        accumuloImpl.startCluster( "test" );

        // assertions
        assertNotNull( accumuloImpl.startCluster( "test" ) );
    }


    @Test
    public void testStopCluster() throws Exception
    {
        accumuloImpl.stopCluster( "test" );

        // assertions
        assertNotNull( accumuloImpl.stopCluster( "test" ) );
    }


    @Test
    public void testCheckNode() throws Exception
    {
        accumuloImpl.checkNode( "test", "test" );

        // assertions
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
    }


    @Test
    public void testAddNode1() throws Exception
    {
        accumuloImpl.addNode( "test", "test", NodeType.MASTER_NODE );

        // assertions
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
    }


    @Test
    public void testAddNode2() throws Exception
    {
        accumuloImpl.addNode( "test", NodeType.MASTER_NODE );

        // assertions
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
    }


    @Test
    public void testDestroyNode() throws Exception
    {
        accumuloImpl.destroyNode( "test", "test", NodeType.MASTER_NODE );

        // assertions
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
    }


    @Test
    public void testAddProperty() throws Exception
    {
        accumuloImpl.addProperty( "test", "test", "test" );

        // assertions
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
    }


    @Test
    public void testRemoveProperty() throws Exception
    {
        accumuloImpl.removeProperty( "test", "test" );

        // assertions
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
    }


    @Test
    public void testOnEnvironmentCreated() throws Exception
    {
        when( environment.getName() ).thenReturn( "Environment name" );
        accumuloImpl.onEnvironmentCreated( environment );
        verify( environment ).getName();
    }


    @Test
    public void testOnEnvironmentGrown() throws Exception
    {
        EnvironmentContainerHost node = mock( EnvironmentContainerHost.class );
        when( node.getHostname() ).thenReturn( "hostname" );
        when( environment.getId() ).thenReturn( id );
        accumuloImpl.onEnvironmentGrown( environment, Sets.newSet( node ) );
        verify( node ).getHostname();
        verify( environment ).getId();
    }


    @Test
    public void testOnContainerDestroyed() throws Exception
    {
        when( environment.getId() ).thenReturn( id );

        when( accumuloClusterConfig.removeNode( any( String.class ) ) ).thenReturn( false );
        accumuloImpl.onContainerDestroyed( environment, id );
        verify( pluginDAO ).deleteInfo( anyString(), anyString() );

        when( accumuloClusterConfig.removeNode( any( String.class ) ) ).thenReturn( true );
        accumuloImpl.onContainerDestroyed( environment, id );
        verify( pluginDAO ).saveInfo( anyString(), anyString(), anyObject() );
    }


    @Test
    public void testOnEnvironmentDestroyed() throws Exception
    {
        when( environment.getId() ).thenReturn( id );
        accumuloImpl.onEnvironmentDestroyed( id );
        verify( pluginDAO ).deleteInfo( AccumuloClusterConfig.PRODUCT_KEY, "clusterName" );
    }


    @Test
    public void testSetPluginDAO() throws Exception
    {
        accumuloImpl.setPluginDAO( pluginDAO );
    }


    @Test
    public void testSetExecutor() throws Exception
    {
        accumuloImpl.setExecutor( Executors.newCachedThreadPool() );
    }


    @Test
    public void testSetEnvironmentManager() throws Exception
    {
        accumuloImpl.setEnvironmentManager( environmentManager );
    }


    @Test
    public void testSetTracker() throws Exception
    {
        accumuloImpl.setTracker( tracker );
    }


    @Test
    public void testSetHadoopManager() throws Exception
    {
        accumuloImpl.setHadoopManager( hadoop );
    }


    @Test
    public void testGetZkManager() throws Exception
    {
        assertEquals( zookeeper, accumuloImpl.getZkManager() );
    }


    @Test
    public void testSetZkManager() throws Exception
    {
        accumuloImpl.setZkManager( zookeeper );
    }


    @Test
    public void testSubscribeToAlerts() throws Exception
    {
        accumuloImpl.subscribeToAlerts( environment );
    }


    @Test
    public void testSubscribeToAlerts1() throws Exception
    {
        accumuloImpl.subscribeToAlerts( containerHost );
    }


    @Test
    public void testUnsubscribeFromAlerts() throws Exception
    {
        accumuloImpl.unsubscribeFromAlerts( environment );
    }


    @Test
    public void testGetMonitor() throws Exception
    {
        assertEquals( monitor, accumuloImpl.getMonitor() );
    }


    @Test
    public void testSetMonitor() throws Exception
    {
        Monitor monitor1 = mock( Monitor.class );
        accumuloImpl.setMonitor( monitor1 );
        assertNotEquals( monitor, accumuloImpl.getMonitor() );
    }


    @Test
    public void testGetAlertSettings() throws Exception
    {
        assertNotNull( accumuloImpl.getAlertSettings() );
    }


    @Test
    public void testGetQuotaManager() throws Exception
    {
        assertNotNull( accumuloImpl.getQuotaManager() );
    }


    @Test
    public void testSetQuotaManager() throws Exception
    {
        QuotaManager quotaManager1 = mock( QuotaManager.class );
        accumuloImpl.setQuotaManager( quotaManager1 );
        assertNotEquals( quotaManager, accumuloImpl.getQuotaManager() );
    }
}