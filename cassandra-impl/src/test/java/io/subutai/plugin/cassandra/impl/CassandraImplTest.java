package io.subutai.plugin.cassandra.impl;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.Cassandra;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterSetupStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class CassandraImplTest
{
    private CassandraImpl cassandraImpl;
    private UUID uuid;
    @Mock
    Cassandra cassandra;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    Connection connection;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    CassandraClusterConfig cassandraClusterConfig;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    ContainerHost containerHost;
    @Mock
    Iterator<ContainerHost> iterator;
    @Mock
    Set<ContainerHost> mySet;
    @Mock
    CommandResult commandResult;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    ResultSet resultSet;
    @Mock
    ResultSetMetaData resultSetMetaData;
    @Mock
    ExecutorService executor;
    @Mock
    Monitor monitor;


    @Before
    public void setUp() throws SQLException
    {
        //when( dataSource.getConnection() ).thenReturn( connection );
        //when( connection.prepareStatement( any( String.class ) ) ).thenReturn( preparedStatement );
        //when( preparedStatement.executeQuery() ).thenReturn( resultSet );
        //when( resultSet.getMetaData() ).thenReturn( resultSetMetaData );
        //when( resultSetMetaData.getColumnCount() ).thenReturn( 1 );

        uuid = UUID.randomUUID();
        cassandraImpl = new CassandraImpl( monitor,pluginDAO );
        //cassandraImpl.init();
        cassandraImpl.setTracker( tracker );
        cassandraImpl.getTracker();
        cassandraImpl.setEnvironmentManager( environmentManager );
        cassandraImpl.getEnvironmentManager();
        cassandraImpl.setPluginDAO( pluginDAO );
        cassandraImpl.executor = executor;

        // mock InstallClusterHandler
        when( cassandraClusterConfig.getClusterName() ).thenReturn( "test" );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );


        // asserts
        //assertEquals( connection, dataSource.getConnection() );
        //verify( connection ).prepareStatement( any( String.class ) );
        //assertEquals( preparedStatement, connection.prepareStatement( any( String.class ) ) );
    }


    @Test
    public void testGetTracker()
    {
        cassandraImpl.getTracker();

        // assertions
        assertNotNull( cassandraImpl.getTracker() );
        assertEquals( tracker, cassandraImpl.getTracker() );
    }


    @Test
    public void testSetTracker()
    {
        cassandraImpl.setTracker( tracker );

        // assertions
        assertNotNull( cassandraImpl.getTracker() );
        assertEquals( tracker, cassandraImpl.getTracker() );
    }


    @Test
    public void testGetEnvironmentManager()
    {
        cassandraImpl.getEnvironmentManager();

        assertNotNull( cassandraImpl.getEnvironmentManager() );
        assertEquals( environmentManager, cassandraImpl.getEnvironmentManager() );
    }


    @Test
    public void testSetEnvironmentManager()
    {
        cassandraImpl.setEnvironmentManager( environmentManager );

        assertNotNull( cassandraImpl.getEnvironmentManager() );
        assertEquals( environmentManager, cassandraImpl.getEnvironmentManager() );
    }


    @Test
    public void testInit()
    {
        //cassandraImpl.init();
    }


    @Test
    public void testDestroy()
    {
        cassandraImpl.destroy();
    }


    @Test
    public void testInstallCluster()
    {
        cassandraImpl.executor = executor;

        UUID id = cassandraImpl.installCluster( cassandraClusterConfig );

        // asserts
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Ignore
    @Test
    public void testUninstallCluster()
    {
        cassandraImpl.executor = executor;

        UUID id = cassandraImpl.uninstallCluster( "test" );

        // asserts
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testGetClusters()
    {
        //cassandraImpl.getClusters();

        // assertNotNull( cassandraImpl.getClusters() );
    }


    @Test
    public void testGetCluster()
    {
        //cassandraImpl.getCluster( "test" );
    }


    @Test
    public void testGetPluginDAO()
    {
        PluginDAO pl = cassandraImpl.getPluginDAO();

        assertNotNull( pl );
    }


    @Ignore
    @Test
    public void testStartCluster()
    {
        cassandraImpl.executor = executor;

        UUID id = cassandraImpl.startCluster( "test" );

        // asserts
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Ignore
    @Test
    public void testCheckCluster()
    {
        cassandraImpl.executor = executor;

        UUID id = cassandraImpl.checkCluster( "test" );

        // asserts
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Ignore
    @Test
    public void testStopCluster()
    {
        cassandraImpl.executor = executor;

        UUID id = cassandraImpl.stopCluster( "test" );

        // asserts
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testStartService()
    {
        cassandraImpl.executor = executor;

        UUID id = cassandraImpl.startService( "test", "test" );

        // asserts
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testStopService()
    {
        cassandraImpl.executor = executor;

        UUID id = cassandraImpl.stopService( "test", "test" );

        // asserts
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testStatusService()
    {
        cassandraImpl.executor = executor;

        UUID id = cassandraImpl.statusService( "test", "test" );

        // asserts
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testAddNode()
    {
        cassandraImpl.addNode( "test", "test" );
    }


    @Ignore
    @Test
    public void testDestroyNode()
    {
        cassandraImpl.destroyNode( "test", "test" );
    }


    @Test
    public void testCheckNode()
    {
        cassandraImpl.executor = executor;

        UUID id = cassandraImpl.checkNode( "test", "test" );

        // asserts
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testGetClusterSetupStrategy()
    {
        cassandraImpl.getClusterSetupStrategy( environment, cassandraClusterConfig, trackerOperation );
    }


    @Test( expected = NullPointerException.class )
    public void shouldThrowsNullPointerExceptionInGetClusterSetupStrategy()
    {
        cassandraImpl.getClusterSetupStrategy( null, null, null );
    }


    @Test
    public void testConfigureEnvironmentCluster()
    {
        cassandraImpl.executor = executor;

        UUID id = cassandraImpl.installCluster( cassandraClusterConfig );

        // asserts
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }
}