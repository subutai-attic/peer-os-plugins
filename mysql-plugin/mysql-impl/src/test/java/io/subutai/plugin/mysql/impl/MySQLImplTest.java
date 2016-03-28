package io.subutai.plugin.mysql.impl;


import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.network.api.NetworkManager;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;
import io.subutai.plugin.mysql.impl.alert.MySQLAlertListener;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class MySQLImplTest
{

    //@formatter:off
    @Mock
    private Tracker tracker;
    @Mock
    private ExecutorService executor;
    @Mock
    private EnvironmentManager environmentManager;
    @Mock
    private PluginDAO pluginDAO;
    @Mock
    private Monitor monitor;
    @Mock
    private QuotaManager quotaManager;
    @Mock
    private PeerManager peerManager;
    @Mock
    private NetworkManager networkManager;
    @Mock
    private MySQLAlertListener mySQLAlertListener;
    @Mock
    private Set<ContainerHost> mySet;
    @Mock
    private CommandResult commandResult;
    @Mock
    private MySQLClusterConfig clusterConfig;
    @Mock
    private TrackerOperation trackerOperation;
    private UUID uuid;
    private MySQLCImpl impl;

    //@formatter:on


    @Before
    public void setUp()
    {
        impl = new MySQLCImpl( monitor, pluginDAO );
        uuid = UUID.randomUUID();
        impl.setEnvironmentManager( environmentManager );
        impl.setExecutor( executor );
        impl.setNetworkManager( networkManager );
        impl.setPeerManager( peerManager );
        impl.setPluginDAO( pluginDAO );
        impl.setQuotaManager( quotaManager );
        impl.setTracker( tracker );

        when( clusterConfig.getClusterName() ).thenReturn( "test" );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        when( trackerOperation.getId() ).thenReturn( uuid );
        when( impl.getCluster( clusterConfig.getClusterName() ) ).thenReturn( clusterConfig );
        when( impl.getPluginDAO()
                  .getInfo( MySQLClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName(), MySQLClusterConfig.class ) )
                .thenReturn( clusterConfig );
    }


    @Test
    public void testInstallCluster()
    {
        UUID id = impl.installCluster( clusterConfig );
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testStartCluster()
    {
        UUID id = impl.startCluster( clusterConfig.getClusterName() );
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testStopCluster()
    {
        UUID id = impl.stopCluster( clusterConfig.getClusterName() );
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testDestroyCluster() throws Exception
    {
        UUID id = impl.destroyCluster( clusterConfig.getClusterName() );
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testAddNode() throws Exception
    {
        NodeType nodeType = NodeType.MASTER_NODE;
        UUID id = impl.addNode( clusterConfig.getClusterName(), nodeType );
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testUninstallCluster() throws Exception
    {
        UUID id = impl.uninstallCluster( clusterConfig.getClusterName() );
        verify( executor ).execute( isA( AbstractOperationHandler.class ) );
        assertEquals( uuid, id );
    }


    @Test
    public void testGetCluster() throws Exception
    {
        MySQLClusterConfig config = impl.getCluster( clusterConfig.getClusterName() );
        assertEquals( config, clusterConfig );
    }
}
