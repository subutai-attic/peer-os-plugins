package org.safehaus.subutai.plugin.mysql.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.mdc.SubutaiExecutors;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentEventListener;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.lxc.quota.api.QuotaManager;
import org.safehaus.subutai.core.metric.api.Monitor;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.network.api.NetworkManager;
import org.safehaus.subutai.core.peer.api.PeerManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.mysql.impl.alert.MySQLAlertListener;
import org.safehaus.subutai.plugin.mysql.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.mysql.impl.handler.NodeOperationHandler;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLC;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLClusterConfig;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * Created by tkila on 5/7/15.
 */
public class MySQLCImpl implements MySQLC, EnvironmentEventListener
{

    private static final Logger LOG = Logger.getLogger( MySQLCImpl.class.getName() );

    //@formatter:off
    private Tracker            tracker;
    private ExecutorService    executor;
    private EnvironmentManager environmentManager;
    private PluginDAO          pluginDAO;
    private Monitor            monitor;
    private QuotaManager       quotaManager;
    private PeerManager        peerManager;
    private NetworkManager     networkManager;
    private MySQLAlertListener mySQLAlertListener;
    //@formatter:on

    private Gson gson = new GsonBuilder().serializeNulls().excludeFieldsWithoutExposeAnnotation().create();

    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );



    public MySQLCImpl( Monitor monitor )
    {
        this.monitor = monitor;
        this.mySQLAlertListener = new MySQLAlertListener( this );
        this.monitor.addAlertListener( mySQLAlertListener );
    }


    @Override
    public UUID startCluster( String clusterName )
    {
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.START_ALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopCluster( final String clusterName )
    {
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.STOP_ALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroyCluster( final String clusterName )
    {
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.DESTROY, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID addNode( String clusterName, NodeType nodeType )
    {
        MySQLClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler handler =
                new ClusterOperationHandler( this, config, ClusterOperationType.ADD, nodeType );
        executor.execute( handler );
        return handler.getTrackerId();
    }


    public void init()
    {
        try
        {
            this.pluginDAO = new PluginDAO( null );
        }
        catch ( SQLException e )
        {
            LOG.throwing( MySQLCImpl.class.getName(), "init()", e );
        }

        executor = SubutaiExecutors.newSingleThreadExecutor();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    @Override
    public UUID installCluster( final MySQLClusterConfig config )
    {
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( String clusterName )
    {
        MySQLClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.UNINSTALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public List<MySQLClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( MySQLClusterConfig.PRODUCT_KEY, MySQLClusterConfig.class );
    }


    @Override
    public MySQLClusterConfig getCluster( String clusterName )
    {
        return pluginDAO.getInfo( MySQLClusterConfig.PRODUCT_KEY, clusterName, MySQLClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String agentHostName )
    {
        return null;
    }


    @Override
    public void onEnvironmentCreated( Environment environment )
    {
        LOG.info( "Environment created" );
    }


    @Override
    public void onEnvironmentGrown( Environment environment, Set<ContainerHost> set )
    {
        LOG.info( "Environment grown" );
    }


    @Override
    public void onContainerDestroyed( Environment environment, UUID uuid )
    {

    }


    @Override
    public void onEnvironmentDestroyed( UUID uuid )
    {
        List<String> clusterDataList = pluginDAO.getInfo( MySQLClusterConfig.PRODUCT_KEY );
        for ( final String clusterData : clusterDataList )
        {
            MySQLClusterConfig clusterConfig = gson.fromJson( clusterData, MySQLClusterConfig.class );
            if ( clusterConfig.getEnvironmentId().equals( uuid ) )
            {
                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "MySQL cluster: %s destroyed", clusterConfig.getClusterName() ) );
                }
                catch ( ClusterException e )
                {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final Environment environment, final MySQLClusterConfig config,
                                                         final TrackerOperation po )
    {
        return null;
    }


    @Override
    public void saveConfig( final MySQLClusterConfig config ) throws ClusterException
    {
        if ( !getPluginDAO().saveInfo( MySQLClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final MySQLClusterConfig config ) throws ClusterException
    {
        getPluginDAO().deleteInfo( MySQLClusterConfig.PRODUCT_KEY, config.getClusterName() );
    }


    @Override
    public UUID stopService( final String clusterName, final ContainerHost containerHost, NodeType type )
    {
        return stopNode( clusterName, containerHost.getHostname(), type );
    }


    @Override
    public UUID startService( final String clusterName, final ContainerHost containerHost, NodeType type )
    {
        return startNode( clusterName, containerHost.getHostname(), type );
    }


    @Override
    public UUID statusService( final String clusterName, final ContainerHost containerHost, NodeType type )
    {
        return checkNode( clusterName, containerHost.getHostname(), type );
    }


    @Override
    public UUID destroyService( final String clusterName, final ContainerHost containerHost, final NodeType nodeType )
    {
        return destroyNode( clusterName, containerHost.getHostname(), nodeType );
    }


    @Override
    public UUID startNode( final String clusterName, final String containerHost, final NodeType nodeType )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, containerHost, NodeOperationType.START, nodeType );

        LOG.info( String.format( "Invoked: %s", operationHandler.getClass().toString() ) );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID installSQLServer( final String clusterName, final ContainerHost containerHost, final NodeType nodeType )
    {

        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, containerHost.getHostname(), NodeOperationType.INSTALL,
                        NodeType.SERVER );

        LOG.info( String.format( "Invoked: %s", operationHandler.getClass().toString() ) );


        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopNode( final String clusterName, final String containerHost, final NodeType nodeType )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, containerHost, NodeOperationType.STOP, nodeType );

        LOG.info( String.format( "Invoked: %s", operationHandler.getClass().toString() ) );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkNode( final String clusterName, final String containerHost, final NodeType nodeType )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, containerHost, NodeOperationType.STATUS, nodeType );

        LOG.info( String.format( "Invoked: %s", operationHandler.getClass().toString() ) );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroyNode( final String clusterName, final String containerHost, final NodeType nodeType )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, containerHost, NodeOperationType.DESTROY, nodeType );

        LOG.info( String.format( "Invoked: %s", operationHandler.getClass().toString() ) );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setExecutor( final ExecutorService executor )
    {
        this.executor = executor;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public void setPluginDAO( final PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public void setMonitor( final Monitor monitor )
    {
        this.monitor = monitor;
    }


    public void setQuotaManager( final QuotaManager quotaManager )
    {
        this.quotaManager = quotaManager;
    }


    public void setPeerManager( final PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }


    public void setNetworkManager( final NetworkManager networkManager )
    {
        this.networkManager = networkManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public QuotaManager getQuotaManager()
    {
        return quotaManager;
    }


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    public NetworkManager getNetworkManager()
    {
        return networkManager;
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    public void unsubscribeFromAlerts( final Environment env )
    {
        try
        {
            getMonitor().stopMonitoring( mySQLAlertListener, env );
        }
        catch ( MonitorException e )
        {
            e.printStackTrace();
        }
    }
}
