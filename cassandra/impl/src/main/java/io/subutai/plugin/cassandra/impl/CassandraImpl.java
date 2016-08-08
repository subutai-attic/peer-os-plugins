package io.subutai.plugin.cassandra.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.strategy.api.StrategyManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.Cassandra;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.cassandra.impl.handler.NodeOperationHandler;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;


public class CassandraImpl implements Cassandra, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( CassandraImpl.class.getName() );
    private Tracker tracker;
    protected ExecutorService executor;
    private EnvironmentManager environmentManager;
    private PluginDAO pluginDAO;
    private PeerManager peerManager;
    private QuotaManager quotaManager;
    private StrategyManager strategyManager;
    private Monitor monitor;
    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );


    public CassandraImpl( Monitor monitor, PluginDAO pluginDAO )
    {
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }

    //
    //    public void subscribeToAlerts( Environment environment ) throws MonitorException
    //    {
    //        getMonitor().startMonitoring( CassandraAlertListener.CASSANDRA_ALERT_LISTENER, environment,
    // alertSettings );
    //    }
    //
    //
    //    public void subscribeToAlerts( EnvironmentContainerHost host ) throws MonitorException
    //    {
    //        getMonitor().activateMonitoring( host, alertSettings );
    //    }
    //
    //
    //    public void unsubscribeFromAlerts( final Environment environment ) throws MonitorException
    //    {
    //        getMonitor().stopMonitoring( CassandraAlertListener.CASSANDRA_ALERT_LISTENER, environment );
    //    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    public void setPeerManager( final PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }


    public void init()
    {
        executor = SubutaiExecutors.newSingleThreadExecutor();
    }


    public void destroy()
    {
        this.tracker = null;
        this.environmentManager = null;
        this.pluginDAO = null;
        this.executor.shutdown();
        this.executor = null;
    }


    public UUID installCluster( final CassandraClusterConfig config )
    {
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID uninstallCluster( final String clusterName )
    {
        CassandraClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.REMOVE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID removeCluster( final String clusterName )
    {
        CassandraClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.REMOVE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public List<CassandraClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( CassandraClusterConfig.PRODUCT_KEY, CassandraClusterConfig.class );
    }


    @Override
    public CassandraClusterConfig getCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        return pluginDAO.getInfo( CassandraClusterConfig.PRODUCT_KEY, clusterName, CassandraClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String agentHostName )
    {
        return null;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public void setPluginDAO( PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    @Override
    public UUID startCluster( final String clusterName )
    {
        CassandraClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.START_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkCluster( final String clusterName )
    {
        CassandraClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.STATUS_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopCluster( final String clusterName )
    {
        CassandraClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.STOP_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startService( final String clusterName, final String hostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.START );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopService( final String clusterName, final String hostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.STOP );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID statusService( final String clusterName, final String hostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.STATUS );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID addNode( final String clusterName )
    {
        CassandraClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.ADD );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroyNode( final String clusterName, String hostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.DESTROY );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkNode( final String clusterName, final String hostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.STATUS );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final Environment environment,
                                                         final CassandraClusterConfig config,
                                                         final TrackerOperation po )
    {
        return new CassandraSetupStrategy( environment, config, po, this );
    }


    @Override
    public void saveConfig( final CassandraClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( CassandraClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final CassandraClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().deleteInfo( CassandraClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        //not needed
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {
        //not needed
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String containerId )
    {
        // TODO need to reconfigure after container destroyed
        LOG.info( String.format( "Cassandra environment event: Container destroyed: %s", containerId ) );
        List<CassandraClusterConfig> clusters = getClusters();
        for ( CassandraClusterConfig clusterConfig : clusters )
        {
            if ( environment.getId().equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Cassandra environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                LOG.info( String.format( "Cassandra environment event: Before: %s", clusterConfig ) );

                if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getSeedNodes() ) )
                {
                    clusterConfig.getSeedNodes().remove( containerId );
                }
                try
                {
                    saveConfig( clusterConfig );
                    LOG.info( String.format( "Cassandra environment event: After: %s", clusterConfig ) );
                }
                catch ( ClusterException e )
                {
                    LOG.error( "Error updating cluster config", e );
                }
                break;
            }
        }
    }


    @Override
    public void onEnvironmentDestroyed( final String envId )
    {
        LOG.info( String.format( "Cassandra environment event: Environment destroyed: %s", envId ) );

        List<CassandraClusterConfig> clusters = getClusters();
        for ( CassandraClusterConfig clusterConfig : clusters )
        {
            if ( envId.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Cassandra environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Cassandra environment event: Cluster %s removed",
                            clusterConfig.getClusterName() ) );
                }
                catch ( ClusterException e )
                {
                    LOG.error( "Error deleting cluster config", e );
                }
                break;
            }
        }
    }


    public StrategyManager getStrategyManager()
    {
        return strategyManager;
    }


    public void setStrategyManager( final StrategyManager strategyManager )
    {
        this.strategyManager = strategyManager;
    }


    public QuotaManager getQuotaManager()
    {
        return quotaManager;
    }


    public void setQuotaManager( final QuotaManager quotaManager )
    {
        this.quotaManager = quotaManager;
    }
}
