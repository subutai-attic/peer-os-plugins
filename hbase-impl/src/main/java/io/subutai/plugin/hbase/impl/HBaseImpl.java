package io.subutai.plugin.hbase.impl;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.env.api.EnvironmentEventListener;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.PluginDAO;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hbase.api.HBase;
import io.subutai.plugin.hbase.api.HBaseConfig;
import io.subutai.plugin.hbase.impl.alert.HBaseAlertListener;
import io.subutai.plugin.hbase.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.hbase.impl.handler.NodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class HBaseImpl implements HBase, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( HBaseImpl.class.getName() );
    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );
    protected ExecutorService executor;
    private Hadoop hadoopManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private PluginDAO pluginDAO;
    private Commands commands;
    private Monitor monitor;
    private QuotaManager quotaManager;


    public HBaseImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager,
                      final Monitor monitor )
    {
        this.hadoopManager = hadoopManager;
        this.tracker = tracker;
        this.monitor = monitor;
        this.environmentManager = environmentManager;
    }


    public void subscribeToAlerts( Environment environment ) throws MonitorException
    {
        getMonitor().startMonitoring( HBaseAlertListener.HBASE_ALERT_LISTENER, environment, alertSettings );
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public void setMonitor( final Monitor monitor )
    {
        this.monitor = monitor;
    }


    public void subscribeToAlerts( ContainerHost host ) throws MonitorException
    {
        getMonitor().activateMonitoring( host, alertSettings );
    }


    @Override
    public UUID installCluster( final HBaseConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroyNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        HBaseConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, hostname, NodeOperationType.DESTROY );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        HBaseConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.STOP_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        HBaseConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.START_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        HBaseConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, hostname, NodeOperationType.STATUS );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID addNode( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        HBaseConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.ADD );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public void saveConfig( final HBaseConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( HBaseConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public void setPluginDAO( final PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    @Override
    public UUID uninstallCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        HBaseConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public List<HBaseConfig> getClusters()
    {
        return pluginDAO.getInfo( HBaseConfig.PRODUCT_KEY, HBaseConfig.class );
    }


    @Override
    public HBaseConfig getCluster( String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        return pluginDAO.getInfo( HBaseConfig.PRODUCT_KEY, clusterName, HBaseConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        HBaseConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, hostname, NodeOperationType.ADD );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        LOG.info( String.format( "Environment created: %s", environment.getName() ) );
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<ContainerHost> set )
    {
        List<String> nodeNames = new ArrayList<>();
        for ( final ContainerHost containerHost : set )
        {
            nodeNames.add( containerHost.getHostname() );
        }
        LOG.info( String.format( "Environment: %s grown with containers: %s", environment.getName(),
                nodeNames.toString() ) );
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final UUID containerHostId )
    {
        List<HBaseConfig> clusterConfigs = new ArrayList<>( getClusters() );
        for ( final HBaseConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                if ( clusterConfig.getAllNodes().contains( containerHostId ) )
                {
                    clusterConfig.getBackupMasters().remove( containerHostId );
                    clusterConfig.getHadoopNodes().remove( containerHostId );
                    clusterConfig.getRegionServers().remove( containerHostId );
                    getPluginDAO().saveInfo( HBaseConfig.PRODUCT_KEY, clusterConfig.getClusterName(), clusterConfig );
                    LOG.info( String.format( "Container host:%s is removed from hbase cluster: %s.",
                            containerHostId.toString(), clusterConfig.getClusterName() ) );
                }
            }
        }
    }


    @Override
    public void onEnvironmentDestroyed( final UUID environmentId )
    {
        List<HBaseConfig> clusterConfigs = new ArrayList<>( getClusters() );
        for ( final HBaseConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environmentId ) )
            {
                getPluginDAO().deleteInfo( HBaseConfig.PRODUCT_KEY, clusterConfig.getClusterName() );
                LOG.info( String.format( "Cluster: %s destroyed in environment", clusterConfig.getClusterName() ) );
            }
        }
    }


    public void init()
    {
        try
        {
            this.pluginDAO = new PluginDAO( null );
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }

        this.commands = new Commands();
        this.executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public QuotaManager getQuotaManager()
    {
        return quotaManager;
    }


    public void setQuotaManager( final QuotaManager quotaManager )
    {
        this.quotaManager = quotaManager;
    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


    public void setExecutor( final ExecutorService executor )
    {
        this.executor = executor;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public Commands getCommands()
    {
        return commands;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}

