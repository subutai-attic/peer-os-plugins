package io.subutai.plugin.oozie.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
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
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.oozie.api.Oozie;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.alert.OozieAlertListener;
import io.subutai.plugin.oozie.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.oozie.impl.handler.NodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


public class OozieImpl implements Oozie, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( OozieImpl.class.getName() );
    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );

    private Monitor monitor;
    private QuotaManager quotaManager;

    private Tracker tracker;
    private PluginDAO pluginDao;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;
    private ExecutorService executor;


    public OozieImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager,
                      final Monitor monitor )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
        this.monitor = monitor;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public PluginDAO getPluginDao()
    {
        return pluginDao;
    }


    public void setPluginDao( PluginDAO pluginDao )
    {
        this.pluginDao = pluginDao;
    }


    public void setExecutor( ExecutorService executor )
    {
        this.executor = executor;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    public void init()
    {
        try
        {
            this.pluginDao = new PluginDAO( null );
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }

        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        tracker = null;
        hadoopManager = null;
        executor.shutdown();
    }


    public UUID installCluster( final OozieClusterConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );
        Preconditions
                .checkArgument( !Strings.isNullOrEmpty( config.getClusterName() ), "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID uninstallCluster( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        OozieClusterConfig oozieClusterConfig = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, oozieClusterConfig, ClusterOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public List<OozieClusterConfig> getClusters()
    {
        return pluginDao.getInfo( OozieClusterConfig.PRODUCT_KEY, OozieClusterConfig.class );
    }


    public OozieClusterConfig getCluster( String clusterName )
    {
        return pluginDao.getInfo( OozieClusterConfig.PRODUCT_KEY, clusterName, OozieClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.INSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID startNode( String clusterName, String lxcHostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.START );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID stopNode( String clusterName, String lxcHostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.STOP );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID checkNode( String clusterName, String lxcHostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.STATUS );
        executor.execute( h );
        return h.getTrackerId();
    }


    public ClusterSetupStrategy getClusterSetupStrategy( final OozieClusterConfig config, final TrackerOperation po )
    {
        Preconditions.checkNotNull( config, "Oozie cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation is null" );

        return new OverHadoopSetupStrategy( config, po, this );
    }


    @Override
    public UUID destroyNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public void saveConfig( final OozieClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().saveInfo( OozieClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final OozieClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().deleteInfo( OozieClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    public QuotaManager getQuotaManager()
    {
        return quotaManager;
    }


    public void setQuotaManager( final QuotaManager quotaManager )
    {
        this.quotaManager = quotaManager;
    }


    public void subscribeToAlerts( Environment environment ) throws MonitorException
    {
        getMonitor().startMonitoring( OozieAlertListener.OOZIE_ALERT_LISTENER, environment, alertSettings );
    }


    public void subscribeToAlerts( ContainerHost host ) throws MonitorException
    {
        getMonitor().activateMonitoring( host, alertSettings );
    }


    public void unsubscribeFromAlerts( final Environment environment ) throws MonitorException
    {
        getMonitor().stopMonitoring( OozieAlertListener.OOZIE_ALERT_LISTENER, environment );
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        // not need
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<ContainerHost> set )
    {
        // not need
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final UUID uuid )
    {
        LOG.info( String.format( "Oozie environment event: Container destroyed: %s", uuid ) );
        List<OozieClusterConfig> clusterConfigs = getClusters();
        for ( final OozieClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                LOG.info( String.format( "Oozie environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getAllNodes().contains( uuid ) )
                {
                    LOG.info( String.format( "Oozie environment event: Before: %s", clusterConfig ) );
                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getClients() ) )
                    {
                        clusterConfig.getClients().remove( uuid );
                    }

                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Oozie environment event: After: %s", clusterConfig ) );
                    }
                    catch ( ClusterException e )
                    {
                        LOG.error( "Error updating cluster config", e );
                    }
                    break;
                }
            }
        }
    }


    @Override
    public void onEnvironmentDestroyed( final UUID uuid )
    {
        LOG.info( String.format( "Oozie environment event: Environment destroyed: %s", uuid ) );

        List<OozieClusterConfig> clusterConfigs = getClusters();
        for ( final OozieClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( uuid ) )
            {
                LOG.info( String.format( "Oozie environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Oozie environment event: Cluster removed",
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
}
