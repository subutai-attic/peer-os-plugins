package org.safehaus.subutai.plugin.presto.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.core.env.api.EnvironmentEventListener;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.metric.api.Monitor;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.presto.api.Presto;
import org.safehaus.subutai.plugin.presto.api.PrestoClusterConfig;
import org.safehaus.subutai.plugin.presto.impl.alert.PrestoAlertListener;
import org.safehaus.subutai.plugin.presto.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.presto.impl.handler.NodeOperationHanler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class PrestoImpl implements Presto, EnvironmentEventListener
{
    private static final Logger LOG = LoggerFactory.getLogger( PrestoImpl.class.getName() );
    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );

    private Tracker tracker;
    private PluginDAO pluginDAO;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;
    private ExecutorService executor;
    private Monitor monitor;
    private PrestoAlertListener prestoAlertListener;
    Commands commands;


    public PrestoImpl( final Tracker tracker, final EnvironmentManager environmentManager,
                       final Hadoop hadoopManager, final Monitor monitor )
    {

        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
        this.monitor = monitor;

        prestoAlertListener = new PrestoAlertListener( this );
        monitor.addAlertListener( prestoAlertListener );
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    public void subscribeToAlerts( Environment environment ) throws MonitorException
    {
        monitor.startMonitoring( prestoAlertListener, environment, alertSettings );
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public void subscribeToAlerts( ContainerHost host ) throws MonitorException
    {
        monitor.activateMonitoring( host, alertSettings );
    }


    public void unsubscribeFromAlerts( final Environment environment ) throws MonitorException
    {
        monitor.stopMonitoring( prestoAlertListener, environment );
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

        executor = Executors.newCachedThreadPool();
    }


    public Commands getCommands()
    {
        return commands;
    }


    public void destroy()
    {
        executor.shutdown();
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    @Override
    public UUID installCluster( final PrestoClusterConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final PrestoClusterConfig config )
    {
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.DESTROY );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final String clusterName )
    {
        return null;
    }


    @Override
    public List<PrestoClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( PrestoClusterConfig.PRODUCT_KEY, PrestoClusterConfig.class );
    }


    @Override
    public PrestoClusterConfig getCluster( String clusterName )
    {
        return pluginDAO.getInfo( PrestoClusterConfig.PRODUCT_KEY, clusterName, PrestoClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String agentHostName )
    {
        return addWorkerNode( clusterName, agentHostName );
    }


    @Override
    public UUID addWorkerNode( final String clusterName, final String lxcHostname )
    {

        AbstractOperationHandler operationHandler =
                new NodeOperationHanler( this, clusterName, lxcHostname, NodeOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopAllNodes( final String clusterName )
    {
        PrestoClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.STOP_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroyWorkerNode( final String clusterName, final String lxcHostname )
    {

        AbstractOperationHandler operationHandler =
                new NodeOperationHanler( this, clusterName, lxcHostname, NodeOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHanler( this, clusterName, lxcHostname, NodeOperationType.START );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHanler( this, clusterName, lxcHostname, NodeOperationType.STOP );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHanler( this, clusterName, lxcHostname, NodeOperationType.STATUS );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final TrackerOperation po, final PrestoClusterConfig config )
    {
        return new SetupStrategyOverHadoop( po, this, config );
    }


    @Override
    public void saveConfig( final PrestoClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( PrestoClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final PrestoClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().deleteInfo( PrestoClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
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
        LOG.info( String.format( "Presto environment event: Container destroyed: %s", uuid ) );
        List<PrestoClusterConfig> clusterConfigs = getClusters();
        for ( final PrestoClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                LOG.info(
                        String.format( "Presto environment event: Target cluster: %s", clusterConfig.getClusterName() ) );

                if ( clusterConfig.getAllNodes().contains( uuid ) )
                {
                    LOG.info( String.format( "Hive environment event: Before: %s", clusterConfig ) );
                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getWorkers() ) )
                    {
                        clusterConfig.getWorkers().remove( uuid );
                    }
                    
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Presto environment event: After: %s", clusterConfig ) );
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
        LOG.info( String.format( "Presto environment event: Environment destroyed: %s", uuid ) );

        List<PrestoClusterConfig> clusterConfigs = getClusters();
        for ( final PrestoClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( uuid ) )
            {
                LOG.info(
                        String.format( "Presto environment event: Target cluster: %s", clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info(
                            String.format( "Presto environment event: Cluster removed", clusterConfig.getClusterName() ) );
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
