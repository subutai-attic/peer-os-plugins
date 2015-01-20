package org.safehaus.subutai.plugin.presto.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
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


public class PrestoImpl implements Presto
{
    private static final Logger LOG = LoggerFactory.getLogger( PrestoImpl.class.getName() );
    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );

    private Tracker tracker;
    private PluginDAO pluginDAO;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;
    private DataSource dataSource;
    private ExecutorService executor;
    private Monitor monitor;
    private PrestoAlertListener prestoAlertListener;
    Commands commands;


    public PrestoImpl( final DataSource dataSource, final Tracker tracker, final EnvironmentManager environmentManager,
                       final Hadoop hadoopManager, final Monitor monitor )
    {

        this.dataSource = dataSource;
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
            this.pluginDAO = new PluginDAO( dataSource );
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
}
