package org.safehaus.subutai.plugin.shark.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.metric.api.Monitor;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.OperationType;
import org.safehaus.subutai.plugin.shark.api.Shark;
import org.safehaus.subutai.plugin.shark.api.SharkClusterConfig;
import org.safehaus.subutai.plugin.shark.impl.alert.SharkAlertListener;
import org.safehaus.subutai.plugin.shark.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.shark.impl.handler.NodeOperationHandler;
import org.safehaus.subutai.plugin.spark.api.Spark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class SharkImpl implements Shark
{

    private static final Logger LOG = LoggerFactory.getLogger( SharkImpl.class.getName() );
    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );

    private Spark sparkManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    protected ExecutorService executor;
    private PluginDAO pluginDAO;
    private DataSource dataSource;
    private Monitor monitor;

    protected Commands commands;
    private SharkAlertListener sharkAlertListener;


    public SharkImpl( Tracker tracker, EnvironmentManager environmentManager, Spark sparkManager, DataSource dataSource,
                      Monitor monitor )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.sparkManager = sparkManager;
        this.dataSource = dataSource;
        this.monitor = monitor;
        sharkAlertListener = new SharkAlertListener( this );
        monitor.addAlertListener( sharkAlertListener );
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    public void subscribeToAlerts( Environment environment ) throws MonitorException
    {
        getMonitor().startMonitoring( sharkAlertListener, environment, alertSettings );
    }


    public void subscribeToAlerts( ContainerHost host ) throws MonitorException
    {
        getMonitor().activateMonitoring( host, alertSettings );
    }


    public void unsubscribeFromAlerts( final Environment environment ) throws MonitorException
    {
        getMonitor().stopMonitoring( sharkAlertListener, environment );
    }


    public void setPluginDAO( final PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public Spark getSparkManager()
    {
        return sparkManager;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public PluginDAO getPluginDao()
    {
        return pluginDAO;
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


    public Tracker getTracker()
    {
        return tracker;
    }


    public Commands getCommands()
    {
        return commands;
    }


    public void destroy()
    {
        executor.shutdown();
    }


    @Override
    public UUID installCluster( final SharkClusterConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );

        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final String clusterName )
    {
        SharkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public List<SharkClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( SharkClusterConfig.PRODUCT_KEY, SharkClusterConfig.class );
    }


    @Override
    public SharkClusterConfig getCluster( String clusterName )
    {
        return pluginDAO.getInfo( SharkClusterConfig.PRODUCT_KEY, clusterName, SharkClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String hostname )
    {
        SharkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, hostname, OperationType.INCLUDE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroyNode( final String clusterName, final String lxcHostname )
    {
        SharkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, lxcHostname, OperationType.EXCLUDE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID actualizeMasterIP( final String clusterName )
    {
        SharkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.CUSTOM );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( TrackerOperation po, SharkClusterConfig config,
                                                         Environment environment )
    {
        return new SetupStrategyOverSpark( environment, this, config, po );
    }


    @Override
    public void saveConfig( final SharkClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !pluginDAO.saveInfo( SharkClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }
}

