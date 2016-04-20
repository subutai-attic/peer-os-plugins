package io.subutai.plugin.shark.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.OperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.shark.api.Shark;
import io.subutai.plugin.shark.api.SharkClusterConfig;
import io.subutai.plugin.shark.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.shark.impl.handler.NodeOperationHandler;
import io.subutai.plugin.spark.api.Spark;


public class SharkImpl implements Shark, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( SharkImpl.class.getName() );
    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );

    private Spark sparkManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    protected ExecutorService executor;
    private PluginDAO pluginDAO;
    private Monitor monitor;

    protected Commands commands;


    public SharkImpl( Tracker tracker, EnvironmentManager environmentManager, Spark sparkManager, Monitor monitor,
                      PluginDAO pluginDAO )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.sparkManager = sparkManager;
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
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
        this.commands = new Commands();
        executor = SubutaiExecutors.newCachedThreadPool();
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
    public void deleteConfig( final SharkClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !pluginDAO.deleteInfo( SharkClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String containerId )
    {
        LOG.info( String.format( "Shark environment event: Container destroyed: %s", containerId ) );

        List<SharkClusterConfig> clusters = getClusters();
        for ( SharkClusterConfig clusterConfig : clusters )
        {
            if ( environment.getId().equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Shark environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getNodeIds().contains( containerId ) )
                {
                    LOG.info( String.format( "Shark environment event: Before: %s", clusterConfig ) );

                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getNodeIds() ) )
                    {
                        clusterConfig.getNodeIds().remove( containerId );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Shark environment event: After: %s", clusterConfig ) );
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
    public void onEnvironmentDestroyed( final String envId )
    {
        LOG.info( String.format( "Shark environment event: Environment destroyed: %s", envId ) );

        List<SharkClusterConfig> clusters = getClusters();
        for ( SharkClusterConfig clusterConfig : clusters )
        {
            if ( envId.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Shark environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Shark environment event: Cluster %s removed",
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

