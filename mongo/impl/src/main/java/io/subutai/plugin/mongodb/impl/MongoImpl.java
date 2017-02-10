/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.mongodb.impl;


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
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.strategy.api.StrategyManager;
import io.subutai.core.template.api.TemplateManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.mongodb.api.Mongo;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.plugin.mongodb.api.NodeType;
import io.subutai.plugin.mongodb.impl.common.Commands;
import io.subutai.plugin.mongodb.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.mongodb.impl.handler.NodeOperationHandler;
import io.subutai.webui.api.WebuiModule;


/**
 * Implementation of Mongo interface. Implements all backend logic for mongo cluster management
 */

public class MongoImpl implements Mongo, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( MongoImpl.class.getName() );
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private ExecutorService executor;
    private Commands commands;
    private PluginDAO pluginDAO;
    private PeerManager peerManager;
    private StrategyManager strategyManager;
    private Monitor monitor;
    private MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );
    private MongoWebModule webModule;
    private TemplateManager templateManager;


    public MongoImpl( Monitor monitor, PluginDAO pluginDAO, MongoWebModule webModule )
    {
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
        this.webModule = webModule;
    }


    public Commands getCommands()
    {
        return commands;
    }


    public void setCommands( final Commands commands )
    {
        this.commands = commands;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    public void setPeerManager( final PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setExecutor( final ExecutorService executor )
    {
        this.executor = executor;
    }


    public void init()
    {
        executor = SubutaiExecutors.newSingleThreadExecutor();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    public UUID installCluster( MongoClusterConfig config )
    {
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, templateManager, config, ClusterOperationType.INSTALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID uninstallCluster( final String clusterName )
    {
        MongoClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, templateManager, config, ClusterOperationType.REMOVE, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public List<MongoClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( MongoClusterConfig.PRODUCT_KEY, MongoClusterConfig.class );
    }


    @Override
    public MongoClusterConfig getCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        return pluginDAO.getInfo( MongoClusterConfig.PRODUCT_KEY, clusterName, MongoClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String agentHostName )
    {
        throw new UnsupportedOperationException( "Unsupported operation exception." );
    }


    public UUID addNode( final String clusterName, final NodeType nodeType )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, templateManager, getCluster( clusterName ), ClusterOperationType.ADD,
                        nodeType );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID destroyNode( final String clusterName, final String lxcHostname, final NodeType nodeType )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ), "Lxc hostname is null or empty" );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, lxcHostname, nodeType, NodeOperationType.DESTROY );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID startNode( final String clusterName, final String lxcHostname, NodeType nodeType )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ), "Lxc hostname is null or empty" );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, lxcHostname, nodeType, NodeOperationType.START );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID stopNode( final String clusterName, final String lxcHostname, NodeType nodeType )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ), "Lxc hostname is null or empty" );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, lxcHostname, nodeType, NodeOperationType.STOP );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startAllNodes( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, templateManager, getCluster( clusterName ),
                        ClusterOperationType.START_ALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopAllNodes( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, templateManager, getCluster( clusterName ),
                        ClusterOperationType.STOP_ALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID checkNode( final String clusterName, final String lxcHostname, NodeType nodeType )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ), "Lxc hostname is null or empty" );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, lxcHostname, nodeType, NodeOperationType.STATUS );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final Environment environment, final MongoClusterConfig config,
                                                         final TrackerOperation po )
    {
        Preconditions.checkNotNull( environment, "Environment is null" );
        Preconditions.checkNotNull( config, "Mongo cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation is null" );

        return new MongoDbSetupStrategy( environment, config, po, this );
    }


    /*public void subscribeToAlerts( Environment environment ) throws MonitorException
    {
        getMonitor().startMonitoring( MongoAlertListener.MONGO_ALERT_LISTENER, environment, alertSettings );
    }


    public void subscribeToAlerts( EnvironmentContainerHost host ) throws MonitorException
    {
        getMonitor().activateMonitoring( host, alertSettings );
    }


    public void unsubscribeFromAlerts( final Environment environment ) throws MonitorException
    {
        getMonitor().stopMonitoring( MongoAlertListener.MONGO_ALERT_LISTENER, environment );
    }*/


    @Override
    public MongoClusterConfig newMongoClusterConfigInstance()
    {
        return new MongoClusterConfig();
    }


    @Override
    public void saveConfig( final MongoClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( MongoClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final MongoClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().deleteInfo( MongoClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    @Override
    public UUID startService( String clusterName, String hostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.START );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopService( String clusterName, String hostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.STOP );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    public void setAlertSettings( final MonitoringSettings alertSettings )
    {
        this.alertSettings = alertSettings;
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        LOG.info( "Environment created." );
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {
        LOG.info( "Environment grown" );
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String id )
    {
        LOG.info( String.format( "Mongo environment event: Container destroyed: %s", id ) );
        List<MongoClusterConfig> clusters = getClusters();
        for ( MongoClusterConfig clusterConfig : clusters )
        {
            if ( environment.getId().equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Mongo environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getAllNodes().contains( id ) )
                {
                    LOG.info( String.format( "Mongo environment event: Before: %s", clusterConfig ) );

                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getConfigHosts() ) )
                    {
                        if ( clusterConfig.getConfigHosts().size() == 1 )
                        {
                            try
                            {
                                LOG.info( "Removing cluster config object, since single config server container is "
                                        + "destroyed." );
                                deleteConfig( clusterConfig );
                            }
                            catch ( ClusterException e )
                            {
                                e.printStackTrace();
                            }
                        }
                        else
                        {
                            clusterConfig.getConfigHosts().remove( id );
                        }
                    }
                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getRouterHosts() ) )
                    {
                        if ( clusterConfig.getRouterHosts().size() == 1 )
                        {
                            try
                            {
                                LOG.info( "Removing cluster config object, since single router node container is "
                                        + "destroyed." );
                                deleteConfig( clusterConfig );
                            }
                            catch ( ClusterException e )
                            {
                                e.printStackTrace();
                            }
                        }
                        else
                        {
                            clusterConfig.getRouterHosts().remove( id );
                        }
                    }
                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getDataHosts() ) )
                    {
                        if ( clusterConfig.getDataHosts().size() == 1 )
                        {
                            try
                            {
                                LOG.info( "Removing cluster config object, since single data node container is "
                                        + "destroyed." );
                                deleteConfig( clusterConfig );
                            }
                            catch ( ClusterException e )
                            {
                                e.printStackTrace();
                            }
                        }
                        else
                        {
                            clusterConfig.getDataHosts().remove( id );
                        }
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Mongo environment event: After: %s", clusterConfig ) );
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
    public void onEnvironmentDestroyed( final String environmentId )
    {
        List<MongoClusterConfig> configs = getClusters();
        for ( final MongoClusterConfig config : configs )
        {
            if ( config.getEnvironmentId().equals( environmentId ) )
            {
                try
                {
                    deleteConfig( config );
                    LOG.info( String.format( "Mongo cluster: %s destroyed", config.getClusterName() ) );
                }
                catch ( ClusterException e )
                {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public void onContainerStarted( final Environment environment, final String s )
    {

    }


    @Override
    public void onContainerStopped( final Environment environment, final String s )
    {

    }


    @Override
    public WebuiModule getWebModule()
    {
        return webModule;
    }


    @Override
    public void setWebModule( final WebuiModule webModule )
    {
        this.webModule = ( MongoWebModule ) webModule;
    }


    public StrategyManager getStrategyManager()
    {
        return strategyManager;
    }


    public void setStrategyManager( final StrategyManager strategyManager )
    {
        this.strategyManager = strategyManager;
    }


    public void setTemplateManager( final TemplateManager templateManager )
    {
        this.templateManager = templateManager;
    }
}
