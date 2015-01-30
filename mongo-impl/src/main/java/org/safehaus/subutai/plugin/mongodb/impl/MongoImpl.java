/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mongodb.impl;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.protocol.EnvironmentBlueprint;
import org.safehaus.subutai.common.protocol.NodeGroup;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.UUIDUtil;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.lxc.quota.api.QuotaManager;
import org.safehaus.subutai.core.metric.api.Monitor;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.peer.api.PeerManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.mongodb.api.Mongo;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.NodeType;
import org.safehaus.subutai.plugin.mongodb.impl.alert.MongoAlertListener;
import org.safehaus.subutai.plugin.mongodb.impl.common.Commands;
import org.safehaus.subutai.plugin.mongodb.impl.handler.AddNodeOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.CheckNodeOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.ConfigureEnvironmentOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.DestroyNodeOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.InstallOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.StartNodeOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.StopNodeOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.UninstallOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * Implementation of Mongo interface. Implements all backend logic for mongo cluster management
 */

public class MongoImpl implements Mongo
{

    private static final Logger LOG = LoggerFactory.getLogger( MongoImpl.class.getName() );
    private Tracker tracker;
    //    private ContainerManager containerManager;
    private EnvironmentManager environmentManager;
    private ExecutorService executor;
    private Commands commands;
    private PluginDAO pluginDAO;
    private PeerManager peerManager;
    private Gson GSON = new GsonBuilder().serializeNulls().excludeFieldsWithoutExposeAnnotation().create();
    private Monitor monitor;
    private MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );
    private QuotaManager quotaManager;
    private MongoAlertListener mongoAlertListener;


    public MongoImpl( Monitor monitor )
    {
        this.monitor = monitor;
        this.mongoAlertListener = new MongoAlertListener( this );
        this.monitor.addAlertListener( mongoAlertListener );
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
        try
        {

            GsonBuilder gsonBuilder = new GsonBuilder();
            gsonBuilder.setExclusionStrategies( new ExclusionStrategy()
            {
                @Override
                public boolean shouldSkipField( final FieldAttributes f )
                {
                    switch ( f.getName() )
                    {
                        case "configServers":
                        case "routerServers":
                        case "dataNodes":
                            return true;
                    }
                    return false;
                }


                @Override
                public boolean shouldSkipClass( final Class<?> clazz )
                {
                    return false;
                }
            } );

            GSON = gsonBuilder.serializeNulls().setPrettyPrinting().disableHtmlEscaping().create();
            this.pluginDAO = new PluginDAO( null );
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        this.commands = new Commands();

        executor = Executors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    public Gson getGSON()
    {
        return GSON;
    }


    public UUID installCluster( MongoClusterConfig config )
    {

        Preconditions.checkNotNull( config, "Configuration is null" );

        AbstractOperationHandler operationHandler = new InstallOperationHandler( this, config );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    public UUID uninstallCluster( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );


        AbstractOperationHandler operationHandler = new UninstallOperationHandler( this, clusterName );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    public UUID configureEnvironmentCluster( MongoClusterConfig config )
    {

        Preconditions.checkNotNull( config, "Configuration is null" );

        AbstractOperationHandler operationHandler = new ConfigureEnvironmentOperationHandler( this, config );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    public List<MongoClusterConfig> getClusters()
    {
        List<String> clusterDataList = pluginDAO.getInfo( MongoClusterConfig.PRODUCT_KEY );

        List<MongoClusterConfigImpl> r = new ArrayList<>();
        for ( final String clusterData : clusterDataList )
        {
            MongoClusterConfigImpl config = null;
            try
            {
                config = GSON.fromJson( clusterData, MongoClusterConfigImpl.class )
                             .initTransientFields( environmentManager );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOG.error( "Environment with not found", e );
                return null;
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                return null;
            }
            r.add( config );
        }

        List<MongoClusterConfig> result = new ArrayList<>();
        result.addAll( r );
        return result;
    }


    @Override
    public MongoClusterConfig getCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );

        String jsonString = pluginDAO.getInfo( MongoClusterConfig.PRODUCT_KEY, clusterName );
        MongoClusterConfigImpl clusterConfig = GSON.fromJson( jsonString, MongoClusterConfigImpl.class );
        if ( clusterConfig != null )
        {
            try
            {
                return clusterConfig.initTransientFields( environmentManager );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOG.error( "Error environment not found.", e );
                return null;
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error container host not found.", e );
                return null;
            }
        }
        return null;
    }


    @Override
    public UUID addNode( final String clusterName, final String agentHostName )
    {
        throw new UnsupportedOperationException( "Unsupported operation exception." );
    }


    public UUID addNode( final String clusterName, final NodeType nodeType )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkNotNull( nodeType, "Node type is null" );


        AbstractOperationHandler operationHandler = new AddNodeOperationHandler( this, clusterName, nodeType );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    public UUID destroyNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ), "Lxc hostname is null or empty" );


        AbstractOperationHandler operationHandler = new DestroyNodeOperationHandler( this, clusterName, lxcHostname );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    public UUID startNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ), "Lxc hostname is null or empty" );


        AbstractOperationHandler operationHandler = new StartNodeOperationHandler( this, clusterName, lxcHostname );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    public UUID stopNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ), "Lxc hostname is null or empty" );


        AbstractOperationHandler operationHandler = new StopNodeOperationHandler( this, clusterName, lxcHostname );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    public UUID checkNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ), "Lxc hostname is null or empty" );


        AbstractOperationHandler operationHandler = new CheckNodeOperationHandler( this, clusterName, lxcHostname );

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


    @Override
    public EnvironmentBlueprint getDefaultEnvironmentBlueprint( MongoClusterConfig config )
    {
        Preconditions.checkNotNull( config, "Mongo cluster config is null" );
        //        EnvironmentBuildTask environmentBuildTask = new EnvironmentBuildTask();

        EnvironmentBlueprint environmentBlueprint = new EnvironmentBlueprint();
        environmentBlueprint
                .setName( String.format( "%s-%s", MongoClusterConfig.PRODUCT_KEY, UUIDUtil.generateTimeBasedUUID() ) );
        environmentBlueprint.setLinkHosts( true );
        environmentBlueprint.setDomainName( Common.DEFAULT_DOMAIN_NAME );

        //config servers
        NodeGroup cfgServersGroup = new NodeGroup();
        cfgServersGroup.setName( NodeType.CONFIG_NODE.name() );
        cfgServersGroup.setNumberOfNodes( config.getNumberOfConfigServers() );
        cfgServersGroup.setTemplateName( config.getTemplateName() );
        cfgServersGroup.setPlacementStrategy(
                MongoDbSetupStrategy.getNodePlacementStrategyByNodeType( NodeType.CONFIG_NODE ) );

        //routers
        NodeGroup routersGroup = new NodeGroup();
        routersGroup.setName( NodeType.ROUTER_NODE.name() );
        routersGroup.setNumberOfNodes( config.getNumberOfRouters() );
        routersGroup.setTemplateName( config.getTemplateName() );
        routersGroup.setPlacementStrategy(
                MongoDbSetupStrategy.getNodePlacementStrategyByNodeType( NodeType.ROUTER_NODE ) );

        //data nodes
        NodeGroup dataNodesGroup = new NodeGroup();
        dataNodesGroup.setName( NodeType.DATA_NODE.name() );
        dataNodesGroup.setNumberOfNodes( config.getNumberOfDataNodes() );
        dataNodesGroup.setTemplateName( config.getTemplateName() );
        dataNodesGroup
                .setPlacementStrategy( MongoDbSetupStrategy.getNodePlacementStrategyByNodeType( NodeType.DATA_NODE ) );

        environmentBlueprint.setNodeGroups( Sets.newHashSet( cfgServersGroup, routersGroup, dataNodesGroup ) );

        //        environmentBuildTask.setEnvironmentBlueprint( environmentBlueprint );
        return environmentBlueprint;
    }


    public void subscribeToAlerts( Environment environment ) throws MonitorException
    {
        getMonitor().startMonitoring( mongoAlertListener, environment, alertSettings );
    }


    public void subscribeToAlerts( ContainerHost host ) throws MonitorException
    {
        getMonitor().activateMonitoring( host, alertSettings );
    }


    public void unsubscribeFromAlerts( final Environment environment ) throws MonitorException
    {
        getMonitor().stopMonitoring( mongoAlertListener, environment );
    }


    @Override
    public MongoClusterConfig newMongoClusterConfigInstance()
    {
        return new MongoClusterConfigImpl();
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


    public QuotaManager getQuotaManager()
    {
        return quotaManager;
    }
}
