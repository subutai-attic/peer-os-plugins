/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mongodb.impl;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.mdc.SubutaiExecutors;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentEventListener;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.lxc.quota.api.QuotaManager;
import org.safehaus.subutai.core.metric.api.Monitor;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.peer.api.PeerManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.mongodb.api.Mongo;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoDataNode;
import org.safehaus.subutai.plugin.mongodb.api.NodeType;
import org.safehaus.subutai.plugin.mongodb.impl.alert.MongoAlertListener;
import org.safehaus.subutai.plugin.mongodb.impl.common.Commands;
import org.safehaus.subutai.plugin.mongodb.impl.handler.AddNodeOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.CheckNodeOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.ConfigureEnvironmentOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.DestroyNodeOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.StartAllOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.StartNodeOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.StopNodeOperationHandler;
import org.safehaus.subutai.plugin.mongodb.impl.handler.UninstallOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


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
    private Gson GSON = new GsonBuilder().serializeNulls().excludeFieldsWithoutExposeAnnotation().create();
    private Monitor monitor;
    private MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );
    private QuotaManager quotaManager;
    private MongoAlertListener mongoAlertListener;

    private MongoDataNode primaryNode;


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

        executor = SubutaiExecutors.newCachedThreadPool();
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

        AbstractOperationHandler operationHandler = new ConfigureEnvironmentOperationHandler( this, config );

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


    public UUID destroyNode( final String clusterName, final String lxcHostname, final NodeType nodeType )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ), "Lxc hostname is null or empty" );


        AbstractOperationHandler operationHandler =
                new DestroyNodeOperationHandler( this, clusterName, lxcHostname, nodeType );

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


    @Override
    public UUID startAllNodes( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new StartAllOperationHandler( this, getCluster( clusterName ), ClusterOperationType.START_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopAllNodes( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new StartAllOperationHandler( this, getCluster( clusterName ), ClusterOperationType.STOP_ALL );
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


    @Override
    public void saveConfig( final MongoClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().disableHtmlEscaping()
                                     .excludeFieldsWithoutExposeAnnotation().create();
        String jsonConfig = gson.toJson( config.prepare() );
        if ( !getPluginDAO().saveInfo( MongoClusterConfig.PRODUCT_KEY, config.getClusterName(), jsonConfig ) )
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
            throw new ClusterException( "Could not save cluster info" );
        }
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


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        LOG.info( "Environment created." );
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<ContainerHost> set )
    {
        LOG.info( "Environment grown" );
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final UUID containerHostId )
    {
        List<String> clusterDataList = pluginDAO.getInfo( MongoClusterConfig.PRODUCT_KEY );
        for ( final String clusterData : clusterDataList )
        {
            MongoClusterConfigImpl clusterConfig = GSON.fromJson( clusterData, MongoClusterConfigImpl.class );
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                clusterConfig.removeNode( containerHostId );
                Gson GSON = new GsonBuilder().serializeNulls().excludeFieldsWithoutExposeAnnotation().create();
                String json = GSON.toJson( clusterConfig );
                getPluginDAO().saveInfo( MongoClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName(), json );
                LOG.info( String.format( "Container host: %s destroyed in cluster: %s", containerHostId,
                        clusterConfig.getClusterName() ) );
            }
        }

        //        List<MongoClusterConfig> clusterConfigs = getClusters();
        //        for ( final MongoClusterConfig clusterConfig : clusterConfigs )
        //        {
        //            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
        //            {
        //                if ( clusterConfig.getAllNodeIds().contains( containerHostId ) )
        //                {
        //                    clusterConfig.removeNode( containerHostId );
        //                    Gson GSON = new GsonBuilder().serializeNulls().excludeFieldsWithoutExposeAnnotation()
        // .create();
        //                    String json = GSON.toJson( clusterConfig.prepare() );
        //                    getPluginDAO().saveInfo( MongoClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName()
        // , json );
        //                    LOG.info( String.format( "Container host: %s destroyed in cluster: %s", containerHostId,
        //                            clusterConfig.getClusterName() ) );
        //                }
        //            }
        //        }
    }


    @Override
    public void onEnvironmentDestroyed( final UUID environmentId )
    {
        List<String> clusterDataList = pluginDAO.getInfo( MongoClusterConfig.PRODUCT_KEY );
        for ( final String clusterData : clusterDataList )
        {
            MongoClusterConfigImpl clusterConfig = GSON.fromJson( clusterData, MongoClusterConfigImpl.class );
            if ( clusterConfig.getEnvironmentId().equals( environmentId ) )
            {
                getPluginDAO().deleteInfo( MongoClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName() );
                LOG.info( String.format( "Mongo cluster: %s destroyed", clusterConfig.getClusterName() ) );
            }
        }

        //        List<MongoClusterConfig> clusterConfigs = getClusters();
        //        for ( final MongoClusterConfig clusterConfig : clusterConfigs )
        //        {
        //            if ( clusterConfig.getEnvironmentId().equals( environmentId ) )
        //            {
        //                getPluginDAO().deleteInfo( MongoClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName() );
        //                LOG.info( String.format( "Cluster %s destroyed in environment %s", clusterConfig
        // .getClusterName(),
        //                        environmentId.toString() ) );
        //            }
        //        }
    }
}
