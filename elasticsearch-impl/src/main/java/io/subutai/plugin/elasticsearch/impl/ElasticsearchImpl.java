package io.subutai.plugin.elasticsearch.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.env.api.EnvironmentEventListener;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.elasticsearch.api.Elasticsearch;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.alert.EsAlertListener;
import io.subutai.plugin.elasticsearch.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.elasticsearch.impl.handler.NodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


public class ElasticsearchImpl implements Elasticsearch, EnvironmentEventListener
{
    private static final Logger LOG = LoggerFactory.getLogger( ElasticsearchImpl.class.getName() );
    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );
    private Tracker tracker;
    protected ExecutorService executor;
    private EnvironmentManager environmentManager;
    private PluginDAO pluginDAO;
    private Monitor monitor;
    private PeerManager peerManager;

    Commands commands = new Commands();


    public ElasticsearchImpl( final Monitor monitor,PluginDAO pluginDAO )
    {
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public Commands getCommands()
    {
        return commands;
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public void subscribeToAlerts( Environment environment ) throws MonitorException
    {
        getMonitor().startMonitoring( EsAlertListener.ES_ALERT_LISTENER, environment, alertSettings );
    }


    public void subscribeToAlerts( ContainerHost host ) throws MonitorException
    {
        getMonitor().activateMonitoring( host, alertSettings );
    }


    public void unsubscribeFromAlerts( final Environment environment ) throws MonitorException
    {
        getMonitor().stopMonitoring( EsAlertListener.ES_ALERT_LISTENER, environment );
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public void setPluginDAO( PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void init()
    {
        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    @Override
    public UUID installCluster( final ElasticsearchClusterConfiguration config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );

        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( String clusterName )
    {
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public List<ElasticsearchClusterConfiguration> getClusters()
    {
        return pluginDAO
                .getInfo( ElasticsearchClusterConfiguration.PRODUCT_KEY, ElasticsearchClusterConfiguration.class );
    }


    @Override
    public ElasticsearchClusterConfiguration getCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        return pluginDAO.getInfo( ElasticsearchClusterConfiguration.PRODUCT_KEY, clusterName,
                ElasticsearchClusterConfiguration.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String hostname )
    {
        ElasticsearchClusterConfiguration config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.ADD );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroyNode( final String clusterName, final String hostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID removeCluster( final String clusterName )
    {
        ElasticsearchClusterConfiguration config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.REMOVE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startCluster( final String clusterName )
    {
        ElasticsearchClusterConfiguration config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.START_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopCluster( final String clusterName )
    {
        ElasticsearchClusterConfiguration config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.STOP_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkCluster( final String clusterName )
    {
        ElasticsearchClusterConfiguration config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.STATUS_ALL );
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
    public UUID startNode( final String clusterName, final String hostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.START );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopNode( final String clusterName, final String hostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.STOP );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public void saveConfig( final ElasticsearchClusterConfiguration config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO()
                .saveInfo( ElasticsearchClusterConfiguration.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    @Override
    public void deleteConfig( final ElasticsearchClusterConfiguration config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().deleteInfo( ElasticsearchClusterConfiguration.PRODUCT_KEY, config.getClusterName() ) )
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
    public void onEnvironmentGrown( final Environment environment, final Set<ContainerHost> set )
    {
        //not needed
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final UUID uuid )
    {
        LOG.info( String.format( "Elasticsearch environment event: Container destroyed: %s", uuid ) );
        List<ElasticsearchClusterConfiguration> clusters = getClusters();
        for ( ElasticsearchClusterConfiguration clusterConfig : clusters )
        {
            if ( environment.getId().equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Elasticsearch environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getNodes().contains( uuid ) )
                {
                    LOG.info( String.format( "Elasticsearch environment event: Before: %s", clusterConfig ) );

                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getNodes() ) )
                    {
                        clusterConfig.getNodes().remove( uuid );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Elasticsearch environment event: After: %s", clusterConfig ) );
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
        LOG.info( String.format( "Elasticsearch environment event: Environment destroyed: %s", uuid ) );

        List<ElasticsearchClusterConfiguration> clusters = getClusters();
        for ( ElasticsearchClusterConfiguration clusterConfig : clusters )
        {
            if ( uuid.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Elasticsearch environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Elasticsearch environment event: Cluster %s removed",
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


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    public void setPeerManager( final PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }
}
