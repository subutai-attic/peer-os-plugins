package io.subutai.plugin.galera.impl;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.template.api.TemplateManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.galera.api.Galera;
import io.subutai.plugin.galera.api.GaleraClusterConfig;
import io.subutai.plugin.galera.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.galera.impl.handler.NodeOperationHandler;


public class GaleraImpl implements Galera, EnvironmentEventListener
{
    private static final Logger LOG = LoggerFactory.getLogger( GaleraImpl.class.getName() );

    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private PluginDAO pluginDAO;
    private PeerManager peerManager;
    private TemplateManager templateManager;
    private ExecutorService executor;


    public GaleraImpl( PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
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
    public UUID installCluster( final GaleraClusterConfig galeraClusterConfig )
    {
        Preconditions.checkNotNull( galeraClusterConfig, "Configuration is null" );

        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, galeraClusterConfig, ClusterOperationType.INSTALL );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        GaleraClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.UNINSTALL );
        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public List<GaleraClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( GaleraClusterConfig.PRODUCT_KEY, GaleraClusterConfig.class );
    }


    @Override
    public GaleraClusterConfig getCluster( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );

        return pluginDAO.getInfo( GaleraClusterConfig.PRODUCT_KEY, clusterName, GaleraClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        GaleraClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler h = new ClusterOperationHandler( this, config, ClusterOperationType.ADD );
        executor.execute( h );
        return h.getTrackerId();
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
    public void onEnvironmentCreated( final Environment environment )
    {

    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {

    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String containerId )
    {
        List<GaleraClusterConfig> clusters = getClusters();
        for ( GaleraClusterConfig clusterConfig : clusters )
        {
            if ( environment.getId().equals( clusterConfig.getEnvironmentId() ) )
            {

                if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getNodes() ) )
                {
                    clusterConfig.getNodes().remove( containerId );
                }
                try
                {
                    saveConfig( clusterConfig );
                }
                catch ( ClusterException e )
                {
                    LOG.error( "Error updating cluster config", e );
                }
                break;
            }
        }
    }


    public void saveConfig( final GaleraClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( GaleraClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void onEnvironmentDestroyed( final String envId )
    {
        List<GaleraClusterConfig> clusters = getClusters();
        for ( GaleraClusterConfig clusterConfig : clusters )
        {
            if ( envId.equals( clusterConfig.getEnvironmentId() ) )
            {

                try
                {
                    deleteConfig( clusterConfig );
                }
                catch ( ClusterException e )
                {
                    LOG.error( "Error deleting cluster config", e );
                }
                break;
            }
        }
    }


    private void deleteConfig( final GaleraClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().deleteInfo( GaleraClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final Environment environment,
                                                         final GaleraClusterConfig config, final TrackerOperation po )
    {
        Preconditions.checkNotNull( environment, "Environment is null" );
        Preconditions.checkNotNull( config, "Solr cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation is null" );

        return new GaleraSetupStrategy( this, po, config, environment );
    }


    @Override
    public UUID startNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.START );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID stopNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.STOP );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID checkNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.STATUS );
        executor.execute( h );
        return h.getTrackerId();
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public void setPeerManager( final PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    private void workaround()
    {
        for ( GaleraClusterConfig config : getClusters() )
        {
            try
            {
                Set<ContainerHost> nodes = new HashSet<>();

                for ( String hostId : config.getNodes() )
                {
                    nodes.add( peerManager.getLocalPeer().getContainerHostById( hostId ) );
                }

                for ( ContainerHost node : nodes )
                {
                    node.execute( new RequestBuilder(
                            "mysql -u root -proot -e 'SET GLOBAL wsrep_provider_options=\"pc.bootstrap=yes\";'" ) );
                }
            }
            catch ( Exception e )
            {
                //ignore
            }
        }
    }


    public TemplateManager getTemplateManager()
    {
        return templateManager;
    }


    public void setTemplateManager( final TemplateManager templateManager )
    {
        this.templateManager = templateManager;
    }
}
