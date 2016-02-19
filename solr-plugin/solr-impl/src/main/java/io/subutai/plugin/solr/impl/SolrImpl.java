package io.subutai.plugin.solr.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.subutai.common.environment.Blueprint;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.NodeGroup;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.ContainerSize;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.UUIDUtil;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.solr.api.Solr;
import io.subutai.plugin.solr.api.SolrClusterConfig;
import io.subutai.plugin.solr.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.solr.impl.handler.NodeOperationHandler;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolrImpl implements Solr, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( SolrImpl.class.getName() );
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private PeerManager peerManager;
    private ExecutorService executor;
    private PluginDAO pluginDAO;


    public SolrImpl( PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public void setPluginDAO( PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public void init()
    {
        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
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


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setExecutor( final ExecutorService executor )
    {
        this.executor = executor;
    }


    public UUID installCluster( final SolrClusterConfig solrClusterConfig )
    {

        Preconditions.checkNotNull( solrClusterConfig, "Configuration is null" );

        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, solrClusterConfig, ClusterOperationType.INSTALL );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        SolrClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.UNINSTALL );
        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final SolrClusterConfig config )
    {
        return uninstallCluster( getCluster( config.getClusterName() ) );
    }


    public List<SolrClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( SolrClusterConfig.PRODUCT_KEY, SolrClusterConfig.class );
    }


    @Override
    public SolrClusterConfig getCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );

        return pluginDAO.getInfo( SolrClusterConfig.PRODUCT_KEY, clusterName, SolrClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String agentHostName )
    {
        return null;
    }


    @Override
    public UUID destroyNode( final String clusterName, final String lxcHostname )
    {
        return null;
    }


    public UUID startNode( final String clusterName, final String hostName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostName ), "Lxc hostname is null or empty" );


        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostName, NodeOperationType.START );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    public UUID stopNode( final String clusterName, final String hostName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostName ), "Lxc hostname is null or empty" );


        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostName, NodeOperationType.STOP );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    public UUID checkNode( final String clusterName, final String hostName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostName ), "Lxc hostname is null or empty" );


        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostName, NodeOperationType.STATUS );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final Environment environment, final SolrClusterConfig config,
                                                         final TrackerOperation po )
    {
        Preconditions.checkNotNull( environment, "Environment is null" );
        Preconditions.checkNotNull( config, "Solr cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation is null" );

        return new SolrSetupStrategy( this, po, config, environment );
    }


    @Override
    public Blueprint getDefaultEnvironmentBlueprint( SolrClusterConfig config )
    {
        //1 node group
        Set<NodeGroup> schema = new HashSet<>();
        schema.add( new NodeGroup( "solr-master-1", "solr", ContainerSize.TINY, 0, 0, null, null ) );
        return new Blueprint(
            String.format( "%s-%s", SolrClusterConfig.PRODUCT_KEY, UUIDUtil.generateTimeBasedUUID() ), schema );
    }


    public UUID configureEnvironmentCluster( final SolrClusterConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );

        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL_OVER_ENV );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        LOG.info( "Environment created: " + environment.getName() );
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {
        List<String> containerNames = new ArrayList<>();
        for ( final EnvironmentContainerHost containerHost : set )
        {
            containerNames.add( containerHost.getHostname() );
        }
        LOG.info( String.format( "Environment: %s has been grown with containers: %s", environment.getName(),
                containerNames.toString() ) );
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String containerHostId )
    {
        List<SolrClusterConfig> clusterConfigs = new ArrayList<>( getClusters() );
        for ( final SolrClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                if ( clusterConfig.getNodes().contains( containerHostId ) )
                {
                    clusterConfig.getNodes().remove( containerHostId );
                    clusterConfig.setNumberOfNodes( clusterConfig.getNodes().size() );
                    if ( clusterConfig.getNodes().size() == 0 )
                    {
                        getPluginDAO().deleteInfo( SolrClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName() );
                    }
                    else
                    {
                        getPluginDAO().saveInfo( SolrClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName(),
                                clusterConfig );
                    }
                    LOG.info( String.format( "Container host destroyed from solr cluster." ) );
                }
            }
        }
    }


    @Override
    public void onEnvironmentDestroyed( final String environmentId )
    {
        List<SolrClusterConfig> clusterConfigs = new ArrayList<>( getClusters() );
        for ( final SolrClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environmentId ) )
            {
                getPluginDAO().deleteInfo( SolrClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName() );
                LOG.info( String.format( "Solr cluster destroyed." ) );
            }
        }
    }
}
