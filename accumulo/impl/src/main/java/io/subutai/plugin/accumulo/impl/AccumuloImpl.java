package io.subutai.plugin.accumulo.impl;


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
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.core.plugincommon.api.OperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.accumulo.api.Accumulo;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.accumulo.impl.handler.NodeOperationHandler;
import io.subutai.plugin.hadoop.api.Hadoop;


public class AccumuloImpl implements Accumulo, EnvironmentEventListener
{
    private static final Logger LOG = LoggerFactory.getLogger( AccumuloImpl.class.getName() );

    private Hadoop hadoopManager;
    private EnvironmentManager environmentManager;
    private Tracker tracker;
    private PluginDAO pluginDAO;
    private PeerManager peerManager;
    private ExecutorService executor;


    public AccumuloImpl( final Hadoop hadoopManager, final EnvironmentManager environmentManager, final Tracker tracker,
                         final PluginDAO pluginDAO, final PeerManager peerManager )
    {
        this.hadoopManager = hadoopManager;
        this.environmentManager = environmentManager;
        this.tracker = tracker;
        this.pluginDAO = pluginDAO;
        this.peerManager = peerManager;
    }


    public void init()
    {
        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        this.executor.shutdown();
        this.executor = null;
    }


    @Override
    public UUID installCluster( final AccumuloClusterConfig config )
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
        AccumuloClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public List<AccumuloClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( AccumuloClusterConfig.PRODUCT_KEY, AccumuloClusterConfig.class );
    }


    @Override
    public AccumuloClusterConfig getCluster( final String clusterName )
    {
        return pluginDAO.getInfo( AccumuloClusterConfig.PRODUCT_KEY, clusterName, AccumuloClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String hostId )
    {
        AccumuloClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, hostId, OperationType.INSTALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startNode( final String clusterName, final String hostId, final boolean isMaster )
    {
        AccumuloClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler = new NodeOperationHandler( this, config, hostId, OperationType.START,
                isMaster ? NodeType.MASTER_NODE : NodeType.SLAVE_NODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopNode( final String clusterName, final String hostId, final boolean isMaster )
    {
        AccumuloClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler = new NodeOperationHandler( this, config, hostId, OperationType.STOP,
                isMaster ? NodeType.MASTER_NODE : NodeType.SLAVE_NODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkNode( final String clusterName, final String hostId, final boolean isMaster )
    {
        AccumuloClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, hostId, OperationType.STATUS,
                        isMaster ? NodeType.MASTER_NODE : NodeType.SLAVE_NODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroyNode( final String clusterName, final String hostId )
    {
        AccumuloClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, hostId, OperationType.UNINSTALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public void saveConfig( final AccumuloClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !pluginDAO.saveInfo( AccumuloClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final AccumuloClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !pluginDAO.deleteInfo( AccumuloClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final TrackerOperation po,
                                                         final AccumuloClusterConfig clusterConfig,
                                                         final Environment environment )
    {
        return new AccumuloOverZkNHadoopSetup( po, this, clusterConfig, environment );
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
    public void onContainerDestroyed( final Environment environment, final String s )
    {

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
    public void onEnvironmentDestroyed( final String envId )
    {
        LOG.info( String.format( "Accumulo environment event: Environment destroyed: %s", envId ) );

        List<AccumuloClusterConfig> clusters = getClusters();
        for ( AccumuloClusterConfig clusterConfig : clusters )
        {
            if ( envId.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Accumulo environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Accumulo environment event: Cluster %s removed",
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


    public Tracker getTracker()
    {
        return tracker;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }
}
