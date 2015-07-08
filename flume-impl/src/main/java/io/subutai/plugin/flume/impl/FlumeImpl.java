package io.subutai.plugin.flume.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.env.api.EnvironmentEventListener;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.PluginDAO;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.flume.api.Flume;
import io.subutai.plugin.flume.api.FlumeConfig;
import io.subutai.plugin.flume.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.flume.impl.handler.NodeOperationHandler;
import io.subutai.plugin.hadoop.api.Hadoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class FlumeImpl implements Flume, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( FlumeImpl.class.getName() );
    private Tracker tracker;
    private PluginDAO pluginDao;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;
    private ExecutorService executor;


    public FlumeImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager )
    {

        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
    }

    public void setPluginDao(PluginDAO pluginDao)
    {
        this.pluginDao = pluginDao;
    }


    public void setExecutor(ExecutorService executor)
    {
        this.executor = executor;
    }

    public Tracker getTracker()
    {
        return tracker;
    }


    public PluginDAO getPluginDao()
    {
        return pluginDao;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void init()
    {
        try
        {
            this.pluginDao = new PluginDAO( null );
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }

        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    @Override
    public UUID installCluster( final FlumeConfig config )
    {
        ClusterOperationHandler h = new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final String clusterName )
    {
        ClusterOperationHandler h =
                new ClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public List<FlumeConfig> getClusters()
    {
        return pluginDao.getInfo( FlumeConfig.PRODUCT_KEY, FlumeConfig.class );
    }


    @Override
    public FlumeConfig getCluster( String clusterName )
    {
        return pluginDao.getInfo( FlumeConfig.PRODUCT_KEY, clusterName, FlumeConfig.class );
    }


    @Override
    public UUID startNode( final String clusterName, final String hostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.START );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID stopNode( final String clusterName, final String hostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.STOP );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID checkNode( final String clusterName, final String hostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.STATUS );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID checkServiceStatus( final String clusterName, final String hostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.STATUS );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID addNode( final String clusterName, final String hostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.INSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID destroyNode( final String clusterName, final String hostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( FlumeConfig config, TrackerOperation po )
    {
        return new FlumeSetupStrategy( this, config, po );
    }


    @Override
    public void saveConfig( final FlumeConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().saveInfo( FlumeConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final FlumeConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().deleteInfo( FlumeConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        // not need   
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<ContainerHost> set )
    {
        // not need
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final UUID uuid )
    {
        LOG.info( String.format( "Flume environment event: Container destroyed: %s", uuid ) );
        List<FlumeConfig> clusterConfigs = getClusters();
        for ( final FlumeConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                LOG.info( String.format( "Flume environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getNodes().contains( uuid ) )
                {
                    LOG.info( String.format( "Flume environment event: Before: %s", clusterConfig ) );
                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getNodes() ) )
                    {
                        clusterConfig.getNodes().remove( uuid );
                    }

                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Flume environment event: After: %s", clusterConfig ) );
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
        LOG.info( String.format( "Flume environment event: Environment destroyed: %s", uuid ) );

        List<FlumeConfig> clusterConfigs = getClusters();
        for ( final FlumeConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( uuid ) )
            {
                LOG.info( String.format( "Flume environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Flume environment event: Cluster removed",
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
