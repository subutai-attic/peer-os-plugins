package io.subutai.plugin.pig.impl;


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
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.pig.api.Pig;
import io.subutai.plugin.pig.api.PigConfig;
import io.subutai.plugin.pig.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.pig.impl.handler.NodeOperationHandler;


public class PigImpl implements Pig, EnvironmentEventListener
{
    private static final Logger LOG = LoggerFactory.getLogger( PigImpl.class.getName() );
    private Tracker tracker;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;
    private PluginDAO pluginDao;


    public PigImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager,
                    PluginDAO pluginDAO )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
        this.pluginDao = pluginDAO;
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
        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public PluginDAO getPluginDao()
    {
        return pluginDao;
    }


    @Override
    public UUID installCluster( final PigConfig config )
    {
        ClusterOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final PigConfig config )
    {
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.DESTROY );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final String clusterName )
    {
        return uninstallCluster( getCluster( clusterName ) );
    }


    @Override
    public UUID destroyNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID addNode( final String clusterName, final String lxcHostName )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, lxcHostName, NodeOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final PigConfig config, TrackerOperation po )
    {

        return new PigSetupStrategy( this, config, po );
    }


    @Override
    public void saveConfig( final PigConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().saveInfo( PigConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final PigConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().deleteInfo( PigConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    @Override
    public List<PigConfig> getClusters()
    {
        return pluginDao.getInfo( PigConfig.PRODUCT_KEY, PigConfig.class );
    }


    @Override
    public PigConfig getCluster( String clusterName )
    {
        return pluginDao.getInfo( PigConfig.PRODUCT_KEY, clusterName, PigConfig.class );
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        // not need
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {
        // not need
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String id )
    {
        LOG.info( String.format( "Pig environment event: Container destroyed: %s", id ) );
        List<PigConfig> clusterConfigs = getClusters();
        for ( final PigConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                LOG.info(
                        String.format( "Pig environment event: Target cluster: %s", clusterConfig.getClusterName() ) );

                if ( clusterConfig.getNodes().contains( id ) )
                {
                    LOG.info( String.format( "Pig environment event: Before: %s", clusterConfig ) );
                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getNodes() ) )
                    {
                        clusterConfig.getNodes().remove( id );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Pig environment event: After: %s", clusterConfig ) );
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
    public void onEnvironmentDestroyed( final String id )
    {
        LOG.info( String.format( "Cassandra environment event: Environment destroyed: %s", id ) );

        List<PigConfig> clusterConfigs = getClusters();
        for ( final PigConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( id ) )
            {
                LOG.info(
                        String.format( "Pig environment event: Target cluster: %s", clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Pig environment event: Cluster %s removed",
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
