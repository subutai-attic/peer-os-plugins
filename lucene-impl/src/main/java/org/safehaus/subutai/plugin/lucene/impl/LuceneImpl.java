package org.safehaus.subutai.plugin.lucene.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.core.env.api.EnvironmentEventListener;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.lucene.api.Lucene;
import org.safehaus.subutai.plugin.lucene.api.LuceneConfig;
import org.safehaus.subutai.plugin.lucene.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.lucene.impl.handler.NodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


public class LuceneImpl implements Lucene, EnvironmentEventListener
{
    private static final Logger LOG = LoggerFactory.getLogger( LuceneImpl.class.getName() );
    protected Commands commands;
    private Tracker tracker;
    private Hadoop hadoopManager;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;
    private PluginDAO pluginDao;


    public LuceneImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
    }


    public void setPluginDao( PluginDAO pluginDao )
    {
        this.pluginDao = pluginDao;
    }


    public void setExecutor(ExecutorService executor)
    {
        this.executor = executor;
    }

    public Hadoop getHadoopManager()
    {
        return hadoopManager;
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


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
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
        executor = Executors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    @Override
    public UUID installCluster( final LuceneConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );

        ClusterOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Invalid cluster name" );

        ClusterOperationHandler operationHandler =
                new ClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.DESTROY );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public List<LuceneConfig> getClusters()
    {
        return pluginDao.getInfo( LuceneConfig.PRODUCT_KEY, LuceneConfig.class );
    }


    @Override
    public LuceneConfig getCluster( String clusterName )
    {
        return pluginDao.getInfo( LuceneConfig.PRODUCT_KEY, clusterName, LuceneConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.INSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID uninstallNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( Environment env, LuceneConfig config, TrackerOperation po )
    {
        return new OverHadoopSetupStrategy( this, config, po, env );
    }


    @Override
    public void saveConfig( final LuceneConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().saveInfo( LuceneConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final LuceneConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().deleteInfo( LuceneConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
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
        LOG.info( String.format( "Lucene environment event: Container destroyed: %s", uuid ) );
        List<LuceneConfig> clusters = getClusters();
        for ( LuceneConfig clusterConfig : clusters )
        {
            if ( environment.getId().equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Lucene environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getNodes().contains( uuid ) )
                {
                    LOG.info( String.format( "Lucene environment event: Before: %s", clusterConfig ) );

                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getNodes() ) )
                    {
                        clusterConfig.getNodes().remove( uuid );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Lucene environment event: After: %s", clusterConfig ) );
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
        LOG.info( String.format( "Lucene environment event: Environment destroyed: %s", uuid ) );

        List<LuceneConfig> clusters = getClusters();
        for ( LuceneConfig clusterConfig : clusters )
        {
            if ( uuid.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Lucene environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Lucene environment event: Cluster %s removed",
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
