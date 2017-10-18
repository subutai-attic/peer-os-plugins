package io.subutai.plugin.nutch.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.nutch.api.Nutch;
import io.subutai.plugin.nutch.api.NutchConfig;
import io.subutai.plugin.nutch.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.nutch.impl.handler.NodeOperationHandler;


public class NutchImpl implements Nutch, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( NutchImpl.class.getName() );
    private Tracker tracker;
    private Hadoop hadoopManager;
    private ExecutorService executor;
    private PluginDAO pluginDao;
    private EnvironmentManager environmentManager;


    public NutchImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager,
                      PluginDAO pluginDAO )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
        this.pluginDao = pluginDAO;
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


    public void init()
    {
        executor = Executors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    @Override
    public UUID installCluster( final NutchConfig config )
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
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ) );

        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.DESTROY );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public List<NutchConfig> getClusters()
    {
        return pluginDao.getInfo( NutchConfig.PRODUCT_KEY, NutchConfig.class );
    }


    @Override
    public NutchConfig getCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ) );

        return pluginDao.getInfo( NutchConfig.PRODUCT_KEY, clusterName, NutchConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ) );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ) );

        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroyNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ) );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ) );

        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( NutchConfig config, TrackerOperation trackerOperation )
    {
        Preconditions.checkNotNull( config );
        Preconditions.checkNotNull( trackerOperation );

        return new NutchSetupStrategy( this, config, trackerOperation );
    }


    @Override
    public void saveConfig( final NutchConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().saveInfo( NutchConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final NutchConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().deleteInfo( NutchConfig.PRODUCT_KEY, config.getClusterName() ) )
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
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {
        //not needed
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String uuid )
    {
        LOG.info( String.format( "Nutch environment event: Container destroyed: %s", uuid ) );
        List<NutchConfig> clusters = getClusters();
        for ( NutchConfig clusterConfig : clusters )
        {
            if ( environment.getId().equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Nutch environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getNodes().contains( uuid ) )
                {
                    LOG.info( String.format( "Nutch environment event: Before: %s", clusterConfig ) );

                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getNodes() ) )
                    {
                        clusterConfig.getNodes().remove( uuid );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Nutch environment event: After: %s", clusterConfig ) );
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
    public void onEnvironmentDestroyed( final String uuid )
    {
        LOG.info( String.format( "Nutch environment event: Environment destroyed: %s", uuid ) );

        List<NutchConfig> clusters = getClusters();
        for ( NutchConfig clusterConfig : clusters )
        {
            if ( uuid.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Nutch environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Nutch environment event: Cluster %s removed",
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


    @Override
    public void onContainerStarted( final Environment environment, final String s )
    {

    }


    @Override
    public void onContainerStopped( final Environment environment, final String s )
    {

    }
}
