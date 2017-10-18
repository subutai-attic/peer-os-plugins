package io.subutai.plugin.hipi.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
import io.subutai.plugin.hipi.api.Hipi;
import io.subutai.plugin.hipi.api.HipiConfig;
import io.subutai.plugin.hipi.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.hipi.impl.handler.NodeOperationHandler;
import io.subutai.webui.api.WebuiModule;


public class HipiImpl implements Hipi, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( HipiImpl.class.getName() );
    private Tracker tracker;
    private PluginDAO pluginDao;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;
    private HipiWebModule webModule;


    public HipiImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager,
                     PluginDAO pluginDAO, HipiWebModule webModule )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
        this.pluginDao = pluginDAO;
        this.webModule = webModule;
    }


    public void setExecutor( ExecutorService executor )
    {
        this.executor = executor;
    }


    public Tracker getTracker()
    {
        return tracker;
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
        executor = Executors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    @Override
    public UUID installCluster( final HipiConfig config )
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
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public List<HipiConfig> getClusters()
    {
        return pluginDao.getInfo( HipiConfig.PRODUCT_KEY, HipiConfig.class );
    }


    @Override
    public HipiConfig getCluster( String clusterName )
    {
        return pluginDao.getInfo( HipiConfig.PRODUCT_KEY, clusterName, HipiConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.INCLUDE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroyNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.EXCLUDE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( HipiConfig config, TrackerOperation po )
    {

        return new HipiSetupStrategy( this, config, po );
    }


    @Override
    public void saveConfig( final HipiConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().saveInfo( HipiConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    public PluginDAO getPluginDao()
    {
        return pluginDao;
    }


    public void setPluginDao( PluginDAO pluginDao )
    {
        this.pluginDao = pluginDao;
    }


    @Override
    public void deleteConfig( final HipiConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDao().deleteInfo( HipiConfig.PRODUCT_KEY, config.getClusterName() ) )
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
        LOG.info( String.format( "Hipi environment event: Container destroyed: %s", uuid ) );
        List<HipiConfig> clusters = getClusters();
        for ( HipiConfig clusterConfig : clusters )
        {
            if ( environment.getId().equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info(
                        String.format( "Hipi environment event: Target cluster: %s", clusterConfig.getClusterName() ) );

                if ( clusterConfig.getNodes().contains( uuid ) )
                {
                    LOG.info( String.format( "Hipi environment event: Before: %s", clusterConfig ) );

                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getNodes() ) )
                    {
                        clusterConfig.getNodes().remove( uuid );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Hipi environment event: After: %s", clusterConfig ) );
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
        LOG.info( String.format( "Hipi environment event: Environment destroyed: %s", uuid ) );

        List<HipiConfig> clusters = getClusters();
        for ( HipiConfig clusterConfig : clusters )
        {
            if ( uuid.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info(
                        String.format( "Hipi environment event: Target cluster: %s", clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Hipi environment event: Cluster %s removed",
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


    @Override
    public WebuiModule getWebModule()
    {
        return webModule;
    }


    @Override
    public void setWebModule( final WebuiModule webModule )
    {
        this.webModule = ( HipiWebModule ) webModule;
    }
}
