package io.subutai.plugin.hive.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
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
import io.subutai.plugin.hive.api.Hive;
import io.subutai.plugin.hive.api.HiveConfig;
import io.subutai.plugin.hive.impl.handler.CheckInstallHandler;
import io.subutai.plugin.hive.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.hive.impl.handler.NodeOperationHandler;


public class HiveImpl implements Hive, EnvironmentEventListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger( HiveImpl.class.getName() );
    private Tracker tracker;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;
    private PluginDAO pluginDAO;
    private Hadoop hadoopManager;


    public HiveImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager,
                     PluginDAO pluginDAO )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
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


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    @Override
    public UUID installCluster( final HiveConfig config )
    {
        AbstractOperationHandler h = new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final String hiveClusterName )
    {
        HiveConfig config = getCluster( hiveClusterName );
        AbstractOperationHandler h = new ClusterOperationHandler( this, config, ClusterOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public List<HiveConfig> getClusters()
    {
        return pluginDAO.getInfo( HiveConfig.PRODUCT_KEY, HiveConfig.class );
    }


    @Override
    public HiveConfig getCluster( String clusterName )
    {
        return pluginDAO.getInfo( HiveConfig.PRODUCT_KEY, clusterName, HiveConfig.class );
    }


    @Override
    public UUID addNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.INSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID statusCheck( String clusterName, String hostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.STATUS );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID startNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.START );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID stopNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.STOP );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID restartNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h = new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.RESTART );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID uninstallNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, hostname, NodeOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public boolean isInstalled( String clusterName, String hostname )
    {
        EnvironmentContainerHost containerHost = null;
        try
        {
            containerHost =
                    environmentManager.loadEnvironment( hadoopManager.getCluster( clusterName ).getEnvironmentId() )
                                      .getContainerHostByHostname( hostname );
        }
        catch ( ContainerHostNotFoundException e )
        {
            logExceptionWithMessage( String.format( "Container hosts with id: %s not found", hostname ), e );
        }
        catch ( EnvironmentNotFoundException e )
        {
            logExceptionWithMessage( "Couldn't retrieve environment", e );
        }
        CheckInstallHandler h = new CheckInstallHandler( containerHost );
        return h.check();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final HiveConfig config,
                                                         final TrackerOperation trackerOperation )
    {
        return new HiveSetupStrategy( this, config, trackerOperation );
    }


    @Override
    public void saveConfig( final HiveConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( HiveConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final HiveConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().deleteInfo( HiveConfig.PRODUCT_KEY, config.getClusterName() ) )
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
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {
        // not need
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String uuid )
    {
        LOGGER.info( String.format( "Hive environment event: Container destroyed: %s", uuid ) );
        List<HiveConfig> clusterConfigs = getClusters();
        for ( final HiveConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                LOGGER.info(
                        String.format( "Hive environment event: Target cluster: %s", clusterConfig.getClusterName() ) );

                if ( clusterConfig.getAllNodes().contains( uuid ) )
                {
                    LOGGER.info( String.format( "Hive environment event: Before: %s", clusterConfig ) );
                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getClients() ) )
                    {
                        clusterConfig.getClients().remove( uuid );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOGGER.info( String.format( "Hive environment event: After: %s", clusterConfig ) );
                    }
                    catch ( ClusterException e )
                    {
                        LOGGER.error( "Error updating cluster config", e );
                    }
                    break;
                }
            }
        }
    }


    @Override
    public void onEnvironmentDestroyed( final String uuid )
    {
        LOGGER.info( String.format( "Hive environment event: Environment destroyed: %s", uuid ) );

        List<HiveConfig> clusterConfigs = getClusters();
        for ( final HiveConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( uuid ) )
            {
                LOGGER.info(
                        String.format( "Hive environment event: Target cluster: %s", clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOGGER.info( String.format( "Hive environment event: Cluster %s removed",
                            clusterConfig.getClusterName() ) );
                }
                catch ( ClusterException e )
                {
                    LOGGER.error( "Error deleting cluster config", e );
                }
                break;
            }
        }
    }


    private void logExceptionWithMessage( String message, Exception e )
    {
        LOGGER.error( message, e );
    }
}
