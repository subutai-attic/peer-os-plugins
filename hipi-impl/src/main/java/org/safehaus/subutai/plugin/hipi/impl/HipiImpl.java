package org.safehaus.subutai.plugin.hipi.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hipi.api.Hipi;
import org.safehaus.subutai.plugin.hipi.api.HipiConfig;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.hipi.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.hipi.impl.handler.NodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class HipiImpl implements Hipi
{

    private static final Logger LOG = LoggerFactory.getLogger( HipiImpl.class.getName() );
    private Tracker tracker;
    private PluginDAO pluginDao;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;
    //private DataSource dataSource;
    private Hadoop hadoopManager;


    public HipiImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
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
}
