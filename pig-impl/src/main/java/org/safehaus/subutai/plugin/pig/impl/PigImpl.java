package org.safehaus.subutai.plugin.pig.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.pig.api.Pig;
import org.safehaus.subutai.plugin.pig.api.PigConfig;
import org.safehaus.subutai.plugin.pig.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.pig.impl.handler.NodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PigImpl implements Pig
{
    private static final Logger LOG = LoggerFactory.getLogger( PigImpl.class.getName() );
    private Tracker tracker;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;
    private PluginDAO pluginDao;


    public PigImpl( final Tracker tracker, final EnvironmentManager environmentManager,
                    final Hadoop hadoopManager )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
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
        return null;
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
    public List<PigConfig> getClusters()
    {
        return pluginDao.getInfo( PigConfig.PRODUCT_KEY, PigConfig.class );
    }


    @Override
    public PigConfig getCluster( String clusterName )
    {
        return pluginDao.getInfo( PigConfig.PRODUCT_KEY, clusterName, PigConfig.class );
    }
}
