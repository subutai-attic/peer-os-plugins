package io.subutai.plugin.ceph.impl;


import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.ceph.api.Ceph;
import io.subutai.plugin.ceph.api.CephClusterConfig;
import io.subutai.plugin.ceph.impl.handler.ClusterOperationHandler;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.PluginDAO;


public class CephImpl implements Ceph
{
    private static final Logger LOG = LoggerFactory.getLogger( CephImpl.class.getName() );

    private EnvironmentManager environmentManager;
    private Tracker tracker;
    private PluginDAO pluginDAO;
    private ExecutorService executor;


    public CephImpl( final Tracker tracker, final EnvironmentManager environmentManager, final PluginDAO pluginDAO )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.pluginDAO = pluginDAO;
    }


    public void init()
    {
        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        tracker = null;
        executor.shutdown();
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    @Override
    public UUID installCluster( final CephClusterConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public ClusterSetupStrategy getClusterSetupStrategy( final CephClusterConfig config, final TrackerOperation po )
    {
        Preconditions.checkNotNull( config, "Ceph cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation is null" );

        return new CephClusterSetupStrategy( config, po, this );
    }


    @Override
    public UUID uninstallCluster( final String s )
    {
        return null;
    }


    @Override
    public List<CephClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( CephClusterConfig.PRODUCT_KEY, CephClusterConfig.class );
    }


    @Override
    public CephClusterConfig getCluster( final String clusterName )
    {
        return pluginDAO.getInfo( CephClusterConfig.PRODUCT_KEY, clusterName, CephClusterConfig.class );
    }


    @Override
    public UUID addNode( final String s, final String s1 )
    {
        return null;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public void setPluginDAO( final PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


    public void setExecutor( final ExecutorService executor )
    {
        this.executor = executor;
    }
}
