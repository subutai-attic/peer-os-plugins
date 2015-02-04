/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mahout.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.mahout.api.Mahout;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;
import org.safehaus.subutai.plugin.mahout.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.mahout.impl.handler.NodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


public class MahoutImpl implements Mahout
{

    private static final Logger LOG = LoggerFactory.getLogger( MahoutImpl.class.getName() );
    private PluginDAO pluginDAO;
    private Commands commands;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private ExecutorService executor;
    private Hadoop hadoopManager;


    public MahoutImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager )
    {

        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public void setPluginDAO( final PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public Commands getCommands()
    {
        return commands;
    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


    public void setExecutor( final ExecutorService executor )
    {
        this.executor = executor;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public void init()
    {
        try
        {
            this.pluginDAO = new PluginDAO( null );
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        this.commands = new Commands();
        executor = Executors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    @Override
    public UUID installCluster( MahoutClusterConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );
        ClusterOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
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
    public UUID uninstallCluster( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        MahoutClusterConfig mahoutClusterConfig = getCluster( clusterName );
        ClusterOperationHandler operationHandler =
                new ClusterOperationHandler( this, mahoutClusterConfig, ClusterOperationType.DESTROY );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public List<MahoutClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( MahoutClusterConfig.PRODUCT_KEY, MahoutClusterConfig.class );
    }


    @Override
    public MahoutClusterConfig getCluster( String clusterName )
    {
        return pluginDAO.getInfo( MahoutClusterConfig.PRODUCT_KEY, clusterName, MahoutClusterConfig.class );
    }


    public UUID addNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.INSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID checkNode( final String clustername, final String lxchostname )
    {
        return null;
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final MahoutClusterConfig config, final TrackerOperation po )
    {
        return new OverHadoopSetupStrategy( this, config, po );
    }
}
