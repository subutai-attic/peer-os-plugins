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

import javax.sql.DataSource;

import org.safehaus.subutai.common.protocol.AbstractOperationHandler;
import org.safehaus.subutai.common.protocol.ClusterSetupStrategy;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.mahout.api.Mahout;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;
import org.safehaus.subutai.plugin.mahout.api.SetupType;
import org.safehaus.subutai.plugin.mahout.impl.dao.PluginDAO;
import org.safehaus.subutai.plugin.mahout.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.mahout.impl.handler.NodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class MahoutImpl implements Mahout
{

    private static final Logger LOG = LoggerFactory.getLogger( MahoutImpl.class.getName() );
    private PluginDAO pluginDAO;
    private Commands commands;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private ExecutorService executor;
    private Hadoop hadoopManager;
    private DataSource dataSource;

    public MahoutImpl( final DataSource dataSource, final Tracker tracker, final EnvironmentManager environmentManager,
                       final Hadoop hadoopManager )
    {
        this.dataSource = dataSource;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
    }


    public MahoutImpl( DataSource dataSource )
    {
        this.dataSource = dataSource;
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
            this.pluginDAO = new PluginDAO( dataSource );
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
    public UUID installCluster( final MahoutClusterConfig config, final HadoopClusterConfig hadoopConfig )
    {
        ClusterOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        operationHandler.setHadoopConfig( hadoopConfig );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
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
    public UUID uninstallCluster( final MahoutClusterConfig config )
    {
        ClusterOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.DESTROY );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstalllNode( final String clusterName, final String lxcHostname )
    {
        AbstractOperationHandler h =
                new NodeOperationHandler( this, clusterName, lxcHostname, NodeOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }

    @Override
    public UUID uninstallCluster( final String clusterName )
    {
        return null;
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
    public UUID stopCluster( final String clusterName )
    {
        return null;
    }


    @Override
    public UUID startCluster( final String clusterName )
    {
        return null;
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final Environment environment,
                                                         final MahoutClusterConfig config, final TrackerOperation po )
    {
        if ( config.getSetupType() == SetupType.OVER_HADOOP )
        {
            return new OverHadoopSetupStrategy( this, config, po, environment );
        }
        else if ( config.getSetupType() == SetupType.WITH_HADOOP )
        {
//            WithHadoopSetupStrategy s = new WithHadoopSetupStrategy( this, config, po );
//            s.setEnvironment( environment );
//            return s;
        }
        return null;
    }

}
