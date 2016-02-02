/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.mahout.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.mahout.api.Mahout;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;
import io.subutai.plugin.mahout.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.mahout.impl.handler.NodeOperationHandler;


public class MahoutImpl implements Mahout, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( MahoutImpl.class.getName() );
    private PluginDAO pluginDAO;
    private Commands commands;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private ExecutorService executor;
    private Hadoop hadoopManager;


    public MahoutImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager,
                       PluginDAO pluginDAO )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
        this.pluginDAO = pluginDAO;
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
        this.commands = new Commands();
        executor = SubutaiExecutors.newCachedThreadPool();
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


    @Override
    public void saveConfig( final MahoutClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !pluginDAO.saveInfo( MahoutClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final MahoutClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !pluginDAO.deleteInfo( MahoutClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
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
    public void onContainerDestroyed( final Environment environment, final String containerId )
    {
        LOG.info( String.format( "Mahout environment event: Container destroyed: %s", containerId ) );
        List<MahoutClusterConfig> clusters = getClusters();
        for ( MahoutClusterConfig clusterConfig : clusters )
        {
            if ( environment.getId().equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Mahout environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getNodes().contains( containerId ) )
                {
                    LOG.info( String.format( "Mahout environment event: Before: %s", clusterConfig ) );

                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getNodes() ) )
                    {
                        clusterConfig.getNodes().remove( containerId );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Mahout environment event: After: %s", clusterConfig ) );
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
    public void onEnvironmentDestroyed( final String envId )
    {
        LOG.info( String.format( "Mahout environment event: Environment destroyed: %s", envId ) );

        List<MahoutClusterConfig> clusters = getClusters();
        for ( MahoutClusterConfig clusterConfig : clusters )
        {
            if ( envId.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Mahout environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Mahout environment event: Cluster %s removed",
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
