package org.safehaus.subutai.plugin.hive.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentEventListener;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hive.api.Hive;
import org.safehaus.subutai.plugin.hive.api.HiveConfig;
import org.safehaus.subutai.plugin.hive.impl.handler.CheckInstallHandler;
import org.safehaus.subutai.plugin.hive.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.hive.impl.handler.NodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HiveImpl implements Hive, EnvironmentEventListener
{
    private static final Logger LOG = LoggerFactory.getLogger( HiveImpl.class.getName() );
    private Tracker tracker;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;
    private PluginDAO pluginDAO;
    private Hadoop hadoopManager;


    public HiveImpl( final Tracker tracker, final EnvironmentManager environmentManager,final Hadoop hadoopManager )
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
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
        ContainerHost containerHost = null;
        try
        {
            containerHost = environmentManager.findEnvironment(
                    hadoopManager.getCluster( clusterName ).getEnvironmentId() )
                              .getContainerHostByHostname( hostname );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
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
    public void onEnvironmentCreated( final Environment environment )
    {
        LOG.info( "Environment created " + environment.toString() );

    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<ContainerHost> set )
    {
        String hostNames = "";
        for ( final ContainerHost containerHost : set )
        {
            hostNames += containerHost.getHostname() + "; ";
        }
        LOG.info( String.format( "Environment: %s bred with containers: %s", environment.getName(), hostNames ) );

    }


    @Override
    public void onContainerDestroyed( final Environment environment, final UUID uuid )
    {
        List<HiveConfig> clusterConfigs = getClusters();
        for ( final HiveConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                if ( clusterConfig.getAllNodes().contains( uuid ) )
                {
                    clusterConfig.removeNode( uuid );
                    getPluginDAO()
                            .saveInfo( HiveConfig.PRODUCT_KEY, clusterConfig.getClusterName(), clusterConfig );
                    LOG.info( String.format( "Container host: %s removed from cluster: %s with environment id: %s",
                            uuid.toString(), clusterConfig.getClusterName(),
                            clusterConfig.getEnvironmentId().toString() ) );
                }
            }
        }

    }


    @Override
    public void onEnvironmentDestroyed( final UUID uuid )
    {
        List<HiveConfig> clusterConfigs = getClusters();
        for ( final HiveConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( uuid ) )
            {
                LOG.info(
                        String.format( "Hadoop cluster: %s destroyed in environment %s", clusterConfig.getClusterName(),
                                uuid.toString() ) );
                getPluginDAO().deleteInfo( HiveConfig.PRODUCT_KEY, clusterConfig.getClusterName() );
            }
        }

    }
}
