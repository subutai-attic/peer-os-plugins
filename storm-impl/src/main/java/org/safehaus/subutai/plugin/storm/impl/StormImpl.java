package org.safehaus.subutai.plugin.storm.impl;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.core.env.api.EnvironmentEventListener;
import org.safehaus.subutai.core.metric.api.Monitor;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.storm.impl.alert.StormAlertListener;
import org.safehaus.subutai.plugin.storm.impl.handler.ConfigureEnvironmentClusterHandler;
import org.safehaus.subutai.plugin.storm.impl.handler.StormClusterOperationHandler;
import org.safehaus.subutai.plugin.storm.impl.handler.StormNodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class StormImpl extends StormBase implements EnvironmentEventListener
{
    private static final Logger LOG = LoggerFactory.getLogger( StormImpl.class.getName() );


    public StormImpl( Monitor monitor1 )
    {
        this.monitor = monitor1;
        this.stormAlertListener = new StormAlertListener( this );
        monitor.addAlertListener( stormAlertListener );
    }


    @Override
    public UUID installCluster( StormClusterConfiguration config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );
        AbstractOperationHandler operationHandler = new ConfigureEnvironmentClusterHandler( this, config );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( String clusterName )
    {
        AbstractOperationHandler h =
                new StormClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public List<StormClusterConfiguration> getClusters()
    {
        return pluginDAO.getInfo( StormClusterConfiguration.PRODUCT_NAME, StormClusterConfiguration.class );
    }


    @Override
    public StormClusterConfiguration getCluster( String clusterName )
    {
        return pluginDAO
                .getInfo( StormClusterConfiguration.PRODUCT_NAME, clusterName, StormClusterConfiguration.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String agentHostName )
    {
        return null;
    }


    @Override
    public UUID startNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.START );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID startAll( String clusterName )
    {
        AbstractOperationHandler h =
                new StormClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.START_ALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID stopNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.STOP );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID stopAll( String clusterName )
    {
        AbstractOperationHandler h =
                new StormClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.STOP_ALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID checkNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.STATUS );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID restartNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.RESTART );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID addNode( String clusterName )
    {
        StormClusterConfiguration zookeeperClusterConfig = getCluster( clusterName );

        AbstractOperationHandler h =
                new StormClusterOperationHandler( this, zookeeperClusterConfig, ClusterOperationType.ADD );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID destroyNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.DESTROY );
        executor.execute( h );
        return h.getTrackerId();
    }


    public UUID removeCluster( final String clusterName )
    {
        StormClusterConfiguration config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new StormClusterOperationHandler( this, config, ClusterOperationType.REMOVE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( StormClusterConfiguration config, TrackerOperation po )
    {
        return new StormSetupStrategyDefault( this, config, po );
    }


    @Override
    public void saveConfig( final StormClusterConfiguration config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( StormClusterConfiguration.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final StormClusterConfiguration config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().deleteInfo( StormClusterConfiguration.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        LOG.info( "Environment created: " + environment.getName() );
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<ContainerHost> set )
    {
        List<String> containerNames = new ArrayList<>();
        for ( final ContainerHost containerHost : set )
        {
            containerNames.add( containerHost.getHostname() );
        }
        LOG.info( String.format( "Environment: %s has been grown with containers: %s", environment.getName(),
                containerNames.toString() ) );
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final UUID containerHostId )
    {
        LOG.info( String.format( "Storm environment event: Container destroyed: %s", containerHostId ) );
        List<StormClusterConfiguration> clusterConfigs = getClusters();
        for ( final StormClusterConfiguration clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                LOG.info( String.format( "Storm environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getAllNodes().contains( containerHostId ) )
                {
                    LOG.info( String.format( "Storm environment event: Before: %s", clusterConfig ) );
                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getSupervisors() ) )
                    {
                        clusterConfig.getSupervisors().remove( containerHostId );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Storm environment event: After: %s", clusterConfig ) );
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
    public void onEnvironmentDestroyed( final UUID uuid )
    {
        LOG.info( String.format( "Storm environment event: Environment destroyed: %s", uuid ) );

        List<StormClusterConfiguration> clusterConfigs = getClusters();
        for ( final StormClusterConfiguration clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( uuid ) )
            {
                LOG.info( String.format( "Storm environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Storm environment event: Cluster removed",
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
