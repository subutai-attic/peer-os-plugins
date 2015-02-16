package org.safehaus.subutai.plugin.storm.impl;


import java.util.List;
import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.storm.impl.handler.ConfigureEnvironmentClusterHandler;
import org.safehaus.subutai.plugin.storm.impl.handler.StormClusterOperationHandler;
import org.safehaus.subutai.plugin.storm.impl.handler.StormNodeOperationHandler;

import com.google.common.base.Preconditions;


public class StormImpl extends StormBase
{

    public StormImpl()
    {

    }


    @Override
    public UUID installCluster( StormClusterConfiguration config )
    {
        AbstractOperationHandler h = new StormClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( h );
        return h.getTrackerId();
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
    public UUID checkNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.STATUS );
        executor.execute( h );
        return h.getTrackerId();
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
    public UUID stopNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.STOP );
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


    public UUID configureEnvironmentCluster( final StormClusterConfiguration config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );
        AbstractOperationHandler operationHandler = new ConfigureEnvironmentClusterHandler( this, config );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }
}
