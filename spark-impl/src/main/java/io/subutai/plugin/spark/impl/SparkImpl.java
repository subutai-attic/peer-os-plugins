package io.subutai.plugin.spark.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.NodeType;
import io.subutai.plugin.common.api.OperationType;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.spark.api.Spark;
import io.subutai.plugin.spark.api.SparkClusterConfig;
import io.subutai.plugin.spark.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.spark.impl.handler.NodeOperationHandler;


public class SparkImpl extends SparkBase implements Spark, EnvironmentEventListener
{

    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );


    public SparkImpl( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager,
                      final Monitor monitor, final PluginDAO pluginDAO )
    {
        super( tracker, environmentManager, hadoopManager, monitor, pluginDAO );
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    @Override
    public UUID installCluster( final SparkClusterConfig config )
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

        SparkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.UNINSTALL );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public List<SparkClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( SparkClusterConfig.PRODUCT_KEY, SparkClusterConfig.class );
    }


    @Override
    public SparkClusterConfig getCluster( String clusterName )
    {
        return pluginDAO.getInfo( SparkClusterConfig.PRODUCT_KEY, clusterName, SparkClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String agentHostName )
    {
        return addSlaveNode( clusterName, agentHostName );
    }


    @Override
    public UUID addSlaveNode( final String clusterName, final String lxcHostname )
    {
        SparkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, lxcHostname, OperationType.INCLUDE, null );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID destroySlaveNode( final String clusterName, final String lxcHostname )
    {
        SparkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, lxcHostname, OperationType.EXCLUDE, null );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID changeMasterNode( final String clusterName, final String newMasterHostname, final boolean keepSlave )
    {
        SparkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, newMasterHostname, OperationType.CHANGE_MASTER,
                        keepSlave ? NodeType.SLAVE_NODE : NodeType.MASTER_NODE );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startNode( final String clusterName, final String lxcHostname, final boolean master )
    {
        SparkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, lxcHostname, OperationType.START,
                        master ? NodeType.MASTER_NODE : NodeType.SLAVE_NODE );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startCluster( final String clusterName )
    {
        SparkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.START_ALL );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopNode( final String clusterName, final String lxcHostname, final boolean master )
    {
        SparkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, lxcHostname, OperationType.STOP,
                        master ? NodeType.MASTER_NODE : NodeType.SLAVE_NODE );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopCluster( final String clusterName )
    {
        SparkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.STOP_ALL );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkCluster( final String clusterName )
    {
        SparkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.STATUS_ALL );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkNode( final String clusterName, final String lxcHostname, final boolean master )
    {
        SparkClusterConfig config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, config, lxcHostname, OperationType.STATUS,
                        master ? NodeType.MASTER_NODE : NodeType.SLAVE_NODE );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final TrackerOperation po,
                                                         final SparkClusterConfig clusterConfig,
                                                         final Environment environment )
    {

        Preconditions.checkNotNull( po, "Product operation is null" );
        Preconditions.checkNotNull( clusterConfig, "Spark cluster config is null" );

        return new SetupStrategyOverHadoop( po, this, clusterConfig, environment );
    }


    @Override
    public void saveConfig( final SparkClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( SparkClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final SparkClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !pluginDAO.deleteInfo( SparkClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
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
        LOG.info( String.format( "Spark environment event: Container destroyed: %s", containerId ) );

        List<SparkClusterConfig> clusters = getClusters();
        for ( SparkClusterConfig clusterConfig : clusters )
        {
            if ( environment.getId().equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Spark environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getAllNodesIds().contains( containerId ) )
                {
                    LOG.info( String.format( "Spark environment event: Before: %s", clusterConfig ) );

                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getSlaveIds() ) )
                    {
                        clusterConfig.getSlaveIds().remove( containerId );
                    }

                    if ( containerId.equals( clusterConfig.getMasterNodeId() ) )
                    {
                        clusterConfig.setMasterNodeId( null );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Spark environment event: After: %s", clusterConfig ) );
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
        LOG.info( String.format( "Spark environment event: Environment destroyed: %s", envId ) );

        List<SparkClusterConfig> clusters = getClusters();
        for ( SparkClusterConfig clusterConfig : clusters )
        {
            if ( envId.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Spark environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Spark environment event: Cluster %s removed",
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
