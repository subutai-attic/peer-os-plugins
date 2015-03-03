package org.safehaus.subutai.plugin.accumulo.impl.handler;


import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import org.safehaus.subutai.plugin.accumulo.impl.AccumuloImpl;
import org.safehaus.subutai.plugin.accumulo.impl.AccumuloOverZkNHadoopSetupStrategy;
import org.safehaus.subutai.plugin.accumulo.impl.Commands;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * This class handles operations that are related to whole cluster.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<AccumuloImpl, AccumuloClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private AccumuloClusterConfig config;
    private HadoopClusterConfig hadoopConfig;

    private ExecutorService executor = Executors.newCachedThreadPool();


    public ClusterOperationHandler( final AccumuloImpl manager, final AccumuloClusterConfig config,
                                    final HadoopClusterConfig hadoopConfig,
                                    final ZookeeperClusterConfig zookeeperClusterConfig,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        this.hadoopConfig = hadoopConfig;
        trackerOperation = manager.getTracker().createTrackerOperation( AccumuloClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    public void run()
    {
        Preconditions.checkNotNull( config, "Configuration is null !!!" );
        switch ( operationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                destroyCluster();
                break;
            case START_ALL:
                runOperationOnContainers( operationType );
            case STOP_ALL:
                runOperationOnContainers( operationType );
            case STATUS_ALL:
                runOperationOnContainers( operationType );
                break;
        }
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg =
                    String.format( "Environment with id: %s doesn't exists.", config.getEnvironmentId().toString() );
            trackerOperation.addLogFailed( msg );
            LOG.error( msg, e );
            return;
        }
        CommandResult result = null;
        ContainerHost containerHost = null;
        try
        {
            containerHost = environment.getContainerHostById( config.getMasterNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg = String.format( "Container host with id: %s doesn't exists in environment: %s",
                    config.getMasterNode(), environment.getName() );
            trackerOperation.addLogFailed( msg );
            LOG.error( msg, e );
            return;
        }
        switch ( clusterOperationType )
        {
            case START_ALL:
                manager.getHadoopManager().startNameNode( hadoopConfig );
                result = executeCommand( containerHost, Commands.startCommand );
                break;
            case STOP_ALL:
                result = executeCommand( containerHost, Commands.stopCommand );
                break;
            case STATUS_ALL:
                for ( ContainerHost host : environment.getContainerHosts() )
                {
                    result = executeCommand( host, Commands.statusCommand );
                }
                break;
        }
        trackerOperation.addLogDone( "" );
    }


    @Override
    public void setupCluster()
    {
        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( hadoopConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg = String.format( "Environment with id: %s doesn't exists.",
                    hadoopConfig.getEnvironmentId().toString() );
            trackerOperation.addLogFailed( msg );
            LOG.error( msg, e );
            return;
        }
        AccumuloOverZkNHadoopSetupStrategy setupStrategyOverHadoop =
                new AccumuloOverZkNHadoopSetupStrategy( environment, config, hadoopConfig, trackerOperation, manager );
        try
        {
            setupStrategyOverHadoop.setup();
        }
        catch ( ClusterSetupException e )
        {
            trackerOperation.addLogFailed( "Error setting up accumulo cluster" );
            LOG.error( "Error setting up accumulo cluster", e );
        }
    }


    @Override
    public void destroyCluster()
    {
        AccumuloClusterConfig config = manager.getCluster( clusterName );
        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( hadoopConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg = String.format( "Environment with id: %s doesn't exists.",
                    hadoopConfig.getEnvironmentId().toString() );
            trackerOperation.addLogFailed( msg );
            LOG.error( msg, e );
            return;
        }
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        for ( UUID nodeId : config.getAllNodes() )
        {
            ContainerHost host = null;
            try
            {
                host = environment.getContainerHostById( nodeId );
            }
            catch ( ContainerHostNotFoundException e )
            {
                String msg = String.format( "Container host with id: %s doesn't exists in environment: %s",
                        nodeId.toString(), environment.getName() );
                trackerOperation.addLogFailed( msg );
                LOG.error( msg, e );
                continue;
            }
            CommandResult result;
            try
            {
                result = host.execute( new RequestBuilder(
                        Commands.uninstallCommand + Common.PACKAGE_PREFIX + AccumuloClusterConfig.PRODUCT_KEY
                                .toLowerCase() ) );
                if ( result.hasSucceeded() )
                {
                    trackerOperation.addLog(
                            AccumuloClusterConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                                    + " successfully." );
                }
                else
                {
                    trackerOperation.addLogFailed(
                            "Could not uninstall " + AccumuloClusterConfig.PRODUCT_KEY + " from node " + host
                                    .getHostname() );
                }
            }
            catch ( CommandException e )
            {
                LOG.error( "Error destroying cluster", e );
                trackerOperation.addLog( "Error destroying cluster. " + e.getMessage() );
            }
        }
        ContainerHost namenode = null;
        try
        {
            namenode = environment.getContainerHostById( hadoopConfig.getNameNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg = String.format( "Container host with id: %s doesn't exist in environment: %s",
                    hadoopConfig.getNameNode().toString(), environment.getName() );
            trackerOperation.addLogFailed( msg );
            LOG.error( msg, e );
            return;
        }
        CommandResult result = executeCommand( namenode, Commands.getRemoveAccumuloFromHFDSCommand() );

        if ( result.hasSucceeded() )
        {
            try
            {
                manager.unsubscribeFromAlerts( environment );
                trackerOperation.addLog( AccumuloClusterConfig.PRODUCT_KEY + " cluster info removed from HDFS." );
            }
            catch ( MonitorException e )
            {
                LOG.error( "Error removing subscription for environment.", e );
                trackerOperation.addLogFailed( "Error removing subscription for environment." );
            }
        }
        else
        {
            trackerOperation.addLogFailed( "Failed to remove " + config.getClusterName() + " cluster info from DB." );
        }
        manager.getPluginDAO().deleteInfo( AccumuloClusterConfig.PRODUCT_KEY, config.getClusterName() );
        trackerOperation.addLogDone( AccumuloClusterConfig.PRODUCT_KEY + " is uninstalled from all nodes" );
    }


    private CommandResult executeCommand( ContainerHost containerHost, RequestBuilder commandRequest )
    {
        CommandResult result = null;
        try
        {
            result = containerHost.execute( commandRequest );
        }
        catch ( CommandException e )
        {
            LOG.error( "Could not execute command correctly. ", e );
        }
        return result;
    }
}
