package io.subutai.plugin.accumulo.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;
import io.subutai.plugin.accumulo.impl.AccumuloOverZkNHadoopSetupStrategy;
import io.subutai.plugin.accumulo.impl.Commands;
import io.subutai.plugin.accumulo.impl.Util;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


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
    private CommandUtil commandUtil;
    private Environment environment;


    public ClusterOperationHandler( final AccumuloImpl manager, final AccumuloClusterConfig config,
                                    final HadoopClusterConfig hadoopConfig,
                                    final ZookeeperClusterConfig zookeeperClusterConfig,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        this.hadoopConfig = hadoopConfig;
        this.commandUtil = new CommandUtil();

        try
        {
            this.environment = manager.getEnvironmentManager().loadEnvironment( hadoopConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
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
            case STOP_ALL:
            case STATUS_ALL:
                runOperationOnContainers( operationType );
                break;
        }
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        ContainerHost containerHost;
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
        List<CommandResult> commandResultList = new ArrayList<>();
        switch ( clusterOperationType )
        {
            case START_ALL:
                manager.getHadoopManager().startNameNode( hadoopConfig );
                manager.getZkManager().startAllNodes( config.getZookeeperClusterName() );
                Util.executeCommand( containerHost, Commands.startCommand );
                trackerOperation.addLogDone( "Cluster is started successfully" );
                break;
            case STOP_ALL:
                Util.executeCommand( containerHost, Commands.stopCommand );
                trackerOperation.addLogDone( "Cluster is stopped successfully" );
                break;
            case STATUS_ALL:
                for ( ContainerHost host : environment.getContainerHosts() )
                {
                    trackerOperation.addLog( "Node: " + host.getHostname() );
                    CommandResult result = Util.executeCommand( host, Commands.statusCommand );
                    commandResultList.add( result );
                    logResults( trackerOperation, result );
                }
                trackerOperation.addLogDone( "" );
                break;
        }
    }


    public void logResults( TrackerOperation po, CommandResult result )
    {
        po.addLog( result.getStdOut() );
        if ( po.getState() == OperationState.FAILED )
        {
            po.addLogFailed( "" );
        }
    }


    @Override
    public void setupCluster()
    {
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
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        // stop cluster before destroying cluster
        try
        {
            ContainerHost host = environment.getContainerHostById( config.getMasterNode() );
            try
            {
                CommandResult result = host.execute( Commands.statusCommand );
                if ( result.hasSucceeded() )
                {
                    String output[] = result.getStdOut().split( "\n" );
                    for ( String part : output )
                    {
                        if ( part.toLowerCase().contains( "master" ) )
                        {
                            if ( part.contains( "pid" ) )
                            {
                                UUID uuid = manager.stopCluster( clusterName );
                                LOG.info( "Stopping cluster before destroying it." );
                                Util.waitUntilOperationFinish( manager, uuid );
                            }
                        }
                    }
                }
            }
            catch ( CommandException e )
            {
                LOG.error( "Could not execute check status command successfully." );
                e.printStackTrace();
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Could not find container" );
            e.printStackTrace();
        }

        Set<Host> hostSet = Util.getHosts( config, environment );
        try
        {
            Map<Host, CommandResult> resultMap =
                    commandUtil.executeParallel( new RequestBuilder( Commands.uninstallCommand ), hostSet );
            if ( Util.isAllSuccessful( resultMap, hostSet ) )
            {
                trackerOperation.addLog( "Accumulo is uninstalled from all nodes successfully" );
            }
        }
        catch ( CommandException e )
        {
            LOG.error( "Could not uninstall Accumulo from all nodes !!!" );
            e.printStackTrace();
        }

        ContainerHost namenode;
        try
        {
            namenode = environment.getContainerHostById( hadoopConfig.getNameNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg = String.format( "Container host with id: %s doesn't exist in environment: %s",
                    hadoopConfig.getNameNode(), environment.getName() );
            trackerOperation.addLogFailed( msg );
            LOG.error( msg, e );
            return;
        }

        CommandResult result = Util.executeCommand( namenode, Commands.getRemoveAccumuloFromHFDSCommand() );

        if ( result.hasSucceeded() )
        {
            trackerOperation.addLog( AccumuloClusterConfig.PRODUCT_KEY + " cluster info removed from HDFS." );
//            try
//            {
//                manager.unsubscribeFromAlerts( environment );
//            }
//            catch ( MonitorException e )
//            {
//                LOG.error( "Error removing subscription for environment.", e );
//                trackerOperation.addLogFailed( "Error removing subscription for environment." );
//            }
        }
        else
        {
            trackerOperation.addLogFailed( "Failed to remove " + config.getClusterName() + " cluster info from HDFS." );
        }

        try
        {
            manager.deleteConfig( config );
            trackerOperation
                    .addLogDone( AccumuloClusterConfig.PRODUCT_KEY + " cluster information is removed from DB." );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }
    }
}
