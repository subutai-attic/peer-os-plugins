package io.subutai.plugin.cassandra.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentModificationException;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.environment.NodeGroup;
import io.subutai.common.environment.Topology;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.protocol.PlacementStrategy;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.core.peer.api.LocalPeer;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.plugin.cassandra.impl.ClusterConfiguration;
import io.subutai.plugin.cassandra.impl.Commands;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * This class handles operations that are related to whole cluster.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<CassandraImpl, CassandraClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private CassandraClusterConfig config;
    private CommandUtil commandUtil;


    public ClusterOperationHandler( final CassandraImpl manager, final CassandraClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( CassandraClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
        commandUtil = new CommandUtil();
    }


    public void run()
    {
        Preconditions.checkNotNull( config, "Configuration is null !!!" );
        switch ( operationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case START_ALL:
                startNStopCluster( config, ClusterOperationType.START_ALL );
                break;
            case STOP_ALL:
                startNStopCluster( config, ClusterOperationType.STOP_ALL );
                break;
            case STATUS_ALL:
                runOperationOnContainers( operationType );
                break;
            case ADD:
                addNode();
                break;
            case REMOVE:
                destroyCluster();
                break;
        }
    }


    private void startNStopCluster( CassandraClusterConfig config, ClusterOperationType type )
    {
        for ( UUID uuid : config.getNodes() )
        {
            try
            {
                ContainerHost host = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() )
                                            .getContainerHostById( uuid );
                switch ( type )
                {
                    case START_ALL:
                        host.execute( new RequestBuilder( Commands.startCommand ) );
                        break;
                    case STOP_ALL:
                        host.execute( new RequestBuilder( Commands.stopCommand ) );
                        break;
                }
            }
            catch ( EnvironmentNotFoundException | ContainerHostNotFoundException | CommandException e )
            {
                trackerOperation.addLogFailed( "Failed to %s " +
                        ( type == ClusterOperationType.START_ALL ? "start" : "stop" )  + config.getClusterName() + " cluster" );
                e.printStackTrace();
            }
        }
        trackerOperation.addLogDone( String.format( "%s cluster %s successfully", config.getClusterName(),
                type == ClusterOperationType.START_ALL ? "started" : "stopped" ) );
    }


    public void addNode()
    {
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        NodeGroup nodeGroup = new NodeGroup( CassandraClusterConfig.PRODUCT_NAME, config.getTEMPLATE_NAME(),
                1, 0, 0, new PlacementStrategy( "ROUND_ROBIN" ) );

        Topology topology = new Topology();

        topology.addNodeGroupPlacement( localPeer, nodeGroup );
        try
        {
            Set<ContainerHost> newNodeSet;
            try
            {
                newNodeSet = environmentManager.growEnvironment( config.getEnvironmentId(), topology, false );
            }
            catch ( EnvironmentNotFoundException | EnvironmentModificationException e )
            {
                LOG.error( "Could not add new node(s) to environment." );
                throw new ClusterException( e );
            }

            ContainerHost newNode = newNodeSet.iterator().next();

            config.getNodes().add( newNode.getId() );

            manager.saveConfig( config );

            ClusterConfiguration configurator = new ClusterConfiguration( trackerOperation, manager );
            Environment environment;
            try
            {
                environment = environmentManager.findEnvironment( config.getEnvironmentId() );
                configurator
                        .configureCluster( config, environmentManager.findEnvironment( config.getEnvironmentId() ) );

                // check if one of seeds in cassandra cluster is already running,
                // then newly added node should be started automatically.
                try
                {
                    ContainerHost coordinator = environment.getContainerHostById( config.getSeedNodes().iterator().next() );
                    RequestBuilder checkMasterIsRunning = new RequestBuilder( Commands.statusCommand );
                    CommandResult result = null;
                    try
                    {
                        result = commandUtil.execute( checkMasterIsRunning, coordinator );
                        if ( result.hasSucceeded() ){
                            if ( result.getStdOut().toLowerCase().contains( "pid" ) ){
                                commandUtil.execute( new RequestBuilder( Commands.startCommand ), newNode );
                            }
                        }
                    }
                    catch ( CommandException e )
                    {
                        LOG.error( "Could not check if Cassandra is running on one of the seeds nodes" );
                        e.printStackTrace();
                    }

                }
                catch ( ContainerHostNotFoundException e )
                {
                    e.printStackTrace();
                }

            }
            catch ( EnvironmentNotFoundException | ClusterConfigurationException e )
            {
                LOG.error( "Could not find environment with id {} ", config.getEnvironmentId() );
                throw new ClusterException( e );
            }

            //subscribe to alerts
            try
            {
                manager.subscribeToAlerts( newNode );
            }
            catch ( MonitorException e )
            {
                throw new ClusterException( "Failed to subscribe to alerts: " + e.getMessage() );
            }
            trackerOperation.addLogDone( "Node added" );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "failed to add node:  %s", e ) );
        }
    }


    @Override
    public void setupCluster()
    {
        trackerOperation.addLog( "Setting up cluster..." );

        try
        {
            Environment env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );

            new ClusterConfiguration( trackerOperation, manager ).configureCluster( config, env );
        }
        catch ( EnvironmentNotFoundException | ClusterConfigurationException e )
        {
            trackerOperation
                    .addLogFailed( String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() ) );
        }
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            CommandResult result = null;
            List<CommandResult> commandResultList = new ArrayList<>();
            switch ( clusterOperationType )
            {
                case START_ALL:
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        result = executeCommand( containerHost, Commands.startCommand );

                    }
                    NodeOperationHandler.logResults( trackerOperation, result );
                    break;
                case STOP_ALL:
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        result = executeCommand( containerHost, Commands.stopCommand );

                    }
                    NodeOperationHandler.logResults( trackerOperation, result );
                    break;
                case STATUS_ALL:
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        //executeCommand( containerHost, Commands.statusCommand );
                        commandResultList.add( executeCommand( containerHost, Commands.statusCommand ) );
                    }
                    logResults( trackerOperation, commandResultList );
                    break;
            }

//            NodeOperationHandler.logResults( trackerOperation, result );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found" );
        }
    }

    public void logResults( TrackerOperation po, List<CommandResult> commandResultList )
    {
        Preconditions.checkNotNull( commandResultList );
        for ( CommandResult commandResult : commandResultList )
        {
            po.addLog( commandResult.getStdOut() );
        }
        if ( po.getState() == OperationState.FAILED )
        {
            po.addLogFailed( "" );
        }
        else
        {
            po.addLogDone( "" );
        }
    }


    private CommandResult executeCommand( ContainerHost containerHost, String command )
    {
        CommandResult result = null;
        try
        {
            result = containerHost.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            LOG.error( "Could not execute command correctly. ", command );
            e.printStackTrace();
        }
        return result;
    }


    @Override
    public void destroyCluster()
    {
        CassandraClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }

        try
        {
            manager.unsubscribeFromAlerts( environment );
        }
        catch ( MonitorException e )
        {
            trackerOperation.addLog( String.format( "Failed to unsubscribe from alerts: %s", e.getMessage() ) );
        }
        try
        {
            manager.deleteConfig( config );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( "Failed to delete cluster information from database" );
            return;
        }
        trackerOperation.addLogDone( "Cluster removed from database" );
    }
}
