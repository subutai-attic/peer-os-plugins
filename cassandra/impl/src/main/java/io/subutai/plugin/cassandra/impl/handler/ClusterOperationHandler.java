package io.subutai.plugin.cassandra.impl.handler;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Blueprint;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentModificationException;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.environment.NodeSchema;
import io.subutai.common.environment.Topology;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.PeerException;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.strategy.api.ContainerPlacementStrategy;
import io.subutai.core.strategy.api.RoundRobinStrategy;
import io.subutai.core.strategy.api.StrategyException;
import io.subutai.core.template.api.TemplateManager;
import io.subutai.hub.share.quota.ContainerQuota;
import io.subutai.hub.share.quota.ContainerSize;
import io.subutai.hub.share.resource.PeerGroupResources;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.plugin.cassandra.impl.ClusterConfiguration;
import io.subutai.plugin.cassandra.impl.Commands;


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
    private TemplateManager templateManager;


    public ClusterOperationHandler( final CassandraImpl manager, final TemplateManager templateManager,
                                    final CassandraClusterConfig config, final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        this.templateManager = templateManager;
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
        for ( String id : config.getSeedNodes() )
        {
            try
            {
                EnvironmentContainerHost host =
                        manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                               .getContainerHostByHostname( id );
                switch ( type )
                {
                    case START_ALL:
                        host.execute( new RequestBuilder( Commands.START_COMMAND ) );
                        break;
                    case STOP_ALL:
                        host.execute( new RequestBuilder( Commands.STOP_COMMAND ) );
                        break;
                }
            }
            catch ( EnvironmentNotFoundException | ContainerHostNotFoundException | CommandException e )
            {
                trackerOperation.addLogFailed(
                        "Failed to %s " + ( type == ClusterOperationType.START_ALL ? "start" : "stop" ) + config
                                .getClusterName() + " cluster" );
                e.printStackTrace();
            }
        }
        trackerOperation.addLogDone( String.format( "%s cluster %s successfully", config.getClusterName(),
                type == ClusterOperationType.START_ALL ? "started" : "stopped" ) );
    }


    public void addNode()
    {
        try
        {
            EnvironmentManager environmentManager = manager.getEnvironmentManager();
            Environment env = environmentManager.loadEnvironment( config.getEnvironmentId() );

            List<Integer> containersIndex = Lists.newArrayList();

            for ( final EnvironmentContainerHost containerHost : env.getContainerHosts() )
            {
                String numbers = containerHost.getContainerName().replace( "Container", "" ).trim();
                String contId = numbers.split( "-" )[0];
                containersIndex.add( Integer.parseInt( contId ) );
            }

            Set<EnvironmentContainerHost> newNodeSet = null;
            try
            {
                String containerName = "Container" + String.valueOf( Collections.max( containersIndex ) + 1 );
                NodeSchema node = new NodeSchema( containerName, new ContainerQuota( ContainerSize.HUGE ),
                        CassandraClusterConfig.TEMPLATE_NAME,
                        templateManager.getTemplateByName( CassandraClusterConfig.TEMPLATE_NAME ).getId() );
                List<NodeSchema> nodes = new ArrayList<>();
                nodes.add( node );

                Blueprint blueprint = new Blueprint(
                        manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() ).getName(), nodes );

                ContainerPlacementStrategy strategy =
                        manager.getStrategyManager().findStrategyById( RoundRobinStrategy.ID );
                PeerGroupResources peerGroupResources = manager.getPeerManager().getPeerGroupResources();
                Map<ContainerSize, ContainerQuota> quotas = manager.getQuotaManager().getDefaultQuotas();

                Topology topology =
                        strategy.distribute( blueprint.getName(), blueprint.getNodes(), peerGroupResources, quotas );

                newNodeSet = environmentManager.growEnvironment( config.getEnvironmentId(), topology, false );
            }
            catch ( EnvironmentNotFoundException | EnvironmentModificationException | PeerException |
                    StrategyException e )
            {
                trackerOperation.addLogFailed(
                        String.format( "Could not add new node(s) to environment: %s", config.getEnvironmentId() ) );
                LOG.error( String.format( "Could not add new node(s) to environment: %s", config.getEnvironmentId() ),
                        e );
                throw new ClusterException( e );
            }

            EnvironmentContainerHost newNode = newNodeSet.iterator().next();

            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );

            config.getSeedNodes().add( newNode.getHostname() );

            ClusterConfiguration configurator = new ClusterConfiguration( trackerOperation, manager );
            try
            {
                configurator.addNode( config, environment, newNode );
            }
            catch ( ClusterConfigurationException e )
            {
                trackerOperation.addLogFailed(
                        String.format( "Error during reconfiguration after adding node %s from cluster: %s",
                                newNode.getHostname(), config.getClusterName() ) );
                LOG.error( String.format( "Error during reconfiguration after adding node %s from cluster: %s",
                        newNode.getHostname(), config.getClusterName() ), e );
                e.printStackTrace();
            }

            manager.saveConfig( config );
            trackerOperation.addLogDone( "Node added successfully" );
            LOG.info( "Node added successfully" );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "failed to add node:  %s", e ) );
            e.printStackTrace();
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "failed to find environment:  %s", e ) );
            e.printStackTrace();
        }
    }


    @Override
    public void setupCluster()
    {
        trackerOperation.addLog( "Setting up cluster..." );

        try
        {
            Environment env = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );

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
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            CommandResult result = null;
            List<CommandResult> commandResultList = new ArrayList<>();
            switch ( clusterOperationType )
            {
                case START_ALL:
                    for ( EnvironmentContainerHost containerHost : environment.getContainerHosts() )
                    {
                        result = executeCommand( containerHost, Commands.START_COMMAND );
                    }
                    NodeOperationHandler.logResults( trackerOperation, result );
                    break;
                case STOP_ALL:
                    for ( EnvironmentContainerHost containerHost : environment.getContainerHosts() )
                    {
                        result = executeCommand( containerHost, Commands.STOP_COMMAND );
                    }
                    NodeOperationHandler.logResults( trackerOperation, result );
                    break;
                case STATUS_ALL:
                    for ( EnvironmentContainerHost containerHost : environment.getContainerHosts() )
                    {
                        //executeCommand( containerHost, Commands.statusCommand );
                        commandResultList.add( executeCommand( containerHost, Commands.STATUS_COMMAND ) );
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


    private CommandResult executeCommand( EnvironmentContainerHost containerHost, String command )
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

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            new ClusterConfiguration( trackerOperation, manager ).deleteClusterConfiguration( config, environment );
        }
        catch ( EnvironmentNotFoundException | ClusterConfigurationException e )
        {
            trackerOperation.addLogFailed( "Environment not found" );
        }
    }
}
