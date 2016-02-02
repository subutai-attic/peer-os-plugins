package io.subutai.plugin.elasticsearch.impl.handler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
import io.subutai.common.peer.ContainerSize;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.LocalPeer;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.ClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.Commands;
import io.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;


/**
 * This class handles operations that are related to whole cluster.
 */
public class ClusterOperationHandler
        extends AbstractOperationHandler<ElasticsearchImpl, ElasticsearchClusterConfiguration>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private ElasticsearchClusterConfiguration config;
    CommandUtil commandUtil = new CommandUtil();


    public ClusterOperationHandler( final ElasticsearchImpl manager, final ElasticsearchClusterConfiguration config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation        ( ElasticsearchClusterConfiguration
                .PRODUCT_KEY,
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
            case REMOVE:
                removeCluster();
                break;
            case ADD:
                addNode();
                break;
            case START_ALL:
            case STOP_ALL:
            case STATUS_ALL:
                startNStop( operationType );
                break;
        }
    }


    private void startNStop( final ClusterOperationType operationType )
    {
        boolean isSuccessful = true;
        String message = "started";
        try
        {
            Environment env = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            switch ( operationType )
            {
                case START_ALL:
                    for ( String uuid : config.getNodes() )
                    {
                        try
                        {
                            EnvironmentContainerHost host = env.getContainerHostById( uuid );
                            try
                            {
                                trackerOperation.addLog( "Starting elasticsearch on node " + host.getHostname() );
                                CommandResult result = host.execute( Commands.getStartCommand() );
                                if ( !result.hasSucceeded() )
                                {
                                    isSuccessful = false;
                                }
                            }
                            catch ( CommandException e )
                            {
                                e.printStackTrace();
                            }
                        }
                        catch ( ContainerHostNotFoundException e )
                        {
                            e.printStackTrace();
                        }
                    }
                    break;
                case STOP_ALL:
                    for ( String uuid : config.getNodes() )
                    {
                        try
                        {
                            EnvironmentContainerHost host = env.getContainerHostById( uuid );
                            try
                            {
                                trackerOperation.addLog( "Stopping elasticsearch on node " + host.getHostname() );
                                CommandResult result = host.execute( Commands.getStopCommand() );
                                if ( !result.hasSucceeded() )
                                {
                                    isSuccessful = false;
                                }
                            }
                            catch ( CommandException e )
                            {
                                e.printStackTrace();
                            }
                        }
                        catch ( ContainerHostNotFoundException e )
                        {
                            e.printStackTrace();
                        }
                    }
                    message = "stopped";
                    break;
                case STATUS_ALL:
                    for ( String uuid : config.getNodes() )
                    {
                        try
                        {
                            EnvironmentContainerHost host = env.getContainerHostById( uuid );
                            try
                            {
                                trackerOperation.addLog( "Checking elasticsearch on node " + host.getHostname() );
                                CommandResult result = host.execute( Commands.getStatusCommand() );
                                NodeOperationHandler.logResults( trackerOperation, result );
                            }
                            catch ( CommandException e )
                            {
                                e.printStackTrace();
                            }
                        }
                        catch ( ContainerHostNotFoundException e )
                        {
                            e.printStackTrace();
                        }
                    }
                    return;
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        if ( isSuccessful )
        {
            trackerOperation.addLogDone( "All Elasticsearch nodes is " + message + " successfully" );
        }
        else
        {
            trackerOperation.addLogFailed( "Failed to start all Elasticsearch nodes" );
        }
    }


    private void addNode()
    {
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        NodeGroup nodeGroup = new NodeGroup( ElasticsearchClusterConfiguration.PRODUCT_KEY,
                ElasticsearchClusterConfiguration.TEMPLATE_NAME, ContainerSize.SMALL, 0, 0,
                localPeer.getId (), localPeer.getResourceHosts ().iterator().next().getId () );

        EnvironmentContainerHost newNode;
        try
        {
            EnvironmentContainerHost unUsedContainerInEnvironment =
                    findUnUsedContainerInEnvironment( environmentManager );
            if ( unUsedContainerInEnvironment != null )
            {
                newNode = unUsedContainerInEnvironment;
                config.getNodes().add( unUsedContainerInEnvironment.getId() );
            }
            else
            {
                Set<EnvironmentContainerHost> newNodeSet;
                try
                {
                    newNodeSet = environmentManager.growEnvironment( config.getEnvironmentId(),
                            new Topology(manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() ).getName (), 1, 1),
                            false );
                }
                catch ( EnvironmentNotFoundException | EnvironmentModificationException e )
                {
                    LOG.error( "Could not add new node(s) to environment." );
                    throw new ClusterException( e );
                }

                newNode = newNodeSet.iterator().next();

                config.getNodes().add( newNode.getId() );
            }

            manager.saveConfig( config );

            ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );
            Environment environment;
            try
            {
                environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
                configurator
                        .configureCluster( config, environmentManager.loadEnvironment( config.getEnvironmentId() ) );
                // check if one of nodes in elasticsearch cluster is already running,
                // then newly added node should be started automatically.
                try
                {
                    EnvironmentContainerHost node =
                            environment.getContainerHostById( config.getNodes().iterator().next() );
                    RequestBuilder checkNodeIsRunning = manager.getCommands().getStatusCommand();
                    CommandResult result = null;
                    try
                    {
                        result = commandUtil.execute( checkNodeIsRunning, node );
                        if ( result.hasSucceeded() )
                        {
                            if ( !result.getStdOut().toLowerCase().contains( "not" ) )
                            {
                                commandUtil.execute( manager.getCommands().getStartCommand(), newNode );
                            }
                        }
                    }
                    catch ( CommandException e )
                    {
                        LOG.error( "Could not check if Elasticsearch is running on one of the nodes" );
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
            trackerOperation.addLogDone( "Node added" );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "failed to add node:  %s", e ) );
        }
    }


    private EnvironmentContainerHost findUnUsedContainerInEnvironment( EnvironmentManager environmentManager )
    {
        EnvironmentContainerHost unusedNode = null;

        try
        {
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            Set<EnvironmentContainerHost> containerHostSet = environment.getContainerHosts();
            for ( EnvironmentContainerHost host : containerHostSet )
            {
                if ( ( !config.getNodes().contains( host.getId() ) ) && host.getTemplateName().equals(
                        ElasticsearchClusterConfiguration.TEMPLATE_NAME                              ) )
                {
                    unusedNode = host;
                    break;
                }
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return checkUnusedNode( unusedNode );
    }


    private EnvironmentContainerHost checkUnusedNode( EnvironmentContainerHost node )
    {
        if ( node != null )
        {
            for ( ElasticsearchClusterConfiguration config : manager.getClusters() )
            {
                if ( !config.getNodes().contains( node.getId() ) )
                {
                    return node;
                }
            }
        }
        return null;
    }


    public void removeCluster()
    {
        ElasticsearchClusterConfiguration config = manager.getCluster( clusterName );
        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLog( "Environment not found!" );
        }


        try
        {
            // stop cluster before removing it
            manager.stopCluster( config.getClusterName() );
            manager.deleteConfig( config );
            trackerOperation.addLogDone( "Cluster removed from database" );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void setupCluster()
    {
        Environment env;
        try
        {
            env = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            try
            {
                new ClusterConfiguration( manager, trackerOperation ).configureCluster( config, env );
                trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
            }
            catch ( ClusterConfigurationException e )
            {
                trackerOperation.addLogFailed                                                                         (
                        String.format( "Failed to setup Elasticsearch cluster %s : %s", clusterName, e.getMessage() ) );
                e.printStackTrace();
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
    }


    @Override
    public void destroyCluster()
    {
        ElasticsearchClusterConfiguration config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed                                                                  (
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }
        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( environment == null )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }

        manager.stopCluster( config.getClusterName() );

        if ( manager.getPluginDAO()
                    .deleteInfo( ElasticsearchClusterConfiguration.PRODUCT_KEY, config.getClusterName() ) )
        {
            trackerOperation.addLogDone( "Cluster information deleted from database" );
        }
        else
        {
            trackerOperation.addLogFailed( "Failed to delete cluster information from database" );
        }
    }
}
