package org.safehaus.subutai.plugin.elasticsearch.impl.handler;


import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentModificationException;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.environment.NodeGroup;
import org.safehaus.subutai.common.environment.Topology;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import org.safehaus.subutai.plugin.elasticsearch.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.elasticsearch.impl.Commands;
import org.safehaus.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


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
        trackerOperation = manager.getTracker().createTrackerOperation( ElasticsearchClusterConfiguration.PRODUCT_KEY,
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
        try
        {
            Environment env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            switch ( operationType ){
                case START_ALL:
                    for ( UUID uuid : config.getNodes() ){
                        try
                        {
                            ContainerHost host = env.getContainerHostById( uuid );
                            try
                            {
                                trackerOperation.addLog( "Staring elasticsearch on node " + host.getHostname() );
                                CommandResult result = host.execute( Commands.getStartCommand() );
                                if ( ! result.hasSucceeded() ){
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
                    for ( UUID uuid : config.getNodes() ){
                        try
                        {
                            ContainerHost host = env.getContainerHostById( uuid );
                            try
                            {
                                trackerOperation.addLog( "Stopping elasticsearch on node " + host.getHostname() );
                                CommandResult result = host.execute( Commands.getStartCommand() );
                                if ( ! result.hasSucceeded() ){
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
                case STATUS_ALL:_ALL:
                    for ( UUID uuid : config.getNodes() ){
                        try
                        {
                            ContainerHost host = env.getContainerHostById( uuid );
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
        if ( isSuccessful ){
            trackerOperation.addLogDone( "All Elasticsearch nodes is started succesfully" );
        }
        else{
            trackerOperation.addLogFailed( "Failed to start all Elasticsearch nodes" );
        }
    }


    private void addNode()
    {
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        NodeGroup nodeGroup = new NodeGroup( ElasticsearchClusterConfiguration.PRODUCT_KEY, ElasticsearchClusterConfiguration.TEMPLATE_NAME,
                1, 0, 0, new PlacementStrategy( "ROUND_ROBIN" ) );

        Topology topology = new Topology();

        topology.addNodeGroupPlacement( localPeer, nodeGroup );

        ContainerHost newNode = null;
        try
        {
            ContainerHost unusedNodeInenvironment = findUnUsedContainerInEnvironment( environmentManager );
            if( unusedNodeInenvironment != null )
            {
                newNode = unusedNodeInenvironment;
                config.getNodes().add( unusedNodeInenvironment.getId() );
            }
            else {
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

                newNode = newNodeSet.iterator().next();

                config.getNodes().add( newNode.getId() );
            }

            manager.saveConfig( config );

            ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );
            Environment environment;
            try
            {
                environment = environmentManager.findEnvironment( config.getEnvironmentId() );
                configurator
                        .configureCluster( config, environmentManager.findEnvironment( config.getEnvironmentId() ) );
                // check if one of nodes in elasticsearch cluster is already running,
                // then newly added node should be started automatically.
                try
                {
                    ContainerHost node = environment.getContainerHostById( config.getNodes().iterator().next() );
                    RequestBuilder checkNodeIsRunning =  manager.getCommands().getStatusCommand();
                    CommandResult result = null;
                    try
                    {
                        result = commandUtil.execute( checkNodeIsRunning, node );
                        if ( result.hasSucceeded() ){
                            if ( !result.getStdOut().toLowerCase().contains( "not" ) ){
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


    private ContainerHost findUnUsedContainerInEnvironment( EnvironmentManager environmentManager )
    {
        ContainerHost unusedNode = null;

        try
        {
            Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
            Set<ContainerHost> containerHostSet = environment.getContainerHosts();
            for( ContainerHost host : containerHostSet )
            {
                if( (!config.getNodes().contains( host.getId())) && host.getTemplateName().equals( ElasticsearchClusterConfiguration.TEMPLATE_NAME ) )
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


    private ContainerHost checkUnusedNode( ContainerHost node )
    {
        if( node != null)
        {
            for( ElasticsearchClusterConfiguration config : manager.getClusters() )
            {
                if( !config.getNodes().contains( node.getId() ))
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
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLog( "Environment not found!" );
        }

        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }
        try
        {
            // stop cluster before removing it
            manager.stopCluster( config.getClusterName() );
            manager.unsubscribeFromAlerts( environment );
            manager.deleteConfig( config );
            trackerOperation.addLogDone( "Cluster removed from database" );
        }
        catch ( ClusterException | MonitorException e )
        {
            e.printStackTrace();
        }
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        throw new UnsupportedOperationException();
    }


    private CommandResult executeCommand( ContainerHost containerHost, RequestBuilder command )
    {
        CommandResult result = null;
        try
        {
            result = commandUtil.execute( command, containerHost );
        }
        catch ( CommandException e )
        {
            LOG.error( "Command failed", e );
        }
        return result;
    }


    @Override
    public void setupCluster()
    {
        Environment env;
        try
        {
            env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            try
            {
                new ClusterConfiguration( manager, trackerOperation ).configureCluster( config, env );
                trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
            }
            catch ( ClusterConfigurationException e )
            {
                trackerOperation.addLogFailed(
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
            e.printStackTrace();
        }

        if ( environment == null )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }

        manager.stopCluster( config.getClusterName() );
        try
        {
            manager.unsubscribeFromAlerts( environment );
        }
        catch ( MonitorException e )
        {
            trackerOperation.addLog( String.format( "Failed to unsubscribe from alerts: %s", e.getMessage() ) );
        }

        if ( manager.getPluginDAO().deleteInfo( ElasticsearchClusterConfiguration.PRODUCT_KEY, config.getClusterName() ) )
        {
            trackerOperation.addLogDone( "Cluster information deleted from database" );
        }
        else
        {
            trackerOperation.addLogFailed( "Failed to delete cluster information from database" );
        }
    }
}
