package org.safehaus.subutai.plugin.mongodb.impl.handler;


import java.util.HashSet;
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
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.mongodb.api.Mongo;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoException;
import org.safehaus.subutai.plugin.mongodb.api.NodeType;
import org.safehaus.subutai.plugin.mongodb.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;
import org.safehaus.subutai.plugin.mongodb.impl.common.CommandDef;
import org.safehaus.subutai.plugin.mongodb.impl.common.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;



/**
 * This class handles operations that are related to whole cluster.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<MongoImpl, MongoClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private MongoClusterConfig config;
    private CommandUtil commandUtil;
    private NodeType nodeType;


    public ClusterOperationHandler( final MongoImpl manager, final MongoClusterConfig config,
                                    final ClusterOperationType operationType, NodeType nodeType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
        commandUtil = new CommandUtil();
        this.nodeType = nodeType;
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
                try
                {
                    startAll();
                }
                catch ( MongoException e )
                {
                    e.printStackTrace();
                }
                break;
            case STOP_ALL:
                try
                {
                    stopAll();
                }
                catch ( MongoException e )
                {
                    e.printStackTrace();
                }
                break;
            case STATUS_ALL:
                runOperationOnContainers( operationType );
                break;
            case ADD:
                addNode( nodeType );
                break;
            case REMOVE:
                destroyCluster();
                break;
        }
    }

    private void startAll() throws MongoException
    {
        /** start config nodes  */
        for ( UUID uuid : config.getConfigHosts() ){
            manager.startNode(clusterName, findHost(uuid).getHostname(), NodeType.CONFIG_NODE);
        }

        /** start router nodes  */
        for ( UUID uuid : config.getRouterHosts() ){
            manager.startNode( clusterName, findHost( uuid ).getHostname(), NodeType.CONFIG_NODE );
        }

        /** start data nodes  */
        for ( UUID uuid : config.getDataHosts() ){
            manager.startNode(clusterName, findHost(uuid).getHostname(), NodeType.CONFIG_NODE);
        }
        trackerOperation.addLogDone( operationType + " operation is finished." );
    }


    private void stopAll() throws MongoException
    {
        /** start config nodes  */
        for ( UUID uuid : config.getConfigHosts() ){
            manager.stopNode( clusterName, findHost( uuid ).getHostname(), NodeType.CONFIG_NODE );
        }

        /** start router nodes  */
        for ( UUID uuid : config.getRouterHosts() ){
            manager.stopNode( clusterName, findHost( uuid ).getHostname(), NodeType.ROUTER_NODE );
        }

        /** start data nodes  */
        for ( UUID uuid : config.getDataHosts() ){
            manager.stopNode( clusterName, findHost( uuid ).getHostname(), NodeType.DATA_NODE );
        }
        trackerOperation.addLogDone( operationType + " operation is finished." );
    }


    private ContainerHost findHost( UUID uuid ){
        try
        {
            Environment environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            try
            {
                return environment.getContainerHostById( uuid );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return null;
    }



    public void startDataNode( ContainerHost host ) throws MongoException
    {
        try
        {
            CommandDef commandDef = Commands.getStartDataNodeCommandLine( config.getDataNodePort() );
            CommandResult commandResult = host.execute(
                    commandDef.build( true ).withTimeout( commandDef.getTimeout() ) );

            if ( !commandResult.getStdOut().contains( "child process started successfully, parent exiting" ) )
            {
                throw new CommandException( "Could not start mongo data instance." );
            }
        }
        catch ( CommandException e )
        {
            LOG.error( "Start command failed.", e );
            throw new MongoException( "Start command failed" );
        }
    }


    public void startConfigNode( ContainerHost host ) throws MongoException
    {
        try
        {
            CommandDef commandDef = Commands.getStartConfigServerCommand( config.getCfgSrvPort() );
            CommandResult commandResult = host.execute( commandDef.build( true ).withTimeout( commandDef.getTimeout() ) );

            if ( !commandResult.getStdOut().contains( "child process started successfully, parent exiting" ) )
            {
                throw new CommandException( "Could not start mongo config instance." );
            }
        }
        catch ( CommandException e )
        {
            LOG.error( e.getMessage(), e );
            throw new MongoException( e.getMessage() );
        }
    }


    public void startRouterNode( ContainerHost host ) throws MongoException
    {
        Set<ContainerHost> configServers = new HashSet<>();
        for ( UUID uuid : config.getConfigHosts() ){
            configServers.add( findHost( uuid ) );
        }

        CommandDef commandDef =
                Commands.getStartRouterCommandLine( config.getRouterPort(), config.getCfgSrvPort(),
                        config.getDomainName(), configServers );
        try
        {
            CommandResult commandResult = host.execute( commandDef.build( true ) );

            if ( !commandResult.getStdOut().contains( "child process started successfully, parent exiting" ) )
            {
                throw new CommandException( "Could not start mongo route instance." );
            }
        }
        catch ( CommandException e )
        {
            LOG.error( e.toString(), e );
            throw new MongoException( "Could not start mongo router node:" );
        }
    }


    public void addNode( NodeType nodeType )
    {
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        NodeGroup nodeGroup = new NodeGroup( MongoClusterConfig.PRODUCT_NAME,  MongoClusterConfig.TEMPLATE_NAME,
                1, 0, 0, new PlacementStrategy( "ROUND_ROBIN" ) );

        Topology topology = new Topology();

        topology.addNodeGroupPlacement( localPeer, nodeGroup );

        ContainerHost newNode;
        try
        {
            ContainerHost unusedNodeInEnvironment = findUnUsedContainerInEnvironment( environmentManager );
            if( unusedNodeInEnvironment != null )
            {
                newNode = unusedNodeInEnvironment;
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
            }

            if ( nodeType.equals( NodeType.ROUTER_NODE ) ){
                config.getRouterHosts().add( newNode.getId() );
            }
            else if ( nodeType.equals( NodeType.DATA_NODE ) ){
                config.getDataHosts().add( newNode.getId() );
            }
            manager.saveConfig( config );

            ClusterConfiguration configurator = new ClusterConfiguration( trackerOperation, manager );
            Environment environment;
            try
            {
                environment = environmentManager.findEnvironment( config.getEnvironmentId() );
                configurator
                        .configureCluster( config, environmentManager.findEnvironment( config.getEnvironmentId() ) );

                // check if one of config server nodes in mongo cluster is already running,
                // then newly added node should be started automatically.
                try
                {
                    ContainerHost coordinator = environment.getContainerHostById(
                            config.getConfigHosts().iterator().next() );
                    RequestBuilder checkMasterIsRunning = Commands.getCheckConfigServer().build( true );
                    CommandResult result;
                    try
                    {
                        result = commandUtil.execute( checkMasterIsRunning, coordinator );
                        if ( result.hasSucceeded() ){
                            if ( result.getStdOut().toLowerCase().contains( "pid" ) ){
                                if ( nodeType.equals( NodeType.ROUTER_NODE ) ){
                                    Set<ContainerHost> configServers = new HashSet<>();
                                    for ( UUID uuid : config.getConfigHosts() ){
                                        configServers.add( findHost( uuid ) );
                                    }
                                    commandUtil.execute( Commands.getStartRouterCommandLine( config.getRouterPort(), config.getCfgSrvPort(),
                                            config.getDomainName(), configServers ).build( true ), newNode );
                                }
                                else if ( nodeType.equals( NodeType.DATA_NODE ) ){
                                    commandUtil.execute( Commands.getStartDataNodeCommandLine( config.getDataNodePort() ).build( true ), newNode );
                                }

                            }
                        }
                    }
                    catch ( CommandException e )
                    {
                        LOG.error( "Could not check if Mongo is running on one of the seeds nodes" );
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
                if( (!config.getAllNodes().contains( host.getId())) && host.getTemplateName().equals( MongoClusterConfig.TEMPLATE_NAME ) )
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
            for( MongoClusterConfig config : manager.getClusters() )
            {
                if( !config.getAllNodes().contains( node.getId() ))
                {
                    return node;
                }
            }
        }
        return null;

    }

    @Override
    public void setupCluster()
    {
        trackerOperation.addLog( "Configuring environment..." );

        try
        {
            Environment env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            ClusterSetupStrategy clusterSetupStrategy =
                    manager.getClusterSetupStrategy( env, config, trackerOperation );
            clusterSetupStrategy.setup();
            manager.subscribeToAlerts( env );

            trackerOperation.addLogDone( String.format( "Cluster %s configured successfully", clusterName ) );
        }
        catch ( MonitorException | ClusterSetupException | EnvironmentNotFoundException e )
        {
            LOG.error( String.format( "Failed to configure cluster %s", clusterName ), e );
            trackerOperation.addLogFailed( String.format( "Failed to configure cluster %s", clusterName ) );
        }
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
//        try
//        {
//            Environment environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
//            CommandResult result = null;
//            switch ( clusterOperationType )
//            {
//                case START_ALL:
//                    for ( ContainerHost containerHost : environment.getContainerHosts() )
//                    {
//                        result = executeCommand( containerHost, Commands.startCommand );
//                    }
//                    break;
//                case STOP_ALL:
//                    for ( ContainerHost containerHost : environment.getContainerHosts() )
//                    {
//                        result = executeCommand( containerHost, Commands.stopCommand );
//                    }
//                    break;
//                case STATUS_ALL:
//                    for ( ContainerHost containerHost : environment.getContainerHosts() )
//                    {
//                        result = executeCommand( containerHost, Commands.statusCommand );
//                    }
//                    break;
//            }
//            NodeOperationHandler.logResults( trackerOperation, result );
//        }
//        catch ( EnvironmentNotFoundException e )
//        {
//            trackerOperation.addLogFailed( "Environment not found" );
//        }
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
        MongoClusterConfig config = manager.getCluster( clusterName );
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
            manager.deleteConfig( config );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( "Failed to delete cluster information from database" );
            return;
        }
        trackerOperation.addLogDone( "Cluster removed from database" );
        try
        {
            manager.unsubscribeFromAlerts( environment );
        }
        catch ( MonitorException e )
        {
            trackerOperation.addLog( String.format( "Failed to unsubscribe from alerts: %s", e.getMessage() ) );
        }
    }
}
