package io.subutai.plugin.mongodb.impl.handler;


import java.util.HashSet;
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
import io.subutai.common.peer.Peer;
import io.subutai.common.peer.PeerException;
import io.subutai.common.peer.ResourceHost;
import io.subutai.common.resource.PeerGroupResources;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.plugin.mongodb.api.MongoException;
import io.subutai.plugin.mongodb.api.NodeType;
import io.subutai.plugin.mongodb.impl.ClusterConfiguration;
import io.subutai.plugin.mongodb.impl.MongoImpl;
import io.subutai.plugin.mongodb.impl.common.Commands;


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
        for ( String id : config.getConfigHosts() )
        {
            manager.startNode( clusterName, findHost( id ).getHostname(), NodeType.CONFIG_NODE );
        }

        /** start router nodes  */
        for ( String id : config.getRouterHosts() )
        {
            manager.startNode( clusterName, findHost( id ).getHostname(), NodeType.ROUTER_NODE );
        }

        /** start data nodes  */
        for ( String id : config.getDataHosts() )
        {
            manager.startNode( clusterName, findHost( id ).getHostname(), NodeType.DATA_NODE );
        }
        trackerOperation.addLogDone( operationType + " operation is finished." );
    }


    private void stopAll() throws MongoException
    {
        /** stop config nodes  */
        for ( String id : config.getConfigHosts() )
        {
            manager.stopNode( clusterName, findHost( id ).getHostname(), NodeType.CONFIG_NODE );
        }

        /** stop router nodes  */
        for ( String id : config.getRouterHosts() )
        {
            manager.stopNode( clusterName, findHost( id ).getHostname(), NodeType.ROUTER_NODE );
        }

        /** stop data nodes  */
        for ( String id : config.getDataHosts() )
        {
            manager.stopNode( clusterName, findHost( id ).getHostname(), NodeType.DATA_NODE );
        }
        trackerOperation.addLogDone( operationType + " operation is finished." );
    }


    private EnvironmentContainerHost findHost( String id )
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            try
            {
                return environment.getContainerHostById( id );
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


    public void addNode( NodeType nodeType )
    {
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();

        String hostId = getPreferredHost();
        NodeGroup nodeGroup =
                new NodeGroup( MongoClusterConfig.PRODUCT_NAME, MongoClusterConfig.TEMPLATE_NAME, ContainerSize.TINY, 1,
                        1, localPeer.getId(), hostId );

        EnvironmentContainerHost newNode;
        try
        {
            EnvironmentContainerHost unusedNodeInEnvironment = findUnUsedContainerInEnvironment( environmentManager );
            if ( unusedNodeInEnvironment != null )
            {
                newNode = unusedNodeInEnvironment;
            }
            else
            {
                Set<EnvironmentContainerHost> newNodeSet;
                try
                {
                    Topology topology = new Topology( config.getClusterName(), 0, 0 );
                    topology.addNodeGroupPlacement( nodeGroup.getPeerId(), nodeGroup );
                    newNodeSet = environmentManager.growEnvironment( config.getEnvironmentId(), topology, false );
                }
                catch ( EnvironmentNotFoundException | EnvironmentModificationException e )
                {
                    LOG.error( "Could not add new node(s) to environment." );
                    throw new ClusterException( e );
                }
                newNode = newNodeSet.iterator().next();
            }

            if ( nodeType.equals( NodeType.ROUTER_NODE ) )
            {
                config.getRouterHosts().add( newNode.getId() );
            }
            else if ( nodeType.equals( NodeType.DATA_NODE ) )
            {
                config.getDataHosts().add( newNode.getId() );
            }
            manager.saveConfig( config );

            ClusterConfiguration configurator = new ClusterConfiguration( trackerOperation, manager );
            Environment environment;
            try
            {
                environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
                configurator.configureCluster( config, environment );

                // check if one of config server nodes in mongo cluster is already running,
                // then newly added node should be started automatically.
                try
                {
                    EnvironmentContainerHost coordinator =
                            environment.getContainerHostById( config.getConfigHosts().iterator().next() );
                    RequestBuilder checkMasterIsRunning = Commands.getCheckConfigServer().build( true );
                    CommandResult result;
                    try
                    {
                        result = commandUtil.execute( checkMasterIsRunning, coordinator );
                        if ( result.hasSucceeded() )
                        {
                            if ( !result.getStdOut().isEmpty() )
                            {
                                if ( nodeType.equals( NodeType.ROUTER_NODE ) )
                                {
                                    Set<EnvironmentContainerHost> configServers = new HashSet<>();
                                    for ( String id : config.getConfigHosts() )
                                    {
                                        configServers.add( findHost( id ) );
                                    }
                                    commandUtil.execute( Commands.getStartRouterCommandLine( config.getRouterPort(),
                                            config.getCfgSrvPort(), config.getDomainName(), configServers )
                                                                 .build( true ), newNode );
                                }
                                else if ( nodeType.equals( NodeType.DATA_NODE ) )
                                {
                                    commandUtil.execute(
                                            Commands.getStartDataNodeCommandLine( config.getDataNodePort() )
                                                    .build( true ), newNode );
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
           /* try
            {
                manager.subscribeToAlerts( newNode );
            }
            catch ( MonitorException e )
            {
                throw new ClusterException( "Failed to subscribe to alerts: " + e.getMessage() );
            }*/
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
                if ( ( !config.getAllNodes().contains( host.getId() ) ) && host.getTemplateName().equals(
                        MongoClusterConfig.TEMPLATE_NAME ) )
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
            for ( MongoClusterConfig config : manager.getClusters() )
            {
                if ( !config.getAllNodes().contains( node.getId() ) )
                {
                    return node;
                }
            }
        }
        return null;
    }


    @Override
    public void runOperationOnContainers( final ClusterOperationType clusterOperationType )
    {

    }


    @Override
    public void setupCluster()
    {
        trackerOperation.addLog( "Configuring environment..." );

        try
        {
            Environment env = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            ClusterSetupStrategy clusterSetupStrategy =
                    manager.getClusterSetupStrategy( env, config, trackerOperation );
            clusterSetupStrategy.setup();
            //manager.subscribeToAlerts( env );

            trackerOperation.addLogDone( String.format( "Cluster %s configured successfully", clusterName ) );
        }
        catch ( ClusterSetupException | EnvironmentNotFoundException e )
        {
            LOG.error( String.format( "Failed to configure cluster %s", clusterName ), e );
            trackerOperation.addLogFailed( String.format( "Failed to configure cluster %s", clusterName ) );
        }
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

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
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
       /* try
        {
            manager.unsubscribeFromAlerts( environment );
        }
        catch ( MonitorException e )
        {
            trackerOperation.addLog( String.format( "Failed to unsubscribe from alerts: %s", e.getMessage() ) );
        }*/
    }


    public String getPreferredHost()
    {
        final ResourceHost preferredHost = findFirstAvailableHost( manager.getPeerManager().getLocalPeer() );
        return preferredHost.getId();
    }


    private ResourceHost findFirstAvailableHost( final LocalPeer peer )
    {
        //TODO: fix me: implement RH selection
        return peer.getResourceHosts().iterator().next();
    }
}
