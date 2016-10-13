package io.subutai.plugin.mongodb.impl.handler;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
import io.subutai.common.peer.ContainerSize;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.LocalPeer;
import io.subutai.common.peer.Peer;
import io.subutai.common.peer.PeerException;
import io.subutai.common.peer.ResourceHost;
import io.subutai.hub.share.quota.ContainerQuota;
import io.subutai.hub.share.resource.PeerGroupResources;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.strategy.api.ContainerPlacementStrategy;
import io.subutai.core.strategy.api.RoundRobinStrategy;
import io.subutai.core.strategy.api.StrategyException;
import io.subutai.core.template.api.TemplateManager;
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
    private TemplateManager templateManager;


    public ClusterOperationHandler( final MongoImpl manager, final TemplateManager templateManager,
                                    final MongoClusterConfig config, final ClusterOperationType operationType,
                                    NodeType nodeType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        this.templateManager = templateManager;
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
                trackerOperation.addLogFailed( "Container not found:" + e.getMessage() );
                LOG.error( "Container not found: ", e );
                e.printStackTrace();
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found:" + e.getMessage() );
            LOG.error( "Environment not found: ", e );
            e.printStackTrace();
        }
        return null;
    }


    public void addNode( NodeType nodeType )
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();

        EnvironmentContainerHost newNode;
        try
        {
            Environment env = environmentManager.loadEnvironment( config.getEnvironmentId() );
            List<Integer> containersIndex = Lists.newArrayList();

            for ( final EnvironmentContainerHost containerHost : env.getContainerHosts() )
            {
                String numbers = containerHost.getContainerName().replace( "Container", "" ).trim();
                String contId = numbers.split( "-" )[0];
                containersIndex.add( Integer.parseInt( contId ) );
            }

            EnvironmentContainerHost unusedNodeInEnvironment = findUnUsedContainerInEnvironment( environmentManager );
            if ( unusedNodeInEnvironment != null )
            {
                newNode = unusedNodeInEnvironment;
            }
            else
            {
                Set<EnvironmentContainerHost> newNodeSet = null;
                try
                {
                    String containerName = "Container" + String.valueOf( Collections.max( containersIndex ) + 1 );
                    NodeSchema node =
                            new NodeSchema( containerName, ContainerSize.SMALL, MongoClusterConfig.TEMPLATE_NAME,
                                    templateManager.getTemplateByName( MongoClusterConfig.TEMPLATE_NAME ).getId() );
                    List<NodeSchema> nodes = new ArrayList<>();
                    nodes.add( node );

                    Blueprint blueprint = new Blueprint(
                            manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() ).getName(),
                            nodes );

                    ContainerPlacementStrategy strategy =
                            manager.getStrategyManager().findStrategyById( RoundRobinStrategy.ID );
                    PeerGroupResources peerGroupResources = manager.getPeerManager().getPeerGroupResources();
                    Map<ContainerSize, ContainerQuota> quotas = manager.getQuotaManager().getDefaultQuotas();

                    Topology topology =
                            strategy.distribute( blueprint.getName(), blueprint.getNodes(), peerGroupResources,
                                    quotas );

                    newNodeSet = environmentManager.growEnvironment( config.getEnvironmentId(), topology, false );
                }
                catch ( EnvironmentNotFoundException | EnvironmentModificationException | StrategyException |
                        PeerException e )
                {
                    LOG.error( "Could not add new node(s) to environment." );
                    throw new ClusterException( e );
                }
                newNode = newNodeSet.iterator().next();
            }

            ClusterConfiguration configurator = new ClusterConfiguration( trackerOperation, manager );
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );

            if ( nodeType.equals( NodeType.ROUTER_NODE ) )
            {
                configurator.addNode( config, environment, newNode, NodeType.ROUTER_NODE );
                config.getRouterHosts().add( newNode.getId() );
            }
            else if ( nodeType.equals( NodeType.DATA_NODE ) )
            {
                configurator.addNode( config, environment, newNode, NodeType.DATA_NODE );
                config.getDataHosts().add( newNode.getId() );
            }

            manager.saveConfig( config );

            trackerOperation.addLogDone( "Node added" );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "Failed to add node:  %s", e ) );
            LOG.error( "Failed to add node: ", e );
            e.printStackTrace();
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found:" + e.getMessage() );
            LOG.error( "Environment not found: ", e );
            e.printStackTrace();
        }
        catch ( CommandException | MongoException e )
        {
            trackerOperation.addLogFailed( "Error during execution command:" + e.getMessage() );
            LOG.error( "Error during execution command: ", e );
            e.printStackTrace();
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
        // TODO delete configurations
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
            ClusterConfiguration configurator = new ClusterConfiguration( trackerOperation, manager );

            Set<EnvironmentContainerHost> dataNodes = environment.getContainerHostsByIds( config.getDataHosts() );
            Set<EnvironmentContainerHost> configServers = environment.getContainerHostsByIds( config.getConfigHosts() );
            Set<EnvironmentContainerHost> routerNodes = environment.getContainerHostsByIds( config.getRouterHosts() );

            configurator.destroyCluster( dataNodes, configServers, routerNodes, config, environment );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
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
