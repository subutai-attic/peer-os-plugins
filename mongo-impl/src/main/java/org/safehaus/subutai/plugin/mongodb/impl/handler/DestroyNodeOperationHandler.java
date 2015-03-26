//package org.safehaus.subutai.plugin.mongodb.impl.handler;
//
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.UUID;
//
//import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
//import org.safehaus.subutai.common.environment.Environment;
//import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
//import org.safehaus.subutai.common.peer.ContainerHost;
//import org.safehaus.subutai.core.metric.api.MonitorException;
//import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
//import org.safehaus.subutai.plugin.mongodb.api.MongoDataNode;
//import org.safehaus.subutai.plugin.mongodb.api.MongoException;
//import org.safehaus.subutai.plugin.mongodb.api.MongoNode;
//import org.safehaus.subutai.plugin.mongodb.api.MongoRouterNode;
//import org.safehaus.subutai.plugin.mongodb.api.NodeType;
//import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;
//import org.safehaus.subutai.plugin.mongodb.impl.common.Commands;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//
//
///**
// * Handles destroy mongo node operation
// */
//public class DestroyNodeOperationHandler extends AbstractMongoOperationHandler<MongoImpl, MongoClusterConfig>
//{
//    private final String lxcHostname;
//    private static final Logger LOGGER = LoggerFactory.getLogger( DestroyNodeOperationHandler.class );
//    private final NodeType whichNode;
//
//
//    public DestroyNodeOperationHandler( MongoImpl manager, String clusterName, String lxcHostname, NodeType whichNode )
//    {
//        super( manager, manager.getCluster( clusterName ) );
//        this.lxcHostname = lxcHostname;
//        this.whichNode = whichNode;
//        trackerOperation = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
//                String.format( "Destroying %s in %s", lxcHostname, clusterName ) );
//    }
//
//
//    @Override
//    public void run()
//    {
//        final MongoClusterConfig config = manager.getCluster( clusterName );
//        if ( config == null )
//        {
//            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
//            return;
//        }
//
//        MongoNode node = config.findNode( lxcHostname );
//        if ( node == null )
//        {
//            trackerOperation.addLogFailed( String.format( "Node with hostname %s is not connected", lxcHostname ) );
//            return;
//        }
//        if ( !config.getAllNodes().contains( node ) )
//        {
//            trackerOperation.addLogFailed(
//                    String.format( "Node with hostname %s does not belong to cluster %s", lxcHostname, clusterName ) );
//            return;
//        }
//
//        try
//        {
//            List<NodeType> roles = config.getNodeRoles( node.getContainerHost() );
//
//            final NodeType nodeType = config.getNodeType( node );
//            if ( nodeType == NodeType.CONFIG_NODE && config.getConfigServers().size() == 1 )
//            {
//                trackerOperation.addLogFailed(
//                        "This is the last configuration server in the cluster. Please, destroy cluster instead" );
//                return;
//            }
//            if ( nodeType == NodeType.DATA_NODE && config.getDataHosts().size() == 1 )
//            {
//                trackerOperation
//                        .addLogFailed( "This is the last data node in the cluster. Please, destroy cluster instead" );
//                return;
//            }
//            if ( nodeType == NodeType.ROUTER_NODE && config.getRouterServers().size() == 1 )
//            {
//                trackerOperation
//                        .addLogFailed( "This is the last router in the cluster. Please, destroy cluster instead" );
//                return;
//            }
//            if ( nodeType == NodeType.CONFIG_NODE )
//            {
//
//                config.getConfigServers().remove( node );
//                config.getConfigHosts().remove( node.getContainerHost().getId() );
//                config.setNumberOfConfigServers( config.getNumberOfConfigServers() - 1 );
//                //restart routers
//                trackerOperation.addLog( "Restarting routers..." );
//
//                try
//                {
//                    for ( MongoRouterNode routerNode : config.getRouterServers() )
//                    {
//                        routerNode.stop();
//                        routerNode.start( config );
//                    }
//                }
//                catch ( MongoException me )
//                {
//                    trackerOperation
//                            .addLog( "Not all routers restarted. Use Terminal module to restart them, skipping..." );
//                    LOGGER.error( "Not all routers restarted.", me );
//                    return;
//                }
//            }
//            else if ( nodeType == NodeType.DATA_NODE )
//            {
//                MongoDataNode dataNode = ( MongoDataNode ) node;
//                dataNode.stop();
//                config.getDataHosts().remove( dataNode );
//                config.getDataHostIds().remove( node.getContainerHost().getId() );
//                config.setNumberOfDataNodes( config.getNumberOfDataNodes() - 1 );
//                //unregister from primary
//                trackerOperation.addLog( "Unregistering this node from replica set..." );
//                MongoDataNode primaryDataNode = ( MongoDataNode ) config.findNode( config.getPrimaryNode() );
//                primaryDataNode.unRegisterSecondaryNode( dataNode );
//            }
//            else if ( nodeType == NodeType.ROUTER_NODE )
//            {
//                config.setNumberOfRouters( config.getNumberOfRouters() - 1 );
//                config.getRouterServers().remove( node );
//                config.getRouterHosts().remove( node.getContainerHost().getId() );
//            }
//
//
//            Environment environment = manager.getEnvironmentManager().findEnvironment(
//                    UUID.fromString( node.getContainerHost().getEnvironmentId() ) );
//            manager.unsubscribeFromAlerts( environment );
//
//            ContainerHost containerHost = environment.getContainerHostById( node.getContainerHost().getId() );
//            trackerOperation.addLog( "Purging subutai-mongo from containers." );
//            logResults( trackerOperation,
//                    Arrays.asList( executeCommand( Commands.getStopMongodbService(), containerHost ) ) );
//        }
//        catch ( MongoException | MonitorException | ContainerHostNotFoundException e )
//        {
//            trackerOperation.addLogFailed(
//                    String.format( "Could not destroy lxc container. Use LXC module to cleanup, skipping..." ) );
//            LOGGER.error( "Couldn't destroy lxc container.", e );
//            return;
//        }
//        catch ( EnvironmentNotFoundException e )
//        {
//            trackerOperation.addLogFailed( "Environment not found." );
//            LOGGER.error( "Environment not found.", e );
//            return;
//        }
//
//        //update db
//        trackerOperation.addLog( "Updating cluster information in database..." );
//        Gson gson = new GsonBuilder().serializeNulls().excludeFieldsWithoutExposeAnnotation().create();
//        String json = gson.toJson( config.prepare() );
//        manager.getPluginDAO().saveInfo( MongoClusterConfig.PRODUCT_KEY, config.getClusterName(), json );
//        trackerOperation.addLogDone( "Cluster information updated in database" );
//    }
//}
//
