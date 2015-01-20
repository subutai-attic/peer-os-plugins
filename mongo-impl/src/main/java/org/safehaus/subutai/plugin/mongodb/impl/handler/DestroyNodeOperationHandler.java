package org.safehaus.subutai.plugin.mongodb.impl.handler;


import java.util.Arrays;
import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.core.peer.api.Peer;
import org.safehaus.subutai.core.peer.api.PeerException;
import org.safehaus.subutai.plugin.mongodb.api.InstallationType;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoDataNode;
import org.safehaus.subutai.plugin.mongodb.api.MongoException;
import org.safehaus.subutai.plugin.mongodb.api.MongoNode;
import org.safehaus.subutai.plugin.mongodb.api.MongoRouterNode;
import org.safehaus.subutai.plugin.mongodb.api.NodeType;
import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;
import org.safehaus.subutai.plugin.mongodb.impl.common.Commands;


/**
 * Handles destroy mongo node operation
 */
public class DestroyNodeOperationHandler extends AbstractMongoOperationHandler<MongoImpl, MongoClusterConfig>
{
    private final TrackerOperation po;
    private final String lxcHostname;


    public DestroyNodeOperationHandler( MongoImpl manager, String clusterName, String lxcHostname )
    {
        super( manager, clusterName );
        this.lxcHostname = lxcHostname;
        po = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
                String.format( "Destroying %s in %s", lxcHostname, clusterName ) );
    }


    @Override
    public UUID getTrackerId()
    {
        return po.getId();
    }


    @Override
    public void run()
    {
        final MongoClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            po.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }

        MongoNode node = config.findNode( lxcHostname );
        if ( node == null )
        {
            po.addLogFailed( String.format( "Node with hostname %s is not connected", lxcHostname ) );
            return;
        }
        if ( !config.getAllNodes().contains( node ) )
        {
            po.addLogFailed(
                    String.format( "Node with hostname %s does not belong to cluster %s", lxcHostname, clusterName ) );
            return;
        }

        final NodeType nodeType = config.getNodeType( node );
        if ( nodeType == NodeType.CONFIG_NODE && config.getConfigServers().size() == 1 )
        {
            po.addLogFailed( "This is the last configuration server in the cluster. Please, destroy cluster instead" );
            return;
        }
        if ( nodeType == NodeType.DATA_NODE && config.getDataNodes().size() == 1 )
        {
            po.addLogFailed( "This is the last data node in the cluster. Please, destroy cluster instead" );
            return;
        }
        if ( nodeType == NodeType.ROUTER_NODE && config.getRouterServers().size() == 1 )
        {
            po.addLogFailed( "This is the last router in the cluster. Please, destroy cluster instead" );
            return;
        }
        try
        {
            if ( nodeType == NodeType.CONFIG_NODE )
            {

                config.getConfigServers().remove( node );
                config.setNumberOfConfigServers( config.getNumberOfConfigServers() - 1 );
                //restart routers
                po.addLog( "Restarting routers..." );

                try
                {
                    for ( MongoRouterNode routerNode : config.getRouterServers() )
                    {
                        routerNode.stop();
                        routerNode.start( config );
                    }
                }
                catch ( MongoException me )
                {
                    po.addLog( "Not all routers restarted. Use Terminal module to restart them, skipping..." );
                }
            }
            else if ( nodeType == NodeType.DATA_NODE )
            {
                MongoDataNode dataNode = ( MongoDataNode ) node;
                dataNode.stop();
                config.getDataNodes().remove( dataNode );
                config.setNumberOfDataNodes( config.getNumberOfDataNodes() - 1 );
                //unregister from primary
                po.addLog( "Unregistering this node from replica set..." );
                MongoDataNode primaryDataNode = config.findPrimaryNode();
                primaryDataNode.unRegisterSecondaryNode( dataNode );
            }
            else if ( nodeType == NodeType.ROUTER_NODE )
            {
                config.setNumberOfRouters( config.getNumberOfRouters() - 1 );
                config.getRouterServers().remove( node );
            }

            Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID(
                    UUID.fromString( node.getContainerHost().getEnvironmentId() ) );
            manager.unsubscribeFromAlerts( environment );

            if ( config.getInstallationType() == InstallationType.OVER_ENVIRONMENT )
            {
                ContainerHost containerHost = environment.getContainerHostById( node.getContainerHost().getId() );
                po.addLog( "Purging subutai-hadoop from containers." );
                logResults( po,
                        Arrays.asList( executeCommand( Commands.getClearMongoConfigsCommand(), containerHost ) ) );
            }
            else if ( config.getInstallationType() == InstallationType.STANDALONE )
            {
                //destroy lxc
                po.addLog( "Destroying lxc container..." );

                Peer peer = manager.getPeerManager().getPeer( node.getPeerId() );

                peer.destroyContainer( ( ContainerHost ) node );
                po.addLog( "Lxc container destroyed successfully" );
            }
        }
        catch ( PeerException | MongoException | MonitorException e )
        {
            po.addLog( String.format( "Could not destroy lxc container %s. Use LXC module to cleanup, skipping...",
                    e.getMessage() ) );
        }

        //update db
        po.addLog( "Updating cluster information in database..." );
        String json = manager.getGSON().toJson( config.prepare() );
        manager.getPluginDAO().saveInfo( MongoClusterConfig.PRODUCT_KEY, config.getClusterName(), json );
        po.addLogDone( "Cluster information updated in database" );
    }
}
