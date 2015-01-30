package org.safehaus.subutai.plugin.mongodb.impl.handler;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentModificationException;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.environment.NodeGroup;
import org.safehaus.subutai.common.environment.Topology;
import org.safehaus.subutai.common.exception.SubutaiException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.peer.Host;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoDataNode;
import org.safehaus.subutai.plugin.mongodb.api.MongoNode;
import org.safehaus.subutai.plugin.mongodb.api.MongoRouterNode;
import org.safehaus.subutai.plugin.mongodb.api.NodeType;
import org.safehaus.subutai.plugin.mongodb.impl.MongoConfigNodeImpl;
import org.safehaus.subutai.plugin.mongodb.impl.MongoDataNodeImpl;
import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;
import org.safehaus.subutai.plugin.mongodb.impl.MongoRouterNodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * Handles add mongo node operation
 */
public class AddNodeOperationHandler extends AbstractMongoOperationHandler<MongoImpl, MongoClusterConfig>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( AddNodeOperationHandler.class );
    private final NodeType nodeType;


    public AddNodeOperationHandler( MongoImpl manager, String clusterName, NodeType nodeType )
    {
        super( manager, clusterName );
        this.nodeType = nodeType;
        trackerOperation = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
                String.format( "Adding %s to %s...", nodeType, clusterName ) );
    }


    @Override
    public UUID getTrackerId()
    {
        return trackerOperation.getId();
    }


    @Override
    public void run()
    {
        MongoClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }
        if ( nodeType == NodeType.CONFIG_NODE )
        {
            trackerOperation.addLogFailed( "Can not add config server" );
            return;
        }
        if ( nodeType == NodeType.DATA_NODE && config.getDataNodes().size() == 7 )
        {
            trackerOperation.addLogFailed( "Replica set cannot have more than 7 members" );
            return;
        }

        trackerOperation.addLog( "Creating lxc container..." );

        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        MongoNode mongoNode = null;

        try
        {
            Environment environment = null;
            try
            {
                environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                logExceptionWithMessage(
                        String.format( "Error getting environment by id: %s", config.getEnvironmentId().toString() ),
                        e );
                return;
            }
            List<ContainerHost> envContainerHosts = new ArrayList<>( environment.getContainerHosts() );
            try
            {
                envContainerHosts.removeAll( environment.getContainerHostsByIds( config.getAllNodeIds() ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logExceptionWithMessage(
                        String.format( "Couldn't retrieve some container hosts by ids: %s", config.getAllNodeIds() ),
                        e );
                return;
            }

            //remove container hosts cloned not from mongo template
            for ( int i = 0; i < envContainerHosts.size(); i++ )
            {
                ContainerHost host = envContainerHosts.get( i );
                if ( !host.getTemplateName().equalsIgnoreCase( MongoClusterConfig.PRODUCT_NAME ) )
                {
                    envContainerHosts.remove( i );
                }
            }

            if ( envContainerHosts.size() == 0 )
            {
                try
                {
                    Topology topology = config.getTopology();
                    if ( topology == null )
                    {
                        NodeGroup nodeGroup = new NodeGroup( nodeType.name(), MongoClusterConfig.TEMPLATE_NAME,
                                Common.DEFAULT_DOMAIN_NAME, 1, 1, 1, new PlacementStrategy( "ROUND_ROBIN" ) );
                        topology = new Topology();
                        topology.addNodeGroupPlacement( manager.getPeerManager().getLocalPeer(), nodeGroup );
                    }
                    envContainerHosts.addAll( manager.getEnvironmentManager()
                                                     .growEnvironment( config.getEnvironmentId(), topology, false ) );
                }
                catch ( EnvironmentModificationException e )
                {
                    logExceptionWithMessage( "Error growing environment", e );
                    return;
                }
                catch ( EnvironmentNotFoundException e )
                {
                    logExceptionWithMessage( "Error getting environment", e );
                }
            }

            ContainerHost johnnyRaw = envContainerHosts.iterator().next();
            switch ( nodeType )
            {
                case CONFIG_NODE:
                    mongoNode = new MongoConfigNodeImpl( johnnyRaw, config.getDomainName(), config.getCfgSrvPort() );
                    break;
                case ROUTER_NODE:
                    mongoNode = new MongoRouterNodeImpl( johnnyRaw, config.getDomainName(), config.getRouterPort(),
                            config.getCfgSrvPort() );
                    break;
                case DATA_NODE:
                    mongoNode = new MongoDataNodeImpl( johnnyRaw, config.getDomainName(), config.getDataNodePort() );
                    break;
            }
            //            mongoNode.start( config );
            config.addNode( mongoNode, nodeType );
            manager.subscribeToAlerts( mongoNode.getContainerHost() );
            trackerOperation.addLog( "Lxc container created successfully\nConfiguring cluster..." );
        }
        catch ( MonitorException e )
        {
            logExceptionWithMessage( String.format( "Couldn't subscribe container host (%s) for alert notifications.",
                    mongoNode.getContainerHost().getHostname() ), e );
            return;
        }

        boolean result = true;
        //add node
        if ( nodeType == NodeType.DATA_NODE )
        {
            result = addDataNode( config, ( MongoDataNode ) mongoNode );
        }
        else if ( nodeType == NodeType.ROUTER_NODE )
        {
            result = addRouter( config, ( MongoRouterNode ) mongoNode );
        }

        if ( result )
        {
            trackerOperation.addLog( "Updating cluster information in database..." );

            Gson GSON = new GsonBuilder().serializeNulls().excludeFieldsWithoutExposeAnnotation().create();
            String json = GSON.toJson( config.prepare() );
            manager.getPluginDAO().saveInfo( MongoClusterConfig.PRODUCT_KEY, config.getClusterName(), json );
            trackerOperation.addLogDone( "Cluster information updated in database" );
        }
        else
        {
            trackerOperation.addLogFailed( "Node addition failed" );
        }
    }


    private boolean addDataNode( final MongoClusterConfig config, MongoDataNode newDataNode )
    {

        Set<Host> clusterMembers = new HashSet<>();
        for ( MongoNode mongoNode : config.getAllNodes() )
        {
            clusterMembers.add( mongoNode.getContainerHost() );
        }
        clusterMembers.add( newDataNode.getContainerHost() );
        try
        {
            for ( Host c : clusterMembers )
            {
                c.addIpHostToEtcHosts( config.getDomainName(), clusterMembers, Common.IP_MASK );
            }

            newDataNode.setReplicaSetName( config.getReplicaSetName() );
            trackerOperation.addLog( String.format( "Set replica set name succeeded" ) );
            trackerOperation.addLog( String.format( "Stopping node..." ) );
            newDataNode.stop();
            trackerOperation.addLog( String.format( "Starting node..." ) );
            newDataNode.start( config );

            trackerOperation.addLog( String.format( "Data node started successfully" ) );

            MongoDataNode primaryNode = config.findPrimaryNode();

            if ( primaryNode != null )
            {

                primaryNode.registerSecondaryNode( newDataNode );

                trackerOperation.addLog( String.format( "Secondary node registered successfully." ) );
                return true;
            }
        }
        catch ( SubutaiException e )
        {
            logExceptionWithMessage( "Error applying operations on peer", e );
        }
        return false;
    }


    private boolean addRouter( final MongoClusterConfig config, MongoRouterNode newRouter )
    {

        Set<Host> clusterMembers = new HashSet<>();
        for ( MongoNode mongoNode : config.getAllNodes() )
        {
            clusterMembers.add( mongoNode.getContainerHost() );
        }
        clusterMembers.add( newRouter.getContainerHost() );
        try
        {
            for ( Host c : clusterMembers )
            {
                c.addIpHostToEtcHosts( config.getDomainName(), clusterMembers, Common.IP_MASK );
            }

            trackerOperation.addLog( String.format( "Starting router: %s", newRouter.getHostname() ) );
            newRouter.setConfigServers( config.getConfigServers() );
            newRouter.start( config );
            return true;
        }
        catch ( SubutaiException e )
        {
            logExceptionWithMessage( "Couldn't add router node", e );
        }

        return false;
    }


    private void logExceptionWithMessage( String message, Exception e )
    {
        LOGGER.error( message, e );
        trackerOperation.addLogFailed( message );
    }
}
