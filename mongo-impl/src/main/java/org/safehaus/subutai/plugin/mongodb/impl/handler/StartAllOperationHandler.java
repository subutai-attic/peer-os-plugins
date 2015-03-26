//package org.safehaus.subutai.plugin.mongodb.impl.handler;
//
//
//import java.util.UUID;
//
//import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
//import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
//import org.safehaus.subutai.common.peer.ContainerHost;
//import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
//import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
//import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
//import org.safehaus.subutai.plugin.mongodb.api.MongoException;
//import org.safehaus.subutai.plugin.mongodb.api.MongoNode;
//import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;
//
//
//public class StartAllOperationHandler extends AbstractOperationHandler<MongoImpl, MongoClusterConfig>
//{
//    private ClusterOperationType operationType;
//
//
//    public StartAllOperationHandler( final MongoImpl manager, final MongoClusterConfig config,
//                                     ClusterOperationType operationType )
//    {
//        super( manager, config );
//        this.operationType = operationType;
//        trackerOperation = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
//                String.format( "Starting all nodes in %s", clusterName ) );
//    }
//
//
//    @Override
//    public void run()
//    {
//        MongoClusterConfig config = manager.getCluster( clusterName );
//        if ( config == null )
//        {
//            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
//            return;
//        }
//
//        trackerOperation.addLog( "Starting nodes.." );
//
//        for ( UUID uuid : config.getAllNodeIds() )
//        {
//            ContainerHost host = null;
//            MongoNode node = null;
//            try
//            {
//                host = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() ).
//                        getContainerHostById( uuid );
//                node = config.findNode( host.getHostname() );
//                switch ( operationType )
//                {
//
//                    case START_ALL:
//                        node.start( config );
//                        break;
//                    case STOP_ALL:
//                        node.stop();
//                        break;
//                }
//            }
//            catch ( EnvironmentNotFoundException e )
//            {
//                trackerOperation.addLogFailed( "Container host not found" );
//                return;
//            }
//            catch ( ContainerHostNotFoundException e )
//            {
//                trackerOperation
//                        .addLogFailed( "Error getting environment by id: " + config.getEnvironmentId().toString() );
//                return;
//            }
//            catch ( MongoException e )
//            {
//                trackerOperation.addLogFailed( String.format( "Failed to start nodes" ) );
//                return;
//            }
//        }
//    }
//}
