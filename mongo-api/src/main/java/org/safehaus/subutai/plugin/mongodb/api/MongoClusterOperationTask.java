package org.safehaus.subutai.plugin.mongodb.api;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.CompleteEvent;
import org.safehaus.subutai.plugin.common.api.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MongoClusterOperationTask implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( MongoClusterOperationTask.class );
    private final String clusterName;
    private final Mongo mongo;
    private ClusterOperationType operationType;
    private Tracker tracker;
    private CompleteEvent completeEvent;
    private EnvironmentManager environmentManager;

    public MongoClusterOperationTask( Mongo mongo, Tracker tracker, String clusterName,
                                   ClusterOperationType operationType, CompleteEvent completeEvent,
                                   EnvironmentManager environmentManager, UUID trackID )
    {
        this.mongo = mongo;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.tracker = tracker;
        this.completeEvent = completeEvent;
        this.environmentManager = environmentManager;
    }


    @Override
    public void run()
    {
        MongoClusterConfig config = mongo.getCluster( clusterName );
        try
        {
            Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
            Set<UUID> uuids = new HashSet<>();
            switch ( operationType )
            {
                case START_ALL:
                    for( UUID uuid : config.getConfigHosts() ){
                        uuids.add( mongo.startNode( clusterName, findHost( environment, uuid ).getHostname(),
                                NodeType.CONFIG_NODE ) );
                    }
                    for( UUID uuid : config.getRouterHosts() ){
                        uuids.add( mongo.startNode( clusterName, findHost( environment, uuid ).getHostname(),
                                NodeType.ROUTER_NODE ) );
                    }
                    for( UUID uuid : config.getDataHosts() ){
                        uuids.add( mongo.startNode( clusterName, findHost( environment, uuid ).getHostname(),
                                NodeType.DATA_NODE ) );
                    }
                    break;
                case STOP_ALL:
                    for( UUID uuid : config.getConfigHosts() ){
                        uuids.add( mongo.stopNode( clusterName, findHost( environment, uuid ).getHostname(),
                                NodeType.CONFIG_NODE ) );
                    }
                    for( UUID uuid : config.getRouterHosts() ){
                        uuids.add( mongo.stopNode( clusterName, findHost( environment, uuid ).getHostname(),
                                NodeType.ROUTER_NODE ) );
                    }
                    for( UUID uuid : config.getDataHosts() ){
                        uuids.add( mongo.stopNode( clusterName, findHost( environment, uuid ).getHostname(),
                                NodeType.DATA_NODE ) );
                    }
                    break;
            }
            waitUntilOperationsFinish( uuids );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Could not find environment." );
            e.printStackTrace();
        }

    }


    public void waitUntilOperationsFinish( Set<UUID>  uuidSet )
    {
        for ( UUID uuid : uuidSet ){
            long start = System.currentTimeMillis();
            while ( !Thread.interrupted() )
            {
                TrackerOperationView po = tracker.getTrackerOperation( MongoClusterConfig.PRODUCT_KEY, uuid );
                if ( po != null )
                {
                    if ( po.getState() != OperationState.RUNNING )
                    {
                        break;
                    }
                }
                try
                {
                    Thread.sleep( 1000 );
                }
                catch ( InterruptedException ex )
                {
                    break;
                }
                if ( System.currentTimeMillis() - start > ( 180 ) * 1000 )
                {
                    break;
                }
            }
        }
        completeEvent.onComplete( NodeState.UNKNOWN );
    }

    public void waitUntilOperationFinish( UUID trackID )
    {
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( MongoClusterConfig.PRODUCT_KEY, trackID );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    break;
                }
            }
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
            if ( System.currentTimeMillis() - start > ( 180 ) * 1000 )
            {
                break;
            }
        }
        completeEvent.onComplete( NodeState.UNKNOWN );
    }


    public ContainerHost findHost( Environment environment, UUID uuid ){
        try
        {
            return  environment.getContainerHostById( uuid );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error( "Could not find container host" );
            e.printStackTrace();
        }
        return null;
    }

}
