package org.safehaus.subutai.plugin.mongodb.api;


import java.util.UUID;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.CompleteEvent;
import org.safehaus.subutai.plugin.common.api.NodeState;


public class MongoClusterOperationTask implements Runnable
{
    private final String clusterName;
    private final Mongo mongo;
    private ClusterOperationType operationType;
    private Tracker tracker;
    private CompleteEvent completeEvent;

    public MongoClusterOperationTask( Mongo mongo, Tracker tracker, String clusterName,
                                   ClusterOperationType operationType, CompleteEvent completeEvent, UUID trackID )
    {
        this.mongo = mongo;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.tracker = tracker;
        this.completeEvent = completeEvent;
    }


    @Override
    public void run()
    {
        UUID uuid = null;
        switch ( operationType )
        {
            case START_ALL:
                uuid = mongo.startAllNodes( clusterName );
                break;
            case STOP_ALL:
                uuid = mongo.stopAllNodes( clusterName );
                break;
        }
        waitUntilOperationFinish( uuid );
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
}
