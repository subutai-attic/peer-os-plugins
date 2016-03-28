package io.subutai.plugin.mongodb.api;


import java.util.UUID;

import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.CompleteEvent;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeState;
import io.subutai.core.plugincommon.impl.AbstractNodeOperationTask;


public class MongoNodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final EnvironmentContainerHost containerHost;
    private final Mongo mongo;
    private NodeOperationType operationType;
    private NodeType nodeType;
    private Tracker tracker;
    private CompleteEvent completeEvent;


    public MongoNodeOperationTask( Mongo mongo, Tracker tracker, String clusterName,
                                   EnvironmentContainerHost containerHost, NodeOperationType operationType,
                                   NodeType nodeType, CompleteEvent completeEvent, UUID trackID )
    {
        super( tracker, mongo.getCluster( clusterName ), completeEvent, trackID, containerHost );
        this.mongo = mongo;
        this.clusterName = clusterName;
        this.containerHost = containerHost;
        this.operationType = operationType;
        this.nodeType = nodeType;
        this.tracker = tracker;
        this.completeEvent = completeEvent;
    }


    @Override
    public UUID runTask()
    {
        UUID trackID = null;
        switch ( operationType )
        {
            case START:
                trackID = mongo.startNode( clusterName, containerHost.getHostname(), nodeType );
                break;
            case STOP:
                trackID = mongo.stopNode( clusterName, containerHost.getHostname(), nodeType );
                break;
            case STATUS:
                trackID = mongo.checkNode( clusterName, containerHost.getHostname(), nodeType );
                break;
        }
        return trackID;
    }


    @Override
    public void waitUntilOperationFinish( UUID trackID )
    {
        NodeState state = NodeState.UNKNOWN;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( MongoClusterConfig.PRODUCT_KEY, trackID );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().toLowerCase().contains( "not" ) )
                    {
                        state = NodeState.STOPPED;
                    }
                    // TODO : change here
                    else if ( po.getLog().toLowerCase().contains( NodeState.RUNNING.name().toLowerCase() ) )
                    {
                        state = NodeState.RUNNING;
                    }
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
            if ( System.currentTimeMillis() - start > ( 30 + 3 ) * 1000 )
            {
                break;
            }
        }

        completeEvent.onComplete( state );
    }


    @Override
    public String getProductStoppedIdentifier()
    {
        return null;
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return null;
    }
}
