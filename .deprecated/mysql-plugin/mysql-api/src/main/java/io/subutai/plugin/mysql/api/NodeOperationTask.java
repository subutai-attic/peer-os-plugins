package io.subutai.plugin.mysql.api;


import java.util.UUID;
import java.util.logging.Logger;

import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.CompleteEvent;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeState;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.core.plugincommon.impl.AbstractNodeOperationTask;



public class NodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final ContainerHost containerHost;
    private NodeOperationType operationType;
    private MySQLC mySQLC;
    private NodeType type;
    private Tracker tracker;
    private CompleteEvent completeEvent;
    private static final Logger LOG = Logger.getLogger( NodeOperationTask.class.getName() );


    public NodeOperationTask( MySQLC mySQLC, Tracker tracker, String clusterName, ContainerHost containerHost,
                              NodeOperationType operationType, CompleteEvent completeEvent, UUID trackID,
                              NodeType type )
    {
        super( tracker, mySQLC.getCluster( clusterName ), completeEvent, trackID, containerHost );

        this.type = type;
        this.clusterName = clusterName;
        this.containerHost = containerHost;
        this.operationType = operationType;
        this.mySQLC = mySQLC;
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
                trackID = mySQLC.startService( clusterName, containerHost, type );
                break;
            case STOP:
                trackID = mySQLC.stopService( clusterName, containerHost, type );
                break;
            case STATUS:
                trackID = mySQLC.statusService( clusterName, containerHost, type );
                break;
            case DESTROY:
                trackID = mySQLC.destroyService( clusterName, containerHost, type );
                break;

        }

        LOG.info( String.format( "%s. Cluster name:%s at container:%s with node type:%s and UUID:%s",
                operationType.name(), clusterName, containerHost.getHostname(), type.name(), trackID.toString() ) );
        return trackID;
    }


    @Override
    public void waitUntilOperationFinish( UUID trackID )
    {
        NodeState state = NodeState.UNKNOWN;
        long start = System.currentTimeMillis();
        LOG.info( String.format( "Waiting till operation finishes with UUID: %s", trackID ) );
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( MySQLClusterConfig.PRODUCT_KEY, trackID );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().toLowerCase().contains( getProductStoppedIdentifier().toLowerCase() ) )
                    {
                        state = NodeState.STOPPED;
                    }
                    else if ( po.getLog().toLowerCase().contains( getProductRunningIdentifier().toLowerCase() ) )
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
        return type.name() + " service is not running on node " + containerHost.getHostname();
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return type.name() + " service is running on node " + containerHost.getHostname();
    }
}
