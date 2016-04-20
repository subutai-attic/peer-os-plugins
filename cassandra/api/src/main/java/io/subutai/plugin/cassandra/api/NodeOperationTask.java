package io.subutai.plugin.cassandra.api;


import java.util.UUID;

import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.CompleteEvent;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.impl.AbstractNodeOperationTask;


public class NodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final EnvironmentContainerHost containerHost;
    private final Cassandra cassandra;
    private NodeOperationType operationType;


    public NodeOperationTask( Cassandra cassandra, Tracker tracker, String clusterName,
                              EnvironmentContainerHost containerHost, NodeOperationType operationType,
                              CompleteEvent completeEvent, UUID trackID )
    {
        super( tracker, cassandra.getCluster( clusterName ), completeEvent, trackID, containerHost );
        this.cassandra = cassandra;
        this.clusterName = clusterName;
        this.containerHost = containerHost;
        this.operationType = operationType;
    }


    @Override
    public UUID runTask()
    {
        UUID trackID = null;
        switch ( operationType )
        {
            case START:
                trackID = cassandra.startService( clusterName, containerHost.getHostname() );
                break;
            case STOP:
                trackID = cassandra.stopService( clusterName, containerHost.getHostname() );
                break;
            case STATUS:
                trackID = cassandra.statusService( clusterName, containerHost.getHostname() );
                break;
        }
        return trackID;
    }


    @Override
    public String getProductStoppedIdentifier()
    {
        return "cassandra is not running";
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return "cassandra is running";
    }
}
