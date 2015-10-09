package io.subutai.plugin.zookeeper.api;


import java.util.UUID;

import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.CompleteEvent;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.impl.AbstractNodeOperationTask;


public class NodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final EnvironmentContainerHost containerHost;
    private final Zookeeper zookeeper;
    private NodeOperationType operationType;


    public NodeOperationTask( Zookeeper zookeeper, Tracker tracker, String clusterName,
                              EnvironmentContainerHost containerHost, NodeOperationType operationType,
                              CompleteEvent completeEvent, UUID trackID )
    {
        super( tracker, zookeeper.getCluster( clusterName ), completeEvent, trackID, containerHost );
        this.zookeeper = zookeeper;
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
                trackID = zookeeper.startNode( clusterName, containerHost.getHostname() );
                break;
            case STOP:
                trackID = zookeeper.stopNode( clusterName, containerHost.getHostname() );
                break;
            case STATUS:
                trackID = zookeeper.checkNode( clusterName, containerHost.getHostname() );
                break;
        }
        return trackID;
    }


    @Override
    public String getProductStoppedIdentifier()
    {
        return "Zookeeper Server is not running";
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return "Zookeeper Server is running";
    }
}
