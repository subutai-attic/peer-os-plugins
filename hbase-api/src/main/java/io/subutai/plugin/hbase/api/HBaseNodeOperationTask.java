package io.subutai.plugin.hbase.api;


import java.util.UUID;

import io.subutai.common.peer.ContainerHost;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.api.NodeState;
import io.subutai.plugin.common.api.NodeType;
import io.subutai.plugin.common.impl.AbstractNodeOperationTask;


public class HBaseNodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final ContainerHost containerHost;
    private NodeOperationType operationType;
    private HBase hbase;
    private NodeType nodeType;


    public HBaseNodeOperationTask( HBase hbase, Tracker tracker, String clusterName, ContainerHost containerHost,
                                   NodeOperationType operationType, NodeType nodeType,
                                   io.subutai.plugin.common.api.CompleteEvent completeEvent, UUID trackID )
    {
        super( tracker, hbase.getCluster( clusterName ), completeEvent, trackID, containerHost );
        this.hbase = hbase;
        this.clusterName = clusterName;
        this.containerHost = containerHost;
        this.operationType = operationType;
        this.nodeType = nodeType;
    }


    @Override
    public UUID runTask()
    {
        UUID trackID = null;
        switch ( operationType )
        {
            case STATUS:
                trackID = hbase.checkNode( clusterName, containerHost.getHostname() );
                break;
            case START:
                break;
            case STOP:
                break;
        }
        return trackID;
    }


    @Override
    public String getProductStoppedIdentifier()
    {
        return "NOT running";
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return NodeState.RUNNING.name();
    }
}
