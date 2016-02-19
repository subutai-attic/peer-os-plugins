package io.subutai.plugin.hbase.api;


import java.util.UUID;

import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeState;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.core.plugincommon.impl.AbstractNodeOperationTask;


public class HBaseNodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final EnvironmentContainerHost containerHost;
    private NodeOperationType operationType;
    private HBase hbase;
    private NodeType nodeType;


    public HBaseNodeOperationTask( HBase hbase, Tracker tracker, String clusterName,
                                   EnvironmentContainerHost containerHost, NodeOperationType operationType,
                                   NodeType nodeType, io.subutai.core.plugincommon.api.CompleteEvent completeEvent,
                                   UUID trackID )
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
