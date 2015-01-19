package org.safehaus.subutai.plugin.hbase.api;


import java.util.UUID;

import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.api.NodeState;
import org.safehaus.subutai.plugin.common.impl.AbstractNodeOperationTask;


public class HBaseNodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final ContainerHost containerHost;
    private NodeOperationType operationType;
    private HBase hbase;

    public HBaseNodeOperationTask( HBase hbase, Tracker tracker, String clusterName, ContainerHost containerHost,
                                   NodeOperationType operationType,
                                   org.safehaus.subutai.plugin.common.api.CompleteEvent completeEvent, UUID trackID )
    {
        super( tracker, hbase.getCluster( clusterName ), completeEvent, trackID, containerHost );
        this.hbase = hbase;
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
        return NodeState.STOPPED.name();
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return NodeState.RUNNING.name();
    }
}
