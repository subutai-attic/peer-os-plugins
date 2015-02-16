package org.safehaus.subutai.plugin.spark.api;


import java.util.UUID;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.CompleteEvent;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.impl.AbstractNodeOperationTask;


public class NodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final ContainerHost containerHost;
    private final Spark spark;
    private NodeOperationType operationType;
    private boolean isMaster;


    public NodeOperationTask( Spark spark, Tracker tracker, String clusterName, ContainerHost containerHost,
                              NodeOperationType operationType, boolean isMaster, CompleteEvent completeEvent, UUID trackID )
    {
        super( tracker, spark.getCluster( clusterName ), completeEvent, trackID, containerHost );
        this.spark = spark;
        this.clusterName = clusterName;
        this.containerHost = containerHost;
        this.operationType = operationType;
        this.isMaster = isMaster;
    }


    @Override
    public UUID runTask()
    {
        UUID trackID = null;
        switch ( operationType )
        {
            case START:
                trackID = spark.startNode( clusterName, containerHost.getHostname(), isMaster );
                break;
            case STOP:
                trackID = spark.stopNode( clusterName, containerHost.getHostname(), isMaster );
                break;
            case STATUS:
                trackID = spark.checkNode( clusterName, containerHost.getHostname(), isMaster );
                break;
        }
        return trackID;
    }


    @Override
    public String getProductStoppedIdentifier()
    {
        return "not running";
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return "running";
    }
}
