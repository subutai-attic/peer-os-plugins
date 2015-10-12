package io.subutai.plugin.hive.api;


import java.util.UUID;

import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.CompleteEvent;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.impl.AbstractNodeOperationTask;


public class HiveNodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final EnvironmentContainerHost containerHost;
    private final Hive hive;
    private NodeOperationType operationType;


    public HiveNodeOperationTask( Hive hive, Tracker tracker, String clusterName,
                                  EnvironmentContainerHost containerHost, NodeOperationType operationType,
                                  CompleteEvent completeEvent, UUID trackID )
    {
        super( tracker, hive.getCluster( clusterName ), completeEvent, trackID, containerHost );
        this.hive = hive;
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
                trackID = hive.startNode( clusterName, containerHost.getHostname() );
                break;
            case STOP:
                trackID = hive.stopNode( clusterName, containerHost.getHostname() );
                break;
            case STATUS:
                trackID = hive.statusCheck( clusterName, containerHost.getHostname() );
                break;
        }
        return trackID;
    }


    @Override
    public String getProductStoppedIdentifier()
    {
        return "Hive Thrift Server is not running";
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return "Hive Thrift Server is running";
    }
}
