package io.subutai.plugin.oozie.api;

import io.subutai.common.peer.ContainerHost;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.CompleteEvent;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.impl.AbstractNodeOperationTask;

import java.util.UUID;


public class OozieNodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final ContainerHost containerHost;
    private final Oozie oozie;
    private NodeOperationType operationType;


    public OozieNodeOperationTask( Oozie oozie, Tracker tracker, String clusterName, ContainerHost containerHost,
                                  NodeOperationType operationType, CompleteEvent completeEvent, UUID trackID )
    {
        super( tracker, oozie.getCluster( clusterName ), completeEvent, trackID, containerHost );
        this.oozie = oozie;
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
                trackID = oozie.startNode( clusterName, containerHost.getHostname() );
                break;
            case STOP:
                trackID = oozie.stopNode( clusterName, containerHost.getHostname() );
                break;
            case STATUS:
                trackID = oozie.checkNode( clusterName, containerHost.getHostname() );
                break;
        }
        return trackID;
    }


    @Override
    public String getProductStoppedIdentifier()
    {
        return "oozie Server is not running";
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return "oozie Server is running";
    }
}
