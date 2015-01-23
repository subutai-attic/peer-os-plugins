package org.safehaus.subutai.plugin.oozie.api;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.CompleteEvent;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.impl.AbstractNodeOperationTask;

import java.util.UUID;

/**
 * Created by ermek on 1/22/15.
 */
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
//                trackID = oozie.( clusterName, containerHost.getHostname() );
                break;
        }
        return trackID;
    }


    @Override
    public String getProductStoppedIdentifier()
    {
        return "oozie Thrift Server is not running";
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return "oozie Thrift Server is running";
    }
}
