package org.safehaus.subutai.plugin.mongodb.api;


import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.CompleteEvent;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.impl.AbstractNodeOperationTask;


public class MongoNodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final ContainerHost containerHost;
    private final Mongo mongo;
    private NodeOperationType operationType;
    private NodeType nodeType;


    public MongoNodeOperationTask( Mongo mongo, Tracker tracker, String clusterName, ContainerHost containerHost,
                                   NodeOperationType operationType, NodeType nodeType, CompleteEvent completeEvent, UUID trackID )
    {
        super( tracker, mongo.getCluster( clusterName ), completeEvent, trackID, containerHost );
        this.mongo = mongo;
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
            case START:
                trackID = mongo.startNode( clusterName, containerHost.getHostname(), nodeType );
                break;
            case STOP:
                trackID = mongo.stopNode( clusterName, containerHost.getHostname() );
                break;
            case STATUS:
                trackID = mongo.checkNode( clusterName, containerHost.getHostname(), nodeType );
                break;
        }
        return trackID;
    }



    @Override
    public String getProductStoppedIdentifier()
    {
        return null;
    }


    @Override
    public String getProductRunningIdentifier()
    {
        return null;
    }
}
