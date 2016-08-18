package io.subutai.plugin.hadoop.api;


import java.util.UUID;

import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeState;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.core.plugincommon.impl.AbstractNodeOperationTask;
import io.subutai.core.tracker.api.Tracker;


public class HadoopNodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final EnvironmentContainerHost containerHost;
    private final Hadoop hadoop;
    private NodeOperationType operationType;
    private final NodeType nodeType;


    public HadoopNodeOperationTask( Hadoop hadoop, Tracker tracker, String clusterName,
                                    EnvironmentContainerHost containerHost, NodeOperationType operationType,
                                    NodeType nodeType, io.subutai.core.plugincommon.api.CompleteEvent completeEvent,
                                    UUID trackID )
    {
        super( tracker, hadoop.getCluster( clusterName ), completeEvent, trackID, containerHost );
        this.hadoop = hadoop;
        this.clusterName = clusterName;
        this.containerHost = containerHost;
        this.operationType = operationType;
        this.nodeType = nodeType;
    }


    @Override
    public UUID runTask()
    {
        UUID trackID = null;
        switch ( nodeType )
        {
            case NAMENODE:
                switch ( operationType )
                {
                    case START:
//                        trackID = hadoop.startNameNode( hadoop.getCluster( clusterName ) );
                        break;
                    case STOP:
//                        trackID = hadoop.stopNameNode( hadoop.getCluster( clusterName ) );
                        break;
                    case STATUS:
//                        trackID = hadoop.statusNameNode( hadoop.getCluster( clusterName ) );
                        break;
                }
                break;
            case JOBTRACKER:
                switch ( operationType )
                {
                    case START:
//                        trackID = hadoop.startJobTracker( hadoop.getCluster( clusterName ) );
                        break;
                    case STOP:
//                        trackID = hadoop.stopJobTracker( hadoop.getCluster( clusterName ) );
                        break;
                    case STATUS:
//                        trackID = hadoop.statusJobTracker( hadoop.getCluster( clusterName ) );
                        break;
                }
                break;
            case SECONDARY_NAMENODE:
                switch ( operationType )
                {
                    case STATUS:
//                        trackID = hadoop.statusSecondaryNameNode( hadoop.getCluster( clusterName ) );
                        break;
                }
                break;
            case DATANODE:
                switch ( operationType )
                {
                    case START:
                        trackID = hadoop.startDataNode( hadoop.getCluster( clusterName ), containerHost.getHostname() );
                        break;
                    case STOP:
                        trackID = hadoop.stopDataNode( hadoop.getCluster( clusterName ), containerHost.getHostname() );
                        break;
                    case STATUS:
//                        trackID =
//                                hadoop.statusDataNode( hadoop.getCluster( clusterName ), containerHost.getHostname() );
                        break;
                }
                break;
            case TASKTRACKER:
                switch ( operationType )
                {
                    case START:
//                        trackID = hadoop.startTaskTracker( hadoop.getCluster( clusterName ),
//                                containerHost.getHostname() );
                        break;
                    case STOP:
//                        trackID =
//                                hadoop.stopTaskTracker( hadoop.getCluster( clusterName ), containerHost.getHostname() );
                        break;
                    case STATUS:
//                        trackID = hadoop.statusTaskTracker( hadoop.getCluster( clusterName ),
//                                containerHost.getHostname() );
                        break;
                }
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
