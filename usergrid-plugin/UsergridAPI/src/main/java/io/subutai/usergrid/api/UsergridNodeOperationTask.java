/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.usergrid.api;


import java.util.UUID;

import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.plugincommon.api.CompleteEvent;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeState;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.core.plugincommon.impl.AbstractNodeOperationTask;
import io.subutai.core.tracker.api.Tracker;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class UsergridNodeOperationTask extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final EnvironmentContainerHost containerHost;
    private final UsergridInterface usergrid;
    private NodeOperationType operationType;
    private NodeType nodeType;


    public UsergridNodeOperationTask ( String clusterName, EnvironmentContainerHost environmentContainerHost,
                                       UsergridInterface usergrid, NodeOperationType operationType,
                                       NodeType nodeType,
                                       Tracker tracker, ConfigBase clusterConfig, CompleteEvent completeEvent,
                                       UUID trackID,
                                       ContainerHost containerHost )
    {
        super ( tracker, usergrid.getCluster ( clusterName ), completeEvent, trackID, containerHost );
        this.clusterName = clusterName;
        this.containerHost = environmentContainerHost;
        this.usergrid = usergrid;
        this.operationType = operationType;
        this.nodeType = nodeType;
    }


    @Override
    public UUID runTask ()
    {
        UUID returnID = null;

        switch ( operationType )
        {
            case START:
            {
                returnID = usergrid.startCluster ( clusterName );
                break;
            }
            case STOP:
            {
                returnID = usergrid.stopCluster ( clusterName );
                break;
            }
            case STATUS:
            {
                returnID = usergrid.statusCluster ( clusterName );
                break;
            }
        }

        return returnID;
    }


    @Override
    public String getProductStoppedIdentifier ()
    {
        return NodeState.STOPPED.name ();
    }


    @Override
    public String getProductRunningIdentifier ()
    {
        return NodeState.RUNNING.name ();
    }

}

