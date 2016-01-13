/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.api;


import java.util.UUID;

import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.CompleteEvent;
import io.subutai.plugin.common.api.ConfigBase;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.api.NodeState;
import io.subutai.plugin.common.api.NodeType;
import io.subutai.plugin.common.impl.AbstractNodeOperationTask;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class AppScaleNodeOperation extends AbstractNodeOperationTask implements Runnable
{
    private final String clusterName;
    private final EnvironmentContainerHost environmentContainerHost;
    private final AppScaleInterface appscale;
    private NodeOperationType nodeOperationType;
    private NodeType nodeType;


    public AppScaleNodeOperation ( String clusterName, EnvironmentContainerHost environmentContainerHost,
                                   AppScaleInterface appscale, NodeOperationType nodeOperationType, NodeType nodeType,
                                   Tracker tracker, ConfigBase clusterConfig, CompleteEvent completeEvent, UUID trackID,
                                   ContainerHost containerHost )
    {
        super ( tracker, clusterConfig, completeEvent, trackID, containerHost );
        this.clusterName = clusterName;
        this.environmentContainerHost = environmentContainerHost;
        this.appscale = appscale;
        this.nodeOperationType = nodeOperationType;
        this.nodeType = nodeType;
    }


    @Override
    public UUID runTask ()
    {
        UUID returnID = null;

        switch ( nodeOperationType )
        {
            case START:
            {
                returnID = appscale.startCluster ( clusterName );
                break;
            }
            case STOP:
            {
                returnID = appscale.stopCluster ( clusterName );
                break;
            }
            case STATUS:
            {
                returnID = appscale.statusCluster ( clusterName );
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

