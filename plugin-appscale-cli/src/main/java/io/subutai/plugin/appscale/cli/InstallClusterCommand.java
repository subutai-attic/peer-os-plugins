/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.cli;


import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.api.AppScaleInterface;
import io.subutai.plugin.common.api.NodeState;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
@Command ( scope = "appscale", name = "install-cluster", description = "Install Cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument ( index = 0, name = "clusterName", description = "name of cluster", required = true, multiValued = false )
    String clusterName = null;
    @Argument ( index = 1, name = "domainName", description = "name of domain", required = true, multiValued = false )
    String domainName = null;
    @Argument ( index = 2, name = "environmentId", description = "environment id", required = true, multiValued = false )
    String environmentId;


    private AppScaleInterface appScaleInterface;
    private Tracker tracker;


    @Override
    protected Object doExecute () throws Exception
    {
        AppScaleConfig appScaleConfig = new AppScaleConfig ();
        appScaleConfig.setEnvironmentId ( environmentId );
        appScaleConfig.setClusterName ( clusterName );
        appScaleConfig.setDomainName ( domainName );
        appScaleConfig.setTracker ( clusterName );
        UUID installCluster = appScaleInterface.installCluster ( appScaleConfig );
        System.out.println ( "Installing ..." + StartClusterCommand.operationState ( tracker, installCluster ) );
        UUID startCluster = appScaleInterface.startCluster ( clusterName );
        System.out.println ( "Starting ..." + StartClusterCommand.operationState ( tracker, startCluster ) );
        return null;
    }


    public void setAppScaleInterface ( AppScaleInterface appScaleInterface )
    {
        this.appScaleInterface = appScaleInterface;
    }


    public void setTracker ( Tracker tracker )
    {
        this.tracker = tracker;
    }


    protected static NodeState waitUntilOperationFinish ( Tracker tracker, UUID uuid )
    {
        NodeState state = NodeState.UNKNOWN;
        long start = System.currentTimeMillis ();
        while ( !Thread.interrupted () )
        {
            TrackerOperationView po = tracker.getTrackerOperation ( AppScaleConfig.PRODUCT_NAME, uuid );
            if ( po != null )
            {
                if ( po.getState () != OperationState.RUNNING )
                {
                    if ( po.getLog ().toLowerCase ().contains ( NodeState.STOPPED.name ().toLowerCase () ) )
                    {
                        state = NodeState.STOPPED;
                    }
                    else if ( po.getLog ().toLowerCase ().contains ( NodeState.RUNNING.name ().toLowerCase () ) )
                    {
                        state = NodeState.RUNNING;
                    }

                    System.out.println ( po.getLog () );
                    break;
                }
            }
            try
            {
                Thread.sleep ( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
            if ( System.currentTimeMillis () - start > ( 30 + 3 ) * 1000 )
            {
                break;
            }
        }

        return state;
    }


}

