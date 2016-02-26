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


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
@Command ( scope = "appscale", name = "start-cluster", description = "Command to start AppScale cluster" )
public class StartClusterCommand extends OsgiCommandSupport
{
    @Argument ( index = 0, name = "clusterName", description = "The name of the cluster.", required = true, multiValued = false )
    String clusterName = null;

    private AppScaleInterface appScaleInterface;
    private Tracker tracker;


    public void setClusterName ( String clusterName )
    {
        this.clusterName = clusterName;
    }


    public void setTracker ( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setAppscale ( AppScaleInterface appscale )
    {
        this.appScaleInterface = appscale;
    }


    public AppScaleInterface getAppScaleInterface ()
    {
        return appScaleInterface;
    }


    public void setAppScaleInterface ( AppScaleInterface appScaleInterface )
    {
        this.appScaleInterface = appScaleInterface;
    }


    @Override
    protected Object doExecute () throws Exception
    {
        // AppScaleConfig appScaleConfig = appscaleInterface.getConfig( clusterName );
        System.out.println ( "Starting Cluster ... " );
        UUID uuid = appScaleInterface.startCluster ( clusterName );
        System.out.println ( "Starting cluster " + operationState ( tracker, uuid ) );
        return null;
    }


    protected static OperationState operationState ( Tracker tracker, UUID uuid )
    {
        OperationState os = null;

        while ( !Thread.interrupted () )
        {
            TrackerOperationView trackerOperationView = tracker.getTrackerOperation ( AppScaleConfig.getPRODUCT_NAME (),
                                                                                      uuid );
            if ( trackerOperationView != null )
            {
                os = trackerOperationView.getState ();
                break;
            }
            try
            {
                Thread.sleep ( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
        }

        return os;
    }
}

