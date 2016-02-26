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

import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.api.AppScaleInterface;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
@Command ( scope = "appscale", name = "check-cluster", description = "Returns the current state of cluster" )
public class StatusClusterCommand extends OsgiCommandSupport
{
    @Argument ( index = 0, name = "clusterName", description = "Name of Cluster", required = true, multiValued = false )
    String clusterName = null;

    private AppScaleInterface appScaleInterface;
    private Tracker tracker;


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
        AppScaleConfig appScaleConfig = appScaleInterface.getConfig ( clusterName );
        UUID uuid = appScaleInterface.statusCluster ( clusterName );
        TrackerOperationView trackerOperation = tracker.getTrackerOperation ( AppScaleConfig.getPRODUCT_NAME (), uuid );
        System.out.println ( "Status: " + trackerOperation.getState ().name ().toLowerCase () );
        return null;
    }


    public void setClusterName ( String clusterName )
    {
        this.clusterName = clusterName;
    }


    public void setTracker ( Tracker tracker )
    {
        this.tracker = tracker;
    }


}

