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

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.appscale.api.AppScaleInterface;


@Command ( scope = "appscale", name = "stop-cluster", description = "Command to stop cluster" )
public class StopClusterCommand extends OsgiCommandSupport
{
    @Argument ( index = 0, name = "clusterName", description = "Name of the cluster", required = true, multiValued = false )
    String clusterName = null;

    private AppScaleInterface appScaleInterface;
    private Tracker tracker;


    public void setclusterName ( String clusterName )
    {
        this.clusterName = clusterName;
    }


    public void setAppScaleInterface ( AppScaleInterface appScaleInterface )
    {
        this.appScaleInterface = appScaleInterface;
    }


    public void setTracker ( Tracker tracker )
    {
        this.tracker = tracker;
    }


    @Override
    protected Object doExecute () throws Exception
    {

        System.out.println ( "Stopping cluster" );
        UUID uuid = appScaleInterface.stopCluster ( clusterName );
        System.out.println ( "Stopping cluster " + StartClusterCommand.operationState ( tracker, uuid ) );
        return null;
    }


}

