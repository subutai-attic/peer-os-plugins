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
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.api.AppScaleInterface;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
@Command( scope = "appscale", name = "check-cluster", description = "Returns the current state of cluster" )
public class StatusClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "Name of Cluster", required = true, multiValued = false )
    String clusterName;
    private AppScaleInterface appscale;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    @Override
    protected Object doExecute() throws Exception
    {
        AppScaleConfig appScaleConfig = appscale.getConfig( clusterName );
        UUID uuid = appscale.statusCluster( clusterName );
        TrackerOperationView trackerOperation = tracker.getTrackerOperation( AppScaleConfig.getPRODUCT_NAME(), uuid );
        System.out.println( "Status: " + trackerOperation.getState().name().toLowerCase() );
        return null;
    }

}

