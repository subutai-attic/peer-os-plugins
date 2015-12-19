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
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.api.AppScaleInterface;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
@Command( scope = "appscale", name = "install-cluster", description = "Install Cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "name of cluster", required = true, multiValued = false )
    String clusterName;
    @Argument( index = 0, name = "domainName", description = "name of domain", required = true, multiValued = false )
    String domainName;
    @Argument( index = 0, name = "environmentId", description = "environment id", required = true, multiValued = false )
    String environmentId;


    AppScaleInterface appScaleInterface;
    Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        AppScaleConfig appScaleConfig = new AppScaleConfig();
        appScaleConfig.setEnvironmentId( environmentId );
        appScaleConfig.setClusterName( clusterName );
        appScaleConfig.setTracker( clusterName );
        UUID installCluster = appScaleInterface.installCluster( appScaleConfig );
        System.out.println( "Installing ..." + StartClusterCommand.operationState( tracker, installCluster ) );
        UUID startCluster = appScaleInterface.startCluster( clusterName );
        System.out.println( "Starting ..." + StartClusterCommand.operationState( tracker, startCluster ) );
        return null;
    }

}

