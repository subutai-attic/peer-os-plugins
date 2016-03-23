/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.cli;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.appscale.api.AppScaleInterface;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
@Command ( scope = "appscale", name = "uninstall-cluster", description = "Uninstall Cluster" )
public class UninstallClusterCommand extends OsgiCommandSupport
{
    @Argument ( index = 0, name = "clusterName", description = "name of cluster", required = true, multiValued = false )
    private String clusterName = null;
    @Argument ( index = 1, name = "domainName", description = "name of domain", required = true, multiValued = false )
    private String domainName = null;
    @Argument ( index = 2, name = "environmentId", description = "environment id", required = true, multiValued = false )
    private String environmentId;

    private AppScaleInterface appScaleInterface;
    private Tracker tracker;
    private static final Logger LOG = LoggerFactory.getLogger ( UninstallClusterCommand.class.getName () );


    public String getclusterName ()
    {
        return clusterName;
    }


    public void setclusterName ( String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getDomainName ()
    {
        return domainName;
    }


    public void setDomainName ( String domainName )
    {
        this.domainName = domainName;
    }


    public String getEnvironmentId ()
    {
        return environmentId;
    }


    public void setEnvironmentId ( String environmentId )
    {
        this.environmentId = environmentId;
    }


    @Override
    protected Object doExecute () throws Exception
    {
        System.out.println ( "Uninstall started.." );
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


}

