/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.cli;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.peer.Peer;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.usergrid.api.UsergridInterface;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
@Command ( scope = "usergrid", name = "install-cluster", description = "Install Cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument ( index = 0, name = "clusterName", description = "name of cluster", required = true, multiValued = false )
    private String clusterName = null;

    private UsergridInterface userGridInterface;
    private Tracker tracker;
    private Peer peer;
    private static final Logger LOG = LoggerFactory.getLogger ( InstallClusterCommand.class.getName () );


    @Override
    protected Object doExecute () throws Exception
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    public UsergridInterface getUserGridInterface ()
    {
        return userGridInterface;
    }


    public void setUserGridInterface ( UsergridInterface userGridInterface )
    {
        this.userGridInterface = userGridInterface;
    }


    public Tracker getTracker ()
    {
        return tracker;
    }


    public void setTracker ( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Peer getPeer ()
    {
        return peer;
    }


    public void setPeer ( Peer peer )
    {
        this.peer = peer;
    }


    public String getClusterName ()
    {
        return clusterName;
    }


    public void setClusterName ( String clusterName )
    {
        this.clusterName = clusterName;
    }


}

