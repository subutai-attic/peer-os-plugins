package org.safehaus.subutai.plugin.oozie.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.oozie.api.Oozie;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command :
 *      oozie:uninstall-cluster test \ {cluster name}
 */
@Command( scope = "oozie", name = "uninstall-cluster", description = "Command to uninstall Oozie cluster" )
public class UninstallClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "Delete cluster" )
    String clusterName;
    private Oozie oozieManager;
    private Tracker tracker;


    protected Object doExecute()
    {
        UUID uuid = oozieManager.uninstallCluster( clusterName );
        System.out.println(
                "Uninstall operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Oozie getOozieManager()
    {
        return oozieManager;
    }


    public void setOozieManager( Oozie oozieManager )
    {
        this.oozieManager = oozieManager;
    }
}
