package org.safehaus.subutai.plugin.hive.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hive.api.Hive;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command :
 *      hive:uninstall-cluster test \ {cluster name}
 */
@Command( scope = "hive", name = "uninstall-cluster", description = "Command to uninstall Hive cluster" )
public class UninstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "Delete cluster" )
    String clusterName;
    private Hive hiveManager;
    private Tracker tracker;


    protected Object doExecute()
    {
        UUID uuid = hiveManager.uninstallCluster( clusterName );
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


    public Hive getHiveManager()
    {
        return hiveManager;
    }


    public void setHiveManager( Hive hiveManager )
    {
        this.hiveManager = hiveManager;
    }
}
