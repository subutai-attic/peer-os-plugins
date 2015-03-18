package org.safehaus.subutai.plugin.pig.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.pig.api.Pig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : pig:uninstall-cluster test \ {cluster name}
 */
@Command( scope = "pig", name = "uninstall-cluster", description = "Command to uninstall Pig cluster" )
public class UninstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Pig pigManager;
    private Tracker tracker;


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Pig getPigManager()
    {
        return pigManager;
    }


    public void setPigManager( Pig pigManager )
    {
        this.pigManager = pigManager;
    }


    protected Object doExecute()
    {
        UUID uuid = pigManager.uninstallCluster( clusterName );

        System.out.println(
                "Uninstall operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }
}
