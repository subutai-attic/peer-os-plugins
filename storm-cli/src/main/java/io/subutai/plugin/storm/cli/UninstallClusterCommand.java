package io.subutai.plugin.storm.cli;


import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.storm.api.Storm;


@Command( scope = "storm", name = "uninstall-cluster", description = "Command to uninstall storm cluster" )
public class UninstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "Delete cluster" )
    String clusterName;

    private Tracker tracker;
    private Storm stormManager;


    protected Object doExecute()
    {
        UUID uuid = stormManager.uninstallCluster( clusterName );
        System.out.println(
                "Uninstall operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setStormManager( final Storm stormManager )
    {
        this.stormManager = stormManager;
    }
}
