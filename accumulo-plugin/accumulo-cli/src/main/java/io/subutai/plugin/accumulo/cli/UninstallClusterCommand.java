package io.subutai.plugin.accumulo.cli;


import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.accumulo.api.Accumulo;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "accumulo", name = "uninstall-cluster", description = "Command to uninstall HBase cluster" )
public class UninstallClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "Delete cluster")
    String clusterName;

    private Accumulo accumuloManager;
    private Tracker tracker;


    protected Object doExecute()
    {
        UUID uuid = accumuloManager.uninstallCluster( clusterName );
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


    public Accumulo getAccumuloManager()
    {
        return accumuloManager;
    }


    public void setAccumuloManager( final Accumulo accumuloManager )
    {
        this.accumuloManager = accumuloManager;
    }
}
