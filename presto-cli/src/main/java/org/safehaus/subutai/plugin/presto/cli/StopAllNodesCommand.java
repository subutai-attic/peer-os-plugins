package org.safehaus.subutai.plugin.presto.cli;


import java.io.IOException;
import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.presto.api.Presto;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

/**
 * sample command :
 *      hadoop:describe-cluster test \ {cluster name}
 */
@Command(scope = "presto", name = "stop-cluster", description = "Command to stop Presto cluster")
public class StopAllNodesCommand extends OsgiCommandSupport
{

    @Argument(index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false)
    String clusterName = null;
    private Presto prestoManager;
    private Tracker tracker;

    protected Object doExecute() throws IOException
    {
        System.out.println( "Stopping " + clusterName + " presto cluster..." );
        UUID uuid = prestoManager.stopAllNodes( clusterName );
        System.out.println( "Stop cluster operation is " +
                StartAllNodesCommand.waitUntilOperationFinish( tracker, uuid ) ) ;
        return null;
    }


    public Presto getPrestoManager()
    {
        return prestoManager;
    }


    public void setPrestoManager( final Presto prestoManager )
    {
        this.prestoManager = prestoManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }
}
