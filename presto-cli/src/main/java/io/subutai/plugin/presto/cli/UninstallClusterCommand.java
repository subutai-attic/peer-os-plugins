package io.subutai.plugin.presto.cli;


import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.presto.api.Presto;


/**
 * sample command : presto:uninstall-cluster test \ {cluster name}
 */
@Command( scope = "presto", name = "uninstall-cluster", description = "Command to uninstall Presto cluster" )
public class UninstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Presto prestoManager;
    private Tracker tracker;


    protected Object doExecute()
    {
        System.out.println( "Uninstalling " + clusterName + " presto cluster..." );
        if ( prestoManager.getCluster( clusterName ) == null )
        {
            System.out.println( "There is no " + clusterName + " cluster saved in database." );
            return null;
        }
        UUID uuid = prestoManager.uninstallCluster( clusterName );
        System.out
                .println( "Uninstall operation is " + StartAllNodesCommand.waitUntilOperationFinish( tracker, uuid ) );
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


    public Presto getPrestoManager()
    {
        return prestoManager;
    }


    public void setPrestoManager( Presto prestoManager )
    {
        this.prestoManager = prestoManager;
    }
}
