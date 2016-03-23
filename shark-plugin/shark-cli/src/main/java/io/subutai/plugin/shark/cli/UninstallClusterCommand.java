package io.subutai.plugin.shark.cli;


import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.shark.api.Shark;


/**
 * sample command : shark:uninstall-cluster test \ {cluster name}
 */
@Command( scope = "shark", name = "uninstall-cluster", description = "Command to uninstall Shark cluster" )
public class UninstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Shark sharkManager;
    private Tracker tracker;


    protected Object doExecute()
    {
        System.out.println( "Uninstalling " + clusterName + " presto cluster..." );
        if ( sharkManager.getCluster( clusterName ) == null )
        {
            System.out.println( "There is no " + clusterName + " cluster saved in database." );
            return null;
        }
        UUID uuid = sharkManager.uninstallCluster( clusterName );
        System.out
                .println( "Uninstall operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
        return null;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setSharkManager( Shark sharkManager )
    {
        this.sharkManager = sharkManager;
    }
}
