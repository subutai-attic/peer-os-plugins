package io.subutai.plugin.mahout.cli;


import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.mahout.api.Mahout;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : mahout:uninstall-node test \ {cluster name} haddop1 \ {node}
 */
@Command( scope = "mahout", name = "uninstall-node", description = "Command to uninstall node from Mahout cluster" )
public class UninstallNodeCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "node", description = "Delete container", required = true,
            multiValued = false )
    String node = null;
    private Mahout mahoutManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Uninstalling " + node + " node..." );
        UUID uuid = mahoutManager.uninstallNode( clusterName, node );
        System.out.println(
                "Uninstall node operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
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


    public Mahout getMahoutManager()
    {
        return mahoutManager;
    }


    public void setMahoutManager( Mahout mahoutManager )
    {
        this.mahoutManager = mahoutManager;
    }
}
