package io.subutai.plugin.nutch.cli;


import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.nutch.api.Nutch;


/**
 * sample command : nutch:uninstall-node test \ {cluster name} haddop1 \ {node}
 */
@Command( scope = "nutch", name = "uninstall-node", description = "Command to uninstall node from Nutch cluster" )
public class UninstallNodeCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "node", description = "Delete container", required = true,
            multiValued = false )
    String node = null;
    private Nutch nutchManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Uninstalling " + node + " node..." );
        UUID uuid = nutchManager.destroyNode( clusterName, node );
        System.out.println(
                "Uninstall node operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
        return null;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setNutchManager( Nutch nutchManager )
    {
        this.nutchManager = nutchManager;
    }
}
