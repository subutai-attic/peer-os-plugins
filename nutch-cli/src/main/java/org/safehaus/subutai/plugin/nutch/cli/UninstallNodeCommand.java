package org.safehaus.subutai.plugin.nutch.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.nutch.api.Nutch;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : nutch:add-node test \ {cluster name} haddop1 \ {node}
 */
@Command( scope = "nutch", name = "add-node", description = "Command to uninstall node from Nutch cluster" )
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
                "Start cluster operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
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


    public Nutch getNutchManager()
    {
        return nutchManager;
    }


    public void setNutchManager( Nutch nutchManager )
    {
        this.nutchManager = nutchManager;
    }
}
