package io.subutai.plugin.pig.cli;


import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.pig.api.Pig;


/**
 * sample command : pig:uninstall-node test \ {cluster name} haddop1 \ {node}
 */
@Command( scope = "pig", name = "uninstall-node", description = "Command to uninstall node from Pig cluster" )
public class UninstallNodeCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "node", description = "Delete container", required = true,
            multiValued = false )
    String node = null;
    private Pig pigManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Uninstalling " + node + " node..." );
        UUID uuid = pigManager.destroyNode( clusterName, node );
        System.out.println(
                "Uninstall node operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
        return null;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setPigManager( Pig pigManager )
    {
        this.pigManager = pigManager;
    }
}
