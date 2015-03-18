package org.safehaus.subutai.plugin.hive.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hive.api.Hive;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : hive:add-node test \ {cluster name} haddop1 \ {node}
 */
@Command( scope = "hive", name = "add-node", description = "Command to add node to Hive cluster" )
public class AddNodeCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "node", description = "Add container", required = true,
            multiValued = false )
    String node = null;
    private Hive hiveManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Adding " + node + " node..." );
        UUID uuid = hiveManager.addNode( clusterName, node );
        System.out.println(
                "Add node operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
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


    public Hive getHiveManager()
    {
        return hiveManager;
    }


    public void setHiveManager( Hive hiveManager )
    {
        this.hiveManager = hiveManager;
    }
}
