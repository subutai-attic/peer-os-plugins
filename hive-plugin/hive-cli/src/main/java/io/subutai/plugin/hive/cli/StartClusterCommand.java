package io.subutai.plugin.hive.cli;


import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.NodeState;
import io.subutai.plugin.hive.api.Hive;


/**
 * sample command : hive:start-cluster test \ {cluster name} haddop1 \ {server}
 */
@Command( scope = "hive", name = "start-node", description = "Command to start node of Hive cluster" )
public class StartClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "server", description = "The hostname of server container", required = true,
            multiValued = false )
    String server = null;
    private Hive hiveManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Starting " + clusterName + " hive cluster..." );
        UUID uuid = hiveManager.startNode( clusterName, server );
        NodeState state = TrackerReader.waitUntilOperationFinish( tracker, uuid );
        System.out.println( "Start cluster operation is " + state );
        return null;
    }


    public Hive getHiveManager()
    {
        return hiveManager;
    }


    public void setHiveManager( final Hive hiveManager )
    {
        this.hiveManager = hiveManager;
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
