package io.subutai.plugin.hive.cli;


import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hive.api.Hive;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command :
 *      hive:stop-cluster test \ {cluster name} haddop1 \ {server}
 */
@Command(scope = "hive", name = "stop-node", description = "Command to stop node of Hive cluster")
public class StopClusterCommand extends OsgiCommandSupport
{
    @Argument(index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false)
    String clusterName = null;
    @Argument( index = 1, name = "server", description = "The hostname of server container", required = true,
            multiValued = false )
    String server = null;
    private Hive hiveManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Stopping " + clusterName + " hive cluster..." );
        UUID uuid = hiveManager.stopNode( clusterName, server );
        System.out.println( "Stop cluster operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) ) ;
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