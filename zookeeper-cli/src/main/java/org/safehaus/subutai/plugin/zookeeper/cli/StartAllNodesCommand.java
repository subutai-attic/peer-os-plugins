package org.safehaus.subutai.plugin.zookeeper.cli;


import java.io.IOException;
import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : zookeeper:start-cluster test \ {cluster name}
 */
@Command( scope = "zookeeper", name = "start-cluster", description = "Command to start Zookeeper cluster" )
public class StartAllNodesCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Zookeeper zookeeperManager;
    private Tracker tracker;


    protected Object doExecute() throws IOException
    {
        System.out.println( "Starting " + clusterName + " zookeeper cluster..." );
        UUID uuid = zookeeperManager.startAllNodes( clusterName );
        System.out.println(
                "Start cluster operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
        return null;
    }


    public Zookeeper getZookeeperManager()
    {
        return zookeeperManager;
    }


    public void setZookeeperManager( final Zookeeper zookeeperManager )
    {
        this.zookeeperManager = zookeeperManager;
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
