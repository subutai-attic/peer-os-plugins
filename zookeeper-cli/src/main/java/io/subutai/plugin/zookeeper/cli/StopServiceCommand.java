package io.subutai.plugin.zookeeper.cli;


import java.io.IOException;
import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : presto:start-node test \ {cluster name} zookeeper1 \ { container hostname }
 */
@Command( scope = "zookeeper", name = "start-node", description = "Command to start Zookeeper service" )
public class StopServiceCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "Name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "hostname", description = "hostname of the container.", required = true,
            multiValued = false )
    String hostname = null;
    private Zookeeper zookeeperManager;
    private Tracker tracker;


    protected Object doExecute() throws IOException
    {
        UUID uuid = zookeeperManager.stopNode( clusterName, hostname );
        tracker.printOperationLog( ZookeeperClusterConfig.PRODUCT_KEY, uuid, 30000 );
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
