package io.subutai.plugin.presto.cli;


import java.io.IOException;
import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.presto.api.Presto;
import io.subutai.plugin.presto.api.PrestoClusterConfig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

/**
 * sample command :
 *      presto:start-node test \ {cluster name}
 *                        hadoop1 \ { container hostname }
 */
@Command( scope = "presto", name = "start-node", description = "Command to start Presto service" )
public class StartServiceCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "Name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "hostname", description = "UUID of the agent.", required = true, multiValued = false )
    String hostname = null;
    private Presto prestoManager;
    private Tracker tracker;

    protected Object doExecute() throws IOException
    {
        UUID uuid = prestoManager.startNode( clusterName, hostname );
        tracker.printOperationLog( PrestoClusterConfig.PRODUCT_KEY, uuid, 30000 );
        return null;
    }


    public Presto getPrestoManager()
    {
        return prestoManager;
    }


    public void setPrestoManager( final Presto prestoManager )
    {
        this.prestoManager = prestoManager;
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
