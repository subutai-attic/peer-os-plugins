package io.subutai.plugin.storm.cli;


import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.storm.api.Storm;


@Command( scope = "storm", name = "start-cluster", description = "Stops cluster" )
public class StopClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Tracker tracker;
    private Storm stormManager;


    @Override
    protected Object doExecute() throws Exception
    {
        UUID uuid = stormManager.stopAll( clusterName );
        System.out.println(
                "Stop cluster operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setStormManager( final Storm stormManager )
    {
        this.stormManager = stormManager;
    }
}
