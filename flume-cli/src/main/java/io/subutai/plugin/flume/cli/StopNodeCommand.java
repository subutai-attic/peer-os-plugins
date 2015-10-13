package io.subutai.plugin.flume.cli;


import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.flume.api.Flume;


@Command( scope = "flume", name = "start-cluster", description = "Stops cluster" )
public class StopNodeCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;

    @Argument( index = 1, name = "hostname", description = "The hostname of flume node.", required = true,
            multiValued = false )
    String hostname = null;

    private Tracker tracker;
    private Flume flumeManager;


    @Override
    protected Object doExecute() throws Exception
    {
        UUID uuid = flumeManager.stopNode( clusterName, hostname );
        System.out.println(
                "Stop cluster operation is " + StartNodeCommand.waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setFlumeManager( final Flume flumeManager )
    {
        this.flumeManager = flumeManager;
    }
}
