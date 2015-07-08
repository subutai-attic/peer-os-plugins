package io.subutai.plugin.mysql.cli;


import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.mysql.api.MySQLC;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * Created by tkila on 5/27/15.
 */
@Command( scope = "mysql", name = "stop-cluster", description = "Stop MySQL Cluster" )
public class StopClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, required = true, multiValued = false, description = "Cluster to be stopped" )
    String clusterName;

    private Tracker tracker;
    private MySQLC manager;


    @Override
    protected Object doExecute() throws Exception
    {
        UUID uuid = manager.stopCluster(clusterName);

        System.out.println(
                "Stop cluster operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
        return null;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public MySQLC getManager()
    {
        return manager;
    }


    public void setManager( final MySQLC manager )
    {
        this.manager = manager;
    }
}
