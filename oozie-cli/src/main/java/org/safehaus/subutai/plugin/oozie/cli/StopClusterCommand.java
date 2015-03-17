package org.safehaus.subutai.plugin.oozie.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.oozie.api.Oozie;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command :
 *      oozie:stop-cluster test \ {cluster name} haddop1 \ {server}
 */
@Command(scope = "oozie", name = "stop-node", description = "Command to stop node of Oozie cluster")
public class StopClusterCommand extends OsgiCommandSupport
{
    @Argument(index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false)
    String clusterName = null;
    @Argument( index = 1, name = "server", description = "The hostname of server container", required = true,
            multiValued = false )
    String server = null;
    private Oozie oozieManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Stopping " + clusterName + " oozie cluster..." );
        UUID uuid = oozieManager.stopNode( clusterName, server );
        System.out.println( "Stop cluster operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) ) ;
        return null;
    }


    public Oozie getOozieManager()
    {
        return oozieManager;
    }


    public void setOozieManager( final Oozie oozieManager )
    {
        this.oozieManager = oozieManager;
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