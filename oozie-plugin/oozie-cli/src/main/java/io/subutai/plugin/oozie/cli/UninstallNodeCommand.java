package io.subutai.plugin.oozie.cli;


import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.oozie.api.Oozie;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : oozie:uninstall-node test \ {cluster name} haddop1 \ {node}
 */
@Command( scope = "oozie", name = "uninstall-node", description = "Command to uninstall node from Oozie cluster" )
public class UninstallNodeCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "node", description = "Delete container", required = true,
            multiValued = false )
    String node = null;
    private Oozie oozieManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Uninstalling " + node + " node..." );
        UUID uuid = oozieManager.destroyNode( clusterName, node );
        System.out.println(
                "Uninstall node operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
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


    public Oozie getOozieManager()
    {
        return oozieManager;
    }


    public void setOozieManager( Oozie oozieManager )
    {
        this.oozieManager = oozieManager;
    }
}
