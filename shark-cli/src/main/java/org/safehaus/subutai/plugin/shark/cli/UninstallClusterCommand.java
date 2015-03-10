package org.safehaus.subutai.plugin.shark.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.shark.api.Shark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : shark:uninstall-cluster test \ {cluster name}
 */
@Command(scope = "shark", name = "uninstall-cluster", description = "Command to uninstall Shark cluster")
public class UninstallClusterCommand extends OsgiCommandSupport
{

    @Argument(index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false)
    String clusterName = null;
    private Shark sharkManager;
    private Tracker tracker;
    private static final Logger LOG = LoggerFactory.getLogger( UninstallClusterCommand.class.getName() );


    protected Object doExecute()
    {
        System.out.println( "Uninstalling " + clusterName + " presto cluster..." );
        if ( sharkManager.getCluster( clusterName ) == null )
        {
            System.out.println( "There is no " + clusterName + " cluster saved in database." );
            return null;
        }
        UUID uuid = sharkManager.uninstallCluster( clusterName );
        System.out
                .println( "Uninstall operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
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


    public Shark getSharkManager()
    {
        return sharkManager;
    }


    public void setSharkManager( Shark sharkManager )
    {
        this.sharkManager = sharkManager;
    }
}
