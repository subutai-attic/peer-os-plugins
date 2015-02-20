package org.safehaus.subutai.plugin.hadoop.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

/**
 * sample command :
 *      hadoop:uninstall-cluster test \ {cluster name}
 */
@Command( scope = "hadoop", name = "uninstall-cluster", description = "Command to uninstall Hadoop cluster" )
public class UninstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Hadoop hadoopManager;
    private Tracker tracker;


    @Override
    protected Object doExecute()
    {
        System.out.println( clusterName + " hadoop cluster will be removed !!!");
        UUID uuid = hadoopManager.uninstallCluster( clusterName );
        System.out.println( "Uninstall cluster operation is " +
                StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
        return null;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
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
