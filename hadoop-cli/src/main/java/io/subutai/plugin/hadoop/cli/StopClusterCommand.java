package io.subutai.plugin.hadoop.cli;


import java.io.IOException;
import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


/**
 * sample command : hadoop:stop-cluster test \ {cluster name}
 */
@Command( scope = "hadoop", name = "stop-cluster", description = "Command to stop Hadoop cluster" )
public class StopClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Hadoop hadoopManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws IOException
    {
        HadoopClusterConfig config = hadoopManager.getCluster( clusterName );
        System.out.println( "Stopping namenode ..." );
        UUID uuid = hadoopManager.stopNameNode( config );

        System.out.println( "Stopping jobtracker ..." );
        UUID uuid2 = hadoopManager.stopJobTracker( config );

        System.out.println(
                "Stop namenode operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
        System.out.println(
                "Stop jobtracker operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid2 ) );
        return null;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}
