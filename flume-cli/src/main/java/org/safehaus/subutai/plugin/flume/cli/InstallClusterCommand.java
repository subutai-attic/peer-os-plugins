package org.safehaus.subutai.plugin.flume.cli;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.flume.api.Flume;
import org.safehaus.subutai.plugin.flume.api.FlumeConfig;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "flume", name = "install-cluster", description = "Command to install Flume cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "flume cluster "
            + "name" )
    String clusterName;

    @Argument( index = 1, name = "hadoopClusterName", description = "The hostname list of worker nodes", required =
            true,
            multiValued = false )
    String hadoopClusterName;

    @Argument( index = 2, name = "nodes", description = "The list of nodes that Flume will be installed", required = true,
            multiValued = false )
    String nodes[];


    private Tracker tracker;
    private Flume flumeManager;
    private Hadoop hadoopManager;


    protected Object doExecute()
    {
        FlumeConfig config = new FlumeConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setEnvironmentId( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );

        Set<UUID> nodesSet = new HashSet<>();
        for ( String uuid : nodes ){
            nodesSet.add( UUID.fromString( uuid ) );
        }
        config.setNodes( nodesSet );
        UUID uuid = flumeManager.installCluster( config );
        System.out.println(
                "Install operation is " + StartNodeCommand.waitUntilOperationFinish( tracker, uuid ) + "." );
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


    public Flume getFlumeManager()
    {
        return flumeManager;
    }


    public void setFlumeManager( final Flume flumeManager )
    {
        this.flumeManager = flumeManager;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}
