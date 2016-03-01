package io.subutai.plugin.accumulo.cli;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.accumulo.api.Accumulo;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.hadoop.api.Hadoop;


@Command( scope = "accumulo", name = "install-cluster", description = "Command to install Accumulo cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "accumulo cluster "
            + "name" )
    String clusterName;

    @Argument( index = 1, name = "hadoopClusterName", description = "The hostname list of worker nodes", required =
            true,
            multiValued = false )
    String hadoopClusterName;

    @Argument( index = 2, name = "masterNode", description = "The hostname of Master node", required = true,
            multiValued = false )
    String masterNode;

    @Argument( index = 3, name = "gcNode", description = "The hostname of GC node", required = true,
            multiValued = false )
    String gcNode = null;

    @Argument( index = 4, name = "monitorNode", description = "The hostname of Monitor node", required = true,
            multiValued = false )
    String monitorNode = null;

    @Argument( index = 5, name = "tracers", description = "The list of tracer machines", required = true,
            multiValued = false )
    String tracers[] = null;


    @Argument( index = 6, name = "slaves", description = "The list of tablet server machines", required = true,
            multiValued = false )
    String slaves[] = null;

    private Tracker tracker;
    private Accumulo accumuloManager;
    private Hadoop hadoopManager;


    protected Object doExecute()
    {
        AccumuloClusterConfig config = new AccumuloClusterConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setEnvironmentId( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );

        // TODO : configure cluster

        config.setMasterNode( masterNode );
        config.setGcNode( gcNode );
        config.setMonitor( monitorNode );

        Set<String> tracersSet = new HashSet<>();
        for ( String uuid : tracers )
        {
            tracersSet.add( uuid );
        }
        config.setTracers( tracersSet );

        Set<String> slavesSet = new HashSet<>();
        for ( String uuid : slaves )
        {
            slavesSet.add( uuid );
        }
        config.setSlaves( slavesSet );

        UUID uuid = accumuloManager.installCluster( config );
        System.out.println(
                "Install operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setAccumuloManager( final Accumulo accumuloManager )
    {
        this.accumuloManager = accumuloManager;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}
