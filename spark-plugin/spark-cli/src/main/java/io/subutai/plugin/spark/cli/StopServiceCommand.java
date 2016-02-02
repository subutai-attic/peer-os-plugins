package io.subutai.plugin.spark.cli;


import java.io.IOException;
import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.spark.api.Spark;
import io.subutai.plugin.spark.api.SparkClusterConfig;


/**
 * sample command : spark:stop-node test \ {cluster name} hadoop1 \ { container hostname }
 */
@Command( scope = "spark", name = "stop-node", description = "Command to stop spark service" )
public class StopServiceCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "Name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "hostname", description = "UUID of the agent.", required = true,
            multiValued = false )
    String hostname = null;
    private Spark sparkManager;
    private EnvironmentManager environmentManager;
    private Tracker tracker;


    public StopServiceCommand( final Spark sparkManager, final EnvironmentManager environmentManager,
                               final Tracker tracker )
    {
        this.sparkManager = sparkManager;
        this.environmentManager = environmentManager;
        this.tracker = tracker;
    }


    protected Object doExecute() throws IOException
    {
        SparkClusterConfig config = sparkManager.getCluster( clusterName );
        UUID uuid = sparkManager.stopNode( clusterName, hostname,
                CheckAllNodesCommand.isMaster( config, hostname, environmentManager ) );
        tracker.printOperationLog( SparkClusterConfig.PRODUCT_KEY, uuid, 30000 );
        return null;
    }
}
