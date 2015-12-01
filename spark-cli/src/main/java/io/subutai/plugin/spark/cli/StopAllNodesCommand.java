package io.subutai.plugin.spark.cli;


import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.spark.api.Spark;
import io.subutai.plugin.spark.api.SparkClusterConfig;


/**
 * sample command : spark:stop-cluster test \ {cluster name}
 */
@Command( scope = "spark", name = "stop-cluster", description = "Command to stop spark cluster" )
public class StopAllNodesCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Spark sparkManager;
    private EnvironmentManager environmentManager;
    private Tracker tracker;
    private static final Logger LOG = LoggerFactory.getLogger( InstallClusterCommand.class.getName() );


    public StopAllNodesCommand( final Spark sparkManager, final EnvironmentManager environmentManager,
                                final Tracker tracker )
    {
        this.sparkManager = sparkManager;
        this.environmentManager = environmentManager;
        this.tracker = tracker;
    }


    protected Object doExecute() throws IOException
    {
        System.out.println( "Stopping " + clusterName + " spark cluster..." );
        SparkClusterConfig config = sparkManager.getCluster( clusterName );
        String sparkMaster = config.getMasterNodeId();
        Environment environment;
        try
        {
            environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            try
            {
                environment.getContainerHostById( sparkMaster ).getHostname();
                UUID uuid = sparkManager.stopCluster( clusterName );
                System.out.println(
                        "Stop cluster operation is " + TrackerReader.waitUntilOperationFinish( tracker, uuid ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Could not find container host !!!" );
                e.printStackTrace();
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Could not find environment !!!" );
            e.printStackTrace();
        }
        return null;
    }
}
