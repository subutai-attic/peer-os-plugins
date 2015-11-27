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
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.spark.api.Spark;
import io.subutai.plugin.spark.api.SparkClusterConfig;


/**
 * sample command : spark:check-cluster test \ {cluster name}
 */
@Command( scope = "spark", name = "check-cluster", description = "Command to check spark cluster" )
public class CheckAllNodesCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Spark sparkManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( InstallClusterCommand.class.getName() );


    protected Object doExecute() throws IOException
    {
        System.out.println( "Checking cluster nodes ... " );
        SparkClusterConfig config = sparkManager.getCluster( clusterName );
        for ( String id : config.getAllNodesIds() )
        {
            try
            {
                Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
                try
                {
                    String hostname = environment.getContainerHostById( id ).getHostname();
                    UUID checkUUID = sparkManager
                            .checkNode( clusterName, hostname, isMaster( config, hostname, environmentManager ) );
                    System.out.println(
                            "Spark on " + hostname + " is " + TrackerReader.checkStatus( tracker, checkUUID ) );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOG.error( "Could not find container host." );
                    e.printStackTrace();
                }
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOG.error( "Could not find environment." );
                e.printStackTrace();
            }
        }
        return null;
    }


    protected static boolean isMaster( SparkClusterConfig config, String hostname,
                                       EnvironmentManager environmentManager )
    {
        try
        {
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            try
            {
                ContainerHost master = environment.getContainerHostById( config.getMasterNodeId() );
                if ( master.getHostname().equals( hostname ) )
                {
                    return true;
                }
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return false;
    }


    public Spark getSparkManager()
    {
        return sparkManager;
    }


    public void setSparkManager( final Spark sparkManager )
    {
        this.sparkManager = sparkManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}
