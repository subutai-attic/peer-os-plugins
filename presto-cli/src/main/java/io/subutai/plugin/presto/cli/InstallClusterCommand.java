package io.subutai.plugin.presto.cli;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.presto.api.Presto;
import io.subutai.plugin.presto.api.PrestoClusterConfig;


/**
 * sample command : presto:install-cluster test \ {cluster name} test \ { hadoop cluster name } hadoop1 \ { coordinator
 * } [ hadoop1, hadoop2 ] \ { list of worker machines }
 */
@Command( scope = "presto", name = "install-cluster", description = "Command to install Presto cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;

    @Argument( index = 1, name = "hadoopClusterName", description = "The name of hadoop cluster.", required = true,
            multiValued = false )
    String hadoopClusterName = null;

    @Argument( index = 2, name = "coordinator", description = "The hostname of coordinator container", required = true,
            multiValued = false )
    String coordinator = null;

    @Argument( index = 3, name = "workers", description = "The hostname list of worker nodes", required = true,
            multiValued = false )
    String workers[] = null;

    private static final Logger LOG = LoggerFactory.getLogger( InstallClusterCommand.class.getName() );
    private Presto prestoManager;
    private Hadoop hadoopManager;
    private EnvironmentManager environmentManager;
    private Tracker tracker;


    protected Object doExecute() throws IOException
    {
        try
        {
            Environment environment = environmentManager
                    .loadEnvironment( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );
            try
            {
                PrestoClusterConfig config = new PrestoClusterConfig();
                config.setClusterName( clusterName );
                config.setHadoopClusterName( hadoopClusterName );
                config.setCoordinatorNode( environment.getContainerHostByHostname( coordinator ).getId() );
                Set<String> workerUUIS = new HashSet<>();
                for ( String hostname : workers )
                {
                    workerUUIS.add( environment.getContainerHostByHostname( hostname ).getId() );
                }
                config.setWorkers( workerUUIS );
                config.setEnvironmentId( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );

                System.out.println( "Installing presto cluster..." );
                UUID uuid = prestoManager.installCluster( config );
                System.out.println(
                        "Install operation is " + StartAllNodesCommand.waitUntilOperationFinish( tracker, uuid ) );
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


    public Presto getPrestoManager()
    {
        return prestoManager;
    }


    public void setPrestoManager( final Presto prestoManager )
    {
        this.prestoManager = prestoManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}
