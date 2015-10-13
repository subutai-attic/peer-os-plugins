package io.subutai.plugin.presto.cli;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.presto.api.Presto;
import io.subutai.plugin.presto.api.PrestoClusterConfig;


/**
 * sample command : presto:describe-cluster test \ {cluster name}
 */
@Command( scope = "presto", name = "describe-cluster", description = "Shows the details of the Presto cluster." )
public class DescribeClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Presto prestoManager;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( InstallClusterCommand.class.getName() );


    protected Object doExecute()
    {
        PrestoClusterConfig config = prestoManager.getCluster( clusterName );
        try
        {
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            StringBuilder sb = new StringBuilder();
            sb.append( "Cluster name: " ).append( config.getClusterName() ).append( "\n" );
            sb.append( "Nodes:" ).append( "\n" );
            for ( String containerId : config.getAllNodes() )
            {
                try
                {
                    sb.append( "   Container Hostname: " ).
                            append( environment.getContainerHostById( containerId ).getHostname() ).append( "\n" );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOG.error( "Could not find container host !!!" );
                    e.printStackTrace();
                }
            }
            sb.append( "Coordinator:" ).append( "\n" );
            try
            {
                sb.append( "   Container Hostname: " ).
                        append( environment.getContainerHostById( config.getCoordinatorNode() ).getHostname() )
                  .append( "\n" );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Could not find container host !!!" );
                e.printStackTrace();
            }
            sb.append( "Workers :" ).append( "\n" );
            for ( String containerId : config.getWorkers() )
            {
                try
                {
                    sb.append( "   Container Hostname: " ).
                            append( environment.getContainerHostById( containerId ).getHostname() ).append( "\n" );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOG.error( "Could not find container host !!!" );
                    e.printStackTrace();
                }
            }
            System.out.println( sb.toString() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Could not find environment !!! " );
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


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}
