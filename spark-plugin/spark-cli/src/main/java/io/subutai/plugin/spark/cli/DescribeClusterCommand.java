package io.subutai.plugin.spark.cli;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.spark.api.Spark;
import io.subutai.plugin.spark.api.SparkClusterConfig;


/**
 * sample command : spark:describe-cluster test \ {cluster name}
 */
@Command( scope = "spark", name = "describe-cluster", description = "Shows the details of the Spark cluster." )
public class DescribeClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Spark sparkManager;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( InstallClusterCommand.class.getName() );


    public DescribeClusterCommand( final Spark sparkManager, final EnvironmentManager environmentManager )
    {
        this.sparkManager = sparkManager;
        this.environmentManager = environmentManager;
    }


    protected Object doExecute()
    {
        SparkClusterConfig config = sparkManager.getCluster( clusterName );
        try
        {
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            StringBuilder sb = new StringBuilder();
            sb.append( "Cluster name: " ).append( config.getClusterName() ).append( "\n" );
            sb.append( "Nodes:" ).append( "\n" );
            for ( String containerId : config.getAllNodesIds() )
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
            sb.append( "Master:" ).append( "\n" );
            try
            {
                sb.append( "   Container Hostname: " ).
                        append( environment.getContainerHostById( config.getMasterNodeId() ).getHostname() )
                  .append( "\n" );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Could not find container host !!!" );
                e.printStackTrace();
            }
            sb.append( "Slaves :" ).append( "\n" );
            for ( String containerId : config.getSlaveIds() )
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
}
