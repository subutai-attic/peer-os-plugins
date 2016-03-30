package io.subutai.plugin.elasticsearch.cli;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.elasticsearch.api.Elasticsearch;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


@Command( scope = "elasticsearchManager", name = "describe-cluster", description = "Shows the details of the "
        + "Elasticsearch cluster." )
public class DescribeClusterCommand extends OsgiCommandSupport
{
    private Elasticsearch elasticsearchManager;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( DescribeClusterCommand.class.getName() );

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;


    public DescribeClusterCommand( final Elasticsearch elasticsearchManager,
                                   final EnvironmentManager environmentManager )
    {
        this.elasticsearchManager = elasticsearchManager;
        this.environmentManager = environmentManager;
    }


    public Object doExecute()
    {
        try
        {
            ElasticsearchClusterConfiguration config = elasticsearchManager.getCluster( clusterName );
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            StringBuilder sb = new StringBuilder();
            sb.append( "Cluster name: " ).append( config.getClusterName() ).append( "\n" );
            sb.append( "Nodes:" ).append( "\n" );
            for ( String containerId : config.getNodes() )
            {
                try
                {
                    sb.append( "Container Hostname: " )
                      .append( environment.getContainerHostById( containerId ).getHostname() ).append( "\n" );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOG.error( "Could not find container host", e );
                    e.printStackTrace();
                }
            }
            System.out.println( sb.toString() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Could not find environment", e );
            e.printStackTrace();
        }
        return null;
    }
}
