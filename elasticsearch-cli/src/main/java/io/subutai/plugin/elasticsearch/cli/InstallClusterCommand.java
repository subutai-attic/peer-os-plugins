package io.subutai.plugin.elasticsearch.cli;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.elasticsearch.api.Elasticsearch;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


@Command( scope = "elasticsearch", name = "install-cluster", description = "Command to install Elasticsearch cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "environmentId", description = "The environment id to be used to setup Elasticsearch"
            + " cluster", required = true,
            multiValued = false )
    String environmentId = null;

    @Argument( index = 1, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;

    @Argument( index = 2, name = "list", description = "The list of nodes", required = true,
            multiValued = false )
    String allNodes[] = null;


    private Elasticsearch elasticsearchManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( DescribeClusterCommand.class.getName() );


    protected Object doExecute() throws IOException
    {
        ElasticsearchClusterConfiguration config = new ElasticsearchClusterConfiguration();
        try
        {
            Environment environment = environmentManager.loadEnvironment( environmentId );
            config.setEnvironmentId( environmentId );
            config.setClusterName( clusterName );
            Set<String> nodes = new HashSet<>();
            for ( String slave : allNodes )
            {
                if ( checkGivenUUID( environment, slave ) )
                {
                    nodes.add( slave );
                }
                else
                {
                    System.out.println( "Could not find container host with given uuid : " + slave );
                    return null;
                }
            }
            config.setNodes( nodes );

            UUID uuid = elasticsearchManager.installCluster( config );
            System.out.println(
                    "Install operation is " + StartAllNodesCommand.waitUntilOperationFinish( tracker, uuid ) );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Could not find environment", e );
            e.printStackTrace();
        }

        return null;
    }


    private boolean checkGivenUUID( Environment environment, String uuid )
    {
        for ( ContainerHost host : environment.getContainerHosts() )
        {
            if ( host.getId().equals( uuid ) )
            {
                return true;
            }
        }
        return false;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Elasticsearch getElasticsearchManager()
    {
        return elasticsearchManager;
    }


    public void setElasticsearchManager( final Elasticsearch elasticsearchManager )
    {
        this.elasticsearchManager = elasticsearchManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}
