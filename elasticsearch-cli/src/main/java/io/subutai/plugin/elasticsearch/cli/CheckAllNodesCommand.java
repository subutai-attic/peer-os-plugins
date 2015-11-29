package io.subutai.plugin.elasticsearch.cli;


import java.io.IOException;
import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.elasticsearch.api.Elasticsearch;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


@Command( scope = "elasticsearch", name = "check-cluster", description = "Command to check Elasticsearch cluster" )
public class CheckAllNodesCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Elasticsearch elasticsearchManager;
    private Tracker tracker;


    public CheckAllNodesCommand( final Elasticsearch elasticsearchManager, final Tracker tracker )
    {
        this.elasticsearchManager = elasticsearchManager;
        this.tracker = tracker;
    }


    protected Object doExecute() throws IOException
    {
        ElasticsearchClusterConfiguration config = elasticsearchManager.getCluster( clusterName );

        for ( String nodeId : config.getNodes() )
        {
            UUID uuid = elasticsearchManager.checkNode( config.getClusterName(), nodeId );
            TrackerReader.checkStatus( tracker, uuid );
        }

        return null;
    }
}
