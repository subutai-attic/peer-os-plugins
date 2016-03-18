package io.subutai.plugin.elasticsearch.cli;


import java.io.IOException;
import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.elasticsearch.api.Elasticsearch;


@Command( scope = "elasticsearch", name = "stop-cluster", description = "Command to start Elasticsearch cluster" )
public class StartAllNodesCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Elasticsearch elasticsearchManager;
    private Tracker tracker;


    public StartAllNodesCommand( final Elasticsearch elasticsearchManager, final Tracker tracker )
    {
        this.elasticsearchManager = elasticsearchManager;
        this.tracker = tracker;
    }


    protected Object doExecute() throws IOException
    {
        UUID uuid = elasticsearchManager.startCluster( clusterName );
        System.out.println( "Start cluster operation is " + TrackerReader.waitUntilOperationFinish( tracker, uuid ) );
        return null;
    }
}
