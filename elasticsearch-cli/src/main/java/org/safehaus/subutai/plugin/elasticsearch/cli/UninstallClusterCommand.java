package org.safehaus.subutai.plugin.elasticsearch.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.elasticsearch.api.Elasticsearch;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command(scope = "elasticsearch", name = "uninstall-cluster", description = "Command to uninstall Elasticsearch cluster")
public class UninstallClusterCommand extends OsgiCommandSupport
{

    @Argument(index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false)
    String clusterName = null;
    private Elasticsearch elasticsearchManager;
    private Tracker tracker;

    protected Object doExecute()
    {
        UUID uuid = elasticsearchManager.uninstallCluster( clusterName );
        System.out.println( "Uninstall operation is " + StartAllNodesCommand.waitUntilOperationFinish( tracker, uuid ) );
        return null;
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
}
