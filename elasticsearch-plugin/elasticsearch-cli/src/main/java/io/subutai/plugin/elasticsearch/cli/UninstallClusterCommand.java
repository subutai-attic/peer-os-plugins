package io.subutai.plugin.elasticsearch.cli;


import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.elasticsearch.api.Elasticsearch;


@Command( scope = "elasticsearch", name = "uninstall-cluster", description = "Command to uninstall Elasticsearch "
        + "cluster" )
public class UninstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Elasticsearch elasticsearchManager;
    private Tracker tracker;


    public UninstallClusterCommand( final Elasticsearch elasticsearchManager, final Tracker tracker )
    {
        this.elasticsearchManager = elasticsearchManager;
        this.tracker = tracker;
    }


    protected Object doExecute()
    {
        UUID uuid = elasticsearchManager.uninstallCluster( clusterName );
        System.out.println( "Uninstall operation is " + TrackerReader.waitUntilOperationFinish( tracker, uuid ) );
        return null;
    }
}
