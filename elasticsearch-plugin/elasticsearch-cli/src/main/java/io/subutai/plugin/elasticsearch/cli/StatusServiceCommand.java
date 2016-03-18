package io.subutai.plugin.elasticsearch.cli;


import java.io.IOException;
import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.elasticsearch.api.Elasticsearch;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


@Command( scope = "elasticsearch", name = "check-node", description = "Command to check Elasticsearch service" )
public class StatusServiceCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "Name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "hostname", description = "Hostname of container.", required = true,
            multiValued = false )
    String hostname = null;
    private Elasticsearch elasticsearchManager;
    private Tracker tracker;


    public StatusServiceCommand( final Elasticsearch elasticsearchManager, final Tracker tracker )
    {
        this.elasticsearchManager = elasticsearchManager;
        this.tracker = tracker;
    }


    protected Object doExecute() throws IOException
    {

        UUID uuid = elasticsearchManager.checkNode( clusterName, hostname );
        tracker.printOperationLog( ElasticsearchClusterConfiguration.PRODUCT_KEY, uuid, 30000 );

        return null;
    }
}
