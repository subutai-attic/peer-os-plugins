package io.subutai.plugin.elasticsearch.cli;



import java.util.List;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.elasticsearch.api.Elasticsearch;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * Displays the last log entries
 */
@Command(scope = "elasticsearch", name = "list-clusters", description = "Gets the list of Elasticsearch clusters")
public class ListClustersCommand extends OsgiCommandSupport
{

    private Elasticsearch elasticsearchManager;
    private Tracker tracker;

    protected Object doExecute()
    {
        List<ElasticsearchClusterConfiguration> list = elasticsearchManager.getClusters();
        if ( !list.isEmpty() )
        {
            StringBuilder sb = new StringBuilder();

            for ( ElasticsearchClusterConfiguration config : list )
            {
                sb.append( config.getClusterName() ).append( "\n" );
            }
            System.out.println( sb.toString() );
        }
        else
        {
            System.out.println( "No clusters found..." );
        }

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
