package io.subutai.plugin.elasticsearch.cli;


import java.util.List;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.plugin.elasticsearch.api.Elasticsearch;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


/**
 * Displays the last log entries
 */
@Command( scope = "elasticsearch", name = "list-clusters", description = "Gets the list of Elasticsearch clusters" )
public class ListClustersCommand extends OsgiCommandSupport
{

    private Elasticsearch elasticsearchManager;


    public ListClustersCommand( final Elasticsearch elasticsearchManager )
    {
        this.elasticsearchManager = elasticsearchManager;
    }


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
}
