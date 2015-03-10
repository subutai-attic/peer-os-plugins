package org.safehaus.subutai.plugin.solr.cli;


import java.io.IOException;
import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.solr.api.Solr;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : solr:start-node test \ {cluster name} solr \ { container hostname }
 */
@Command( scope = "solr", name = "start-node", description = "Command to start Solr service" )
public class StartServiceCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "Name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "hostname", description = "hostName of the container.", required = true,
            multiValued = false )
    String hostname = null;
    private Solr solrManager;
    private Tracker tracker;


    protected Object doExecute() throws IOException
    {
        UUID uuid = getSolrManager().startNode( clusterName, hostname );
        getTracker().printOperationLog( SolrClusterConfig.PRODUCT_KEY, uuid, 30000 );
        return null;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Solr getSolrManager()
    {
        return solrManager;
    }


    public void setSolrManager( final Solr solrManager )
    {
        this.solrManager = solrManager;
    }
}

