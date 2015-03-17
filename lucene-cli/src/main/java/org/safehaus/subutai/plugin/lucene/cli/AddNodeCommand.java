package org.safehaus.subutai.plugin.lucene.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.lucene.api.Lucene;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : lucene:add-node test \ {cluster name} haddop1 \ {node}
 */
@Command( scope = "lucene", name = "add-node", description = "Command to add node to Lucene cluster" )
public class AddNodeCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "node", description = "Add container", required = true,
            multiValued = false )
    String node = null;
    private Lucene luceneManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Adding " + node + " node..." );
        UUID uuid = luceneManager.addNode( clusterName, node );
        System.out.println(
                "Add node operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
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


    public Lucene getLuceneManager()
    {
        return luceneManager;
    }


    public void setLuceneManager( Lucene luceneManager )
    {
        this.luceneManager = luceneManager;
    }
}
