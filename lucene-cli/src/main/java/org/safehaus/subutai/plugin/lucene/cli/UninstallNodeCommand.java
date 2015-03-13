package org.safehaus.subutai.plugin.lucene.cli;


import java.util.UUID;

import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.lucene.api.Lucene;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : lucene:uninstall-node test \ {cluster name} haddop1 \ {node}
 */
@Command( scope = "lucene", name = "uninstall-node", description = "Command to uninstall node from Lucene cluster" )
public class UninstallNodeCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "node", description = "Delete container", required = true,
            multiValued = false )
    String node = null;
    private Lucene luceneManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Uninstalling " + node + " node..." );
        UUID uuid = luceneManager.uninstallNode( clusterName, node );
        System.out.println(
                "Uninstall node operation is " + InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
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
