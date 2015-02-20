package org.safehaus.subutai.plugin.presto.cli;


import java.util.List;

import org.safehaus.subutai.plugin.presto.api.Presto;
import org.safehaus.subutai.plugin.presto.api.PrestoClusterConfig;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

/**
 * sample command :
 *      hadoop:list-clusters
 */
@Command( scope = "presto", name = "list-clusters", description = "Lists Presto clusters" )
public class ListClustersCommand extends OsgiCommandSupport
{
    private Presto prestoManager;

    protected Object doExecute()
    {
        List<PrestoClusterConfig> configList = prestoManager.getClusters();
        if ( !configList.isEmpty() )
        {
            for ( PrestoClusterConfig config : configList )
            {
                System.out.println( config.getClusterName() );
            }
        }
        else
        {
            System.out.println( "There is no Presto cluster" );
        }
        return null;
    }

    public Presto getPrestoManager()
    {
        return prestoManager;
    }


    public void setPrestoManager( Presto prestoManager )
    {
        this.prestoManager = prestoManager;
    }

}
