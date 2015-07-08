package io.subutai.plugin.presto.cli;


import java.util.List;

import io.subutai.plugin.presto.api.Presto;
import io.subutai.plugin.presto.api.PrestoClusterConfig;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

/**
 * sample command :
 *      presto:list-clusters
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
