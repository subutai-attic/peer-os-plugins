package io.subutai.plugin.mahout.cli;


import java.util.List;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.plugin.mahout.api.Mahout;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;


/**
 * sample command : mahout:list-clusters
 */
@Command( scope = "mahout", name = "list-clusters", description = "Lists Mahout clusters" )
public class ListClustersCommand extends OsgiCommandSupport
{

    private Mahout mahoutManager;


    public Mahout getMahoutManager()
    {
        return mahoutManager;
    }


    public void setMahoutManager( Mahout mahoutManager )
    {
        this.mahoutManager = mahoutManager;
    }


    protected Object doExecute()
    {
        List<MahoutClusterConfig> configList = mahoutManager.getClusters();
        if ( !configList.isEmpty() )
        {
            for ( MahoutClusterConfig config : configList )
            {
                System.out.println( config.getClusterName() );
            }
        }
        else
        {
            System.out.println( "There is no Mahout cluster" );
        }

        return null;
    }
}
