package io.subutai.plugin.hive.cli;


import java.util.List;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.plugin.hive.api.Hive;
import io.subutai.plugin.hive.api.HiveConfig;


/**
 * sample command : hive:list-clusters
 */
@Command( scope = "hive", name = "list-clusters", description = "Lists Hive clusters" )
public class ListClustersCommand extends OsgiCommandSupport
{
    private Hive hiveManager;


    protected Object doExecute()
    {
        List<HiveConfig> configList = hiveManager.getClusters();
        if ( !configList.isEmpty() )
        {
            for ( HiveConfig config : configList )
            {
                System.out.println( config.getClusterName() );
            }
        }
        else
        {
            System.out.println( "There is no Hive cluster" );
        }
        return null;
    }


    public Hive getHiveManager()
    {
        return hiveManager;
    }


    public void setHiveManager( Hive hiveManager )
    {
        this.hiveManager = hiveManager;
    }
}