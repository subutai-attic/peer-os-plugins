package io.subutai.plugin.zookeeper.cli;


import java.util.List;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


/**
 * Displays the last log entries
 */
@Command( scope = "zookeeper", name = "list-clusters", description = "mydescription" )
public class ListClustersCommand extends OsgiCommandSupport
{

    private Zookeeper zookeeperManager;


    public void setZookeeperManager( Zookeeper zookeeperManager )
    {
        this.zookeeperManager = zookeeperManager;
    }


    protected Object doExecute()
    {
        List<ZookeeperClusterConfig> configList = zookeeperManager.getClusters();
        if ( !configList.isEmpty() )
        {
            for ( ZookeeperClusterConfig config : configList )
            {
                System.out.println( config.getClusterName() );
            }
        }
        else
        {
            System.out.println( "No Zookeeper cluster" );
        }

        return null;
    }
}
