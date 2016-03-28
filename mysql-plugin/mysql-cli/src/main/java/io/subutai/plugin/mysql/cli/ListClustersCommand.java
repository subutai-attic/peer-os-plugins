package io.subutai.plugin.mysql.cli;


import java.util.List;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.plugin.mysql.api.MySQLC;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;


@Command( scope = "mysql", name = "list-clusters", description = "command to list mysql clusters" )
public class ListClustersCommand extends OsgiCommandSupport
{
    private MySQLC manager;


    public MySQLC getManager()
    {
        return manager;
    }


    public void setManager( final MySQLC manager )
    {
        this.manager = manager;
    }


    @Override
    protected Object doExecute() throws Exception
    {
        List<MySQLClusterConfig> configList = manager.getClusters();

        if ( !configList.isEmpty() )
        {
            for ( MySQLClusterConfig config : configList )
            {
                System.out.println( config.getClusterName() );
            }
        }
        else
        {
            System.out.println( "No SQL Clusters found" );
        }
        return null;
    }
}
