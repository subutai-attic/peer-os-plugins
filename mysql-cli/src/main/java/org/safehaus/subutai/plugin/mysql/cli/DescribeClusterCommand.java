package org.safehaus.subutai.plugin.mysql.cli;


import org.safehaus.subutai.plugin.mysqlc.api.MySQLC;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLClusterConfig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * Created by tkila on 5/18/15.
 */
@Command( scope = "mysql", name = "describe-cluster", description = "Shows the details of the MySQL cluster." )
public class DescribeClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private MySQLC mySqlManager;

    @Override
    protected Object doExecute() throws Exception
    {
        MySQLClusterConfig config = mySqlManager.getCluster( clusterName );
        if ( config != null )
        {
            StringBuilder sb = new StringBuilder();
            sb.append( "Cluster name: " ).append( config.getClusterName() ).append( "\n" );
            System.out.println( sb.toString() );
        }
        else
        {
            System.out.println( "No clusters found..." );
        }

        return null;
    }


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public MySQLC getMySqlManager()
    {
        return mySqlManager;
    }


    public void setMySqlManager( final MySQLC mySqlManager )
    {
        this.mySqlManager = mySqlManager;
    }
}
